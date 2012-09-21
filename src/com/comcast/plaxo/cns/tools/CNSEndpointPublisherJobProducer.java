/**
 * Copyright 2012 Comcast Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.plaxo.cns.tools;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.plaxo.cns.model.CNSEndpointPublishJob;
import com.comcast.plaxo.cns.model.CNSEndpointPublishJob.SubInfo;
import com.comcast.plaxo.cns.model.CNSMessage;
import com.comcast.plaxo.cns.model.CNSSubscription;
import com.comcast.plaxo.cns.persistence.CachedCNSEndpointPublishJob;
import com.comcast.plaxo.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.plaxo.cns.persistence.TopicNotFoundException;

/**
 * This class represents the Producer that reads the CNSMessage, partitions the subscribers and
 * creates the EndpointPublishJobs
 * @author aseem, bwolf, ppang
 *
 * Class is thread-safe
 */

public class CNSEndpointPublisherJobProducer implements RunnableForPartition {
    private static Logger logger = Logger.getLogger(CNSEndpointPublisherJobProducer.class);
    
    static volatile boolean initialized = false; 
    private static volatile User cnsInternalUser = null;
    private static volatile BasicAWSCredentials awsCredentials = null;
    private static final String epPublishQNamePrefix = CMBProperties.getInstance().getCnsEndpointPublishQueueNamePrefix();
    private static final String publishQNamePrefix = CMBProperties.getInstance().getCnsPublishQueueNamePrefix();;
    static volatile AmazonSQS sqs;
    static volatile ICNSSubscriptionPersistence subscriptionPersistence = null; 
    private long lastMinute = System.currentTimeMillis()/(1000*60); 

    public static class TestInterface {
        public static boolean isInitialized() {
            return initialized;
        }
        public static void setInitialized(boolean flag) {
        	initialized = flag;
        }
        public static AmazonSQS getSQS() {
            return sqs;
        }
        public static void setSQS(AmazonSQS inst) {
            sqs = inst;
        }
        public static ICNSSubscriptionPersistence getSubscriptionPersistence() {
        	return subscriptionPersistence;
        }
        public static void setSubscriptionPersistence(ICNSSubscriptionPersistence inst) {
        	subscriptionPersistence = inst; 
        }
    }
    
   /* 
    * Read the EndpointPublishQ_<m> property and ensuring they exist (create if not) 
    * @throws PersistenceException 
    */
   public static void initialize() throws PersistenceException {
	   if (initialized) return;
	   if (cnsInternalUser == null) {

			IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
	        cnsInternalUser = userHandler.getUserByName(CMBProperties.getInstance().getCnsUserName());

	        if (cnsInternalUser == null) {	          
	        	cnsInternalUser =  userHandler.createUser(CMBProperties.getInstance().getCnsUserName(), CMBProperties.getInstance().getCnsUserPassword());
	        }
		}

		if (awsCredentials == null) {
	        awsCredentials = new BasicAWSCredentials(cnsInternalUser.getAccessKey(), cnsInternalUser.getAccessSecret());
		}
		
		if (sqs == null) {
            sqs = new AmazonSQSClient(awsCredentials);
            sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());
		}
		
		if (subscriptionPersistence == null) {
			subscriptionPersistence = PersistenceFactory.getSubscriptionPersistence();
		}

		for (int i = 0; i < CMBProperties.getInstance().getNumPublishJobQs(); i++) {
			GetQueueUrlRequest getQUrlReq = new GetQueueUrlRequest(publishQNamePrefix + i);
			try {
				sqs.getQueueUrl(getQUrlReq);
			} catch (AmazonServiceException e) {
				if (e.getStatusCode() == 400) {
					logger.info("event=initialize info=creating_missing_queue name=" + publishQNamePrefix + i);
					CreateQueueRequest createQReq = new CreateQueueRequest(publishQNamePrefix + i);
					CreateQueueResult createQRes = sqs.createQueue(createQReq);
					if (createQRes.getQueueUrl() == null) {
						throw new IllegalStateException("Could not create queue with name " + publishQNamePrefix + i);
					}
					logger.info("event=initialize info=created_queue name=" + publishQNamePrefix + i);
				} else {
					throw e;
				}
			}
		}

		for (int i = 0; i < CMBProperties.getInstance().getNumEPPublishJobQs(); i++) {
			GetQueueUrlRequest getQUrlReq = new GetQueueUrlRequest(epPublishQNamePrefix + i);
			try {
				sqs.getQueueUrl(getQUrlReq);
			} catch (AmazonServiceException e) {
				if (e.getStatusCode() == 400) {
					logger.info("event=initialize info=creating_missing_queue name=" + epPublishQNamePrefix + i);
					CreateQueueRequest createQReq = new CreateQueueRequest(epPublishQNamePrefix + i);
					CreateQueueResult createQRes = sqs.createQueue(createQReq);
					if (createQRes.getQueueUrl() == null) {
						throw new IllegalStateException("Could not create queue with name " + epPublishQNamePrefix + i);
					}
					logger.info("event=initialize info=created_queue name=" + epPublishQNamePrefix + i);
				} else {
					throw e;
				}
			}
		}

		logger.info("event=initialize status=success");
		initialized = true;
   }

   public static void shutdown() {
       initialized = false;
       logger.info("event=shutdown status=success");
   }
   
    @Override
    /**
     * 1. Call ReceiveMessage(PublishJobQ.<n> where n  in [0..numPublishJobQs]
     * 2. if no message, go to sleep for 100ms and go back to 1
     * 3. if message found, 
     *  3.1 read sub-list for topic, 
     *  3.2 partition them by batch size
     *  3.3 Enqueue into EndpointPublishJobQ.<m> where m is randomly selected between [0..numEPPublishJobQs]
     *  3.4 go back to 1
     */    
    public boolean run(int partition) {
        boolean messageFound = false;
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        
        long ts1 = System.currentTimeMillis();
	    CMBControllerServlet.valueAccumulator.initializeAllCounters();
	    try {

	        if (ts1/(1000*60) > lastMinute) {
                lastMinute = ts1/(1000*60);
                logger.info("event=ping version=" + CMBControllerServlet.VERSION + " ip=" + InetAddress.getLocalHost().getHostAddress());
            }
	        
	        String publishJobQName = publishQNamePrefix + partition;
	        Message message = receiveMessage(publishJobQName); 

	        if (message != null) {
	            CNSMessage publishJob = CNSMessage.parseInstance(message.getBody());

	            List<CNSEndpointPublishJob.SubInfo> subscriptions = null;

	            try {
	                subscriptions = getSubscriptionsForTopic(publishJob.getTopicArn());
	            } catch (TopicNotFoundException e) {
	                //delete this message/job since the topic was deleted.
	                logger.error("event=run status=topic_not_found topic_arn=" + publishJob.getTopicArn());
	                deleteMessage(publishJobQName, message);
	                CMBControllerServlet.valueAccumulator.deleteAllCounters();
	                return false;
	            } catch(Exception e) {
	                logger.error("event=run status=error_fetching_subscriptions", e);
	                CMBControllerServlet.valueAccumulator.deleteAllCounters();
	                return false;
	            }

	            if (subscriptions != null && subscriptions.size() > 0) {
	                messageFound = true;
	                List<CNSEndpointPublishJob> epPublishJobs = createEndpointPublishJobs(publishJob, subscriptions);
	                for(CNSEndpointPublishJob epPublishJob: epPublishJobs) {
	                    sendEPPublishJob(epPublishJob);
	                }
	            }
	            deleteMessage(publishJobQName, message);
	            long ts2 = System.currentTimeMillis();
	            logger.info("event=processed_publish_job CassandraTime=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime)  + " CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime) + " responseTimeMS=" + (ts2 - ts1));
	        }
	    } catch(Exception e) {
	        logger.error("event=run status=exception", e);
	    }

	    CMBControllerServlet.valueAccumulator.deleteAllCounters();
	    return messageFound;
    }
    
    void deleteMessage(String queueName, Message message) {

        long ts1 = System.currentTimeMillis();
    	GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
        GetQueueUrlResult getQRes = sqs.getQueueUrl(getQueueUrlRequest);
        String qUrl = getQRes.getQueueUrl();
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
        
        long ts3 = System.currentTimeMillis();        
        DeleteMessageRequest delReq = new DeleteMessageRequest(qUrl, message.getReceiptHandle());
        sqs.deleteMessage(delReq);
        logger.info("event=delete_publish_job receipt_handle=" + message.getReceiptHandle());
        long ts4 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts4 - ts3);        
    }
    
    Message receiveMessage(String queueName) {
    	
    	Message message = null;
        
        long ts1 = System.currentTimeMillis();
    	GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
        GetQueueUrlResult getQRes = sqs.getQueueUrl(getQueueUrlRequest);
        String qUrl = getQRes.getQueueUrl();
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
        
        long ts3 = System.currentTimeMillis();
        ReceiveMessageRequest recvReq = new ReceiveMessageRequest(qUrl);
        recvReq.setMaxNumberOfMessages(1);
        recvReq.setVisibilityTimeout(CMBProperties.getInstance().getPublishJobVTO());
        ReceiveMessageResult recvRes = sqs.receiveMessage(recvReq);
        List<Message> msgs = recvRes.getMessages();
        long ts4 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts4 - ts3);
        
        if (msgs.size() > 0) {
            logger.info("event=received_messages count=" + msgs.size());
        	try {
        		message = msgs.get(0);
        	} catch (Exception e) {
                logger.error("event=error_receiving_message action=wait_for_revisibility", e);
                message = null;
        	}
        } 
        return message; 
    }
    
    List<CNSEndpointPublishJob.SubInfo> getSubscriptionsForTopic(String topicArn) throws Exception {        
    	List<CNSEndpointPublishJob.SubInfo> subInfoList = new ArrayList<CNSEndpointPublishJob.SubInfo>();
    	if (CMBProperties.getInstance().isUseSubInfoCache()) {
    	    subInfoList.addAll(CachedCNSEndpointPublishJob.getSubInfos(topicArn));
    	} else {
    	    String nextToken = null;
    	    while (true) {
    	        List<CNSSubscription> subscriptions = subscriptionPersistence.listSubscriptionsByTopic(nextToken, topicArn, null, 1000);
    	        if (subscriptions == null || subscriptions.size() == 0) {
    	            break;
    	        }
    	        boolean allPendingConfirmation = true;
    	        for(CNSSubscription subscription: subscriptions) {
    	            if (!subscription.getArn().equals("PendingConfirmation")) {
    	                allPendingConfirmation = false;
    	                subInfoList.add(new CNSEndpointPublishJob.SubInfo(subscription.getProtocol(), subscription.getEndpoint(), subscription.getArn()));
    	                nextToken = subscription.getArn();
    	            }
    	        }
    	        if (allPendingConfirmation) {
    	            break;
    	        }
    	    }
    	}
        logger.info("event=fetched_subscriptions");
    	return subInfoList;
    }
   
    
    List<CNSEndpointPublishJob> createEndpointPublishJobs(CNSMessage message, List<CNSEndpointPublishJob.SubInfo> subscriptions) {
    	List<CNSEndpointPublishJob> epPublishJobs = new ArrayList<CNSEndpointPublishJob>();
    	if (subscriptions != null) {
	    	int maxSubsPerEPPublishJob = CMBProperties.getInstance().getMaxSubscriptionsPerEPPublishJob();
	    	int numEPPublishJobs = (int)Math.ceil(subscriptions.size()/(float)maxSubsPerEPPublishJob);
	    	int subIndex = 0;
	    	for(int i=0; i<numEPPublishJobs; i++) {
	    		List<CNSEndpointPublishJob.SubInfo> epPublishJobSubscriptions = new ArrayList<CNSEndpointPublishJob.SubInfo>();
	    		for(int j=0; j<maxSubsPerEPPublishJob; j++) {
	    		    SubInfo subInfo;
	    		    if (CMBProperties.getInstance().isUseSubInfoCache()) {
	    		        subInfo = new CachedCNSEndpointPublishJob.CachedSubInfo(subscriptions.get(subIndex).protocol, subscriptions.get(subIndex).endpoint, subscriptions.get(subIndex).subArn);
	    		    } else {
	    		        subInfo = new SubInfo(subscriptions.get(subIndex).protocol, subscriptions.get(subIndex).endpoint, subscriptions.get(subIndex).subArn);
	    		    }
	    		     
	    			epPublishJobSubscriptions.add(subInfo);
	    			
	    			if (++subIndex==subscriptions.size()) {
	    				break;
	    			}
	    		}
	    		CNSEndpointPublishJob job;
	    		if (CMBProperties.getInstance().isUseSubInfoCache()) {
	    		    job = new CachedCNSEndpointPublishJob(message, epPublishJobSubscriptions);
	    		} else {
	    		    job = new CNSEndpointPublishJob(message, epPublishJobSubscriptions);
	    		}
	    		epPublishJobs.add(job);
	    	}
    	}
        logger.info("event=created_endpoint_publish_jobs");
    	return epPublishJobs;
    }
    
    void sendEPPublishJob(CNSEndpointPublishJob epPublishJob) {
    	
    	String queueName =  CMBProperties.getInstance().getCnsEndpointPublishQueueNamePrefix() + ((new Random()).nextInt(CMBProperties.getInstance().getNumEPPublishJobQs()));
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
        String queueUrl = null;

        try {
            long ts1 = System.currentTimeMillis();
            GetQueueUrlResult getQueueUrlResult = sqs.getQueueUrl(getQueueUrlRequest);
            queueUrl = getQueueUrlResult.getQueueUrl();
            long ts2 = System.currentTimeMillis();
            CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
            
        } catch (AmazonServiceException ex) {
        	
            if (ex.getStatusCode() == 400) {
            	
                logger.info("event=create_non_existent_queue queue_name=" + queueName);

                long ts3 = System.currentTimeMillis();
                CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
                CreateQueueResult createQueueResult = sqs.createQueue(createQueueRequest);
                long ts4 = System.currentTimeMillis();
                CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts4 - ts3);

                queueUrl = createQueueResult.getQueueUrl();
                
                if (queueUrl == null) {
                    throw new IllegalStateException("Could not create queue with name " + queueName);
                }
                
            } else {
                throw ex;
            }
        }

        long ts5 = System.currentTimeMillis();
        SendMessageResult sendMessageResult = sqs.sendMessage(new SendMessageRequest(queueUrl, epPublishJob.serialize()));
		logger.info("event=sendEPPublishJob status=success message_id=" + sendMessageResult.getMessageId());
        long ts6 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts6 - ts5);
    }
}
