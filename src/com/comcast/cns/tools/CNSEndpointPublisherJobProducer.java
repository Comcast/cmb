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
package com.comcast.cns.tools;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import me.prettyprint.hector.api.HConsistencyLevel;

import org.apache.http.conn.HttpHostConnectException;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cns.model.CNSEndpointPublishJob;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSEndpointPublishJob.CNSEndpointSubscriptionInfo;
import com.comcast.cns.persistence.CNSCachedEndpointPublishJob;
import com.comcast.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.cns.persistence.TopicNotFoundException;

/**
 * This class represents the Producer that reads the CNSMessage, partitions the subscribers and
 * creates the EndpointPublishJobs
 * @author aseem, bwolf, ppang
 *
 * Class is thread-safe
 */

public class CNSEndpointPublisherJobProducer implements CNSPublisherPartitionRunnable {
	
    private static Logger logger = Logger.getLogger(CNSEndpointPublisherJobProducer.class);
    
    private static final String CNS_PRODUCER_QUEUE_NAME_PREFIX = CMBProperties.getInstance().getCnsPublishQueueNamePrefix();
    
    private static volatile boolean initialized = false; 
    
    static volatile ICNSSubscriptionPersistence subscriptionPersistence = null; 

    public static class TestInterface {
        public static boolean isInitialized() {
            return initialized;
        }
        public static void setInitialized(boolean flag) {
        	initialized = flag;
        }
        public static AmazonSQS getSQS() {
            return CQSHandler.getSQSHandler();
        }
        public static void setSQS(AmazonSQS sqs) {
            CQSHandler.setSQSHandler(sqs);
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
	   
	   if (initialized) {
		   return;
	   }
	   
	   CQSHandler.initialize();
	   
	   if (subscriptionPersistence == null) {
			subscriptionPersistence = PersistenceFactory.getSubscriptionPersistence();
	   }

	   CQSHandler.ensureQueuesExist(CNS_PRODUCER_QUEUE_NAME_PREFIX, CMBProperties.getInstance().getNumPublishJobQs());
		
	   logger.info("event=initialize");
	   initialized = true;
   }

   public static void shutdown() {
       initialized = false;
       CQSHandler.shutdown();
       logger.info("event=shutdown");
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
    public boolean run (int partition) {
    	
        boolean messageFound = false;
        
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        
        long ts1 = System.currentTimeMillis();
	    CMBControllerServlet.valueAccumulator.initializeAllCounters();
	    
	    try {

            if (CNSPublisher.lastProducerMinute.getAndSet(ts1/(1000*60)) != ts1/(1000*60)) {
                
	        	String hostAddress = InetAddress.getLocalHost().getHostAddress();
                logger.info("event=ping version=" + CMBControllerServlet.VERSION + " ip=" + hostAddress);

	        	try {
		        	Map<String, String> values = new HashMap<String, String>();
		        	values.put("producerTimestamp", System.currentTimeMillis() + "");
		        	values.put("jmxport", System.getProperty("com.sun.management.jmxremote.port", "0"));
		        	values.put("mode", CNSPublisher.getModeString());
		        	values.put("dataCenter", CMBProperties.getInstance().getCmbDataCenter());
	                CNSPublisher.cassandraHandler.insertOrUpdateRow(hostAddress, "CNSWorkers", values, HConsistencyLevel.QUORUM);
	        	} catch (Exception ex) {
	        		logger.warn("event=ping_glitch", ex);
	        	}
            }
	        
	        String publishJobQName = CNS_PRODUCER_QUEUE_NAME_PREFIX + partition;
	        String queueUrl = CQSHandler.getQueueUrl(publishJobQName);
	        Message message = CQSHandler.receiveMessage(queueUrl); 
    		CNSWorkerMonitor.getInstance().registerCQSServiceAvailable(true);

	        if (message != null) {
	        	
	            CNSMessage publishMessage = CNSMessage.parseInstance(message.getBody());
	            
	            int messageExpirationSeconds = CMBProperties.getInstance().getCnsMessageExpirationSeconds();
	            
	            if (messageExpirationSeconds != 0 && System.currentTimeMillis() - publishMessage.getTimestamp().getTime() > messageExpirationSeconds*1000) {
	                logger.error("event=deleting_publish_job reason=message_too_old topic_arn=" + publishMessage.getTopicArn());
	                CQSHandler.deleteMessage(queueUrl, message);
	                CMBControllerServlet.valueAccumulator.deleteAllCounters();
	                return true; // return true to avoid backoff
	            }
	            
	            List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> subscriptions = null;

	            try {
	                subscriptions = getSubscriptionsForTopic(publishMessage.getTopicArn());
	            } catch (TopicNotFoundException e) {

	            	//delete this message/job since the topic was deleted.
	                
	            	logger.error("event=deleting_publish_job reason=topic_not_found topic_arn=" + publishMessage.getTopicArn());
	                CQSHandler.deleteMessage(queueUrl, message);
	                CMBControllerServlet.valueAccumulator.deleteAllCounters();
	                return true; // return true to avoid backoff
	            
	            } catch (Exception ex) {
	                logger.error("event=skipping_publish_job reason=error_fetching_subscriptions", ex);
	                CMBControllerServlet.valueAccumulator.deleteAllCounters();
	                return true; // return true to avoid backoff
	            }

	            if (subscriptions != null && subscriptions.size() > 0) {
	            	
	                messageFound = true;
	                List<CNSEndpointPublishJob> epPublishJobs = createEndpointPublishJobs(publishMessage, subscriptions);
	                
	                for (CNSEndpointPublishJob epPublishJob: epPublishJobs) {
	                	
	                	String epQueueName =  CMBProperties.getInstance().getCnsEndpointPublishQueueNamePrefix() + ((new Random()).nextInt(CMBProperties.getInstance().getNumEPPublishJobQs()));
	                    String epQueueUrl = CQSHandler.getQueueUrl(epQueueName);
	                    CQSHandler.sendMessage(epQueueUrl, epPublishJob.serialize());
	                }
	            }
	            
	            CQSHandler.deleteMessage(queueUrl, message);
	            
	            long ts2 = System.currentTimeMillis();
	            logger.info("event=processed_publish_job CassandraTime=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime)  + " CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime) + " responseTimeMS=" + (ts2 - ts1));
	        }
	    
	    } catch (AmazonClientException ex) {
	    	
	    	if (ex.getCause() instanceof HttpHostConnectException) {
	    		logger.error("event=cqs_service_unavailable", ex);
	    		CNSWorkerMonitor.getInstance().registerCQSServiceAvailable(false);
	    	} else {
	    		CQSHandler.ensureQueuesExist(CNS_PRODUCER_QUEUE_NAME_PREFIX, CMBProperties.getInstance().getNumPublishJobQs());
	    	}
	    
	    } catch (Exception ex) {
	        logger.error("event=unexpected_exception", ex);
	    }

	    CMBControllerServlet.valueAccumulator.deleteAllCounters();
	    
	    return messageFound;
    }
    
    
    private List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> getSubscriptionsForTopic(String topicArn) throws Exception {        
    	
    	List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> subInfoList = new ArrayList<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo>();
    	
    	if (CMBProperties.getInstance().isUseSubInfoCache()) {
    	    subInfoList.addAll(CNSCachedEndpointPublishJob.getSubInfos(topicArn));
    	} else {
    	
    		String nextToken = null;
    	    
    		while (true) {
    	    
    			List<CNSSubscription> subscriptions = subscriptionPersistence.listSubscriptionsByTopic(nextToken, topicArn, null, 1000);
    	        
    			if (subscriptions == null || subscriptions.size() == 0) {
    	            break;
    	        }
    	        
    			boolean allPendingConfirmation = true;
    	        
    			for (CNSSubscription subscription: subscriptions) {
    	        
    				if (!subscription.getArn().equals("PendingConfirmation")) {
    	                allPendingConfirmation = false;
    	                subInfoList.add(new CNSEndpointPublishJob.CNSEndpointSubscriptionInfo(subscription.getProtocol(), subscription.getEndpoint(), subscription.getArn()));
    	                nextToken = subscription.getArn();
    	            }
    	        }
    			
    	        if (allPendingConfirmation) {
    	            break;
    	        }
    	    }
    	}
    	
    	return subInfoList;
    }
    
    private List<CNSEndpointPublishJob> createEndpointPublishJobs(CNSMessage message, List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> subscriptions) {

    	List<CNSEndpointPublishJob> epPublishJobs = new ArrayList<CNSEndpointPublishJob>();
    	
    	if (subscriptions != null) {
	    
    		int maxSubsPerEPPublishJob = CMBProperties.getInstance().getMaxSubscriptionsPerEPPublishJob();
	    	int numEPPublishJobs = (int)Math.ceil(subscriptions.size()/(float)maxSubsPerEPPublishJob);
	    	int subIndex = 0;
	    	
	    	for (int i=0; i<numEPPublishJobs; i++) {
	    		
	    		List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> epPublishJobSubscriptions = new ArrayList<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo>();
	    		
	    		for (int j=0; j<maxSubsPerEPPublishJob; j++) {
	    		    
	    			CNSEndpointSubscriptionInfo subInfo;
	    		    
	    			if (CMBProperties.getInstance().isUseSubInfoCache()) {
	    		        subInfo = new CNSCachedEndpointPublishJob.CNSCachedEndpointSubscriptionInfo(subscriptions.get(subIndex).protocol, subscriptions.get(subIndex).endpoint, subscriptions.get(subIndex).subArn);
	    		    } else {
	    		        subInfo = new CNSEndpointSubscriptionInfo(subscriptions.get(subIndex).protocol, subscriptions.get(subIndex).endpoint, subscriptions.get(subIndex).subArn);
	    		    }
	    		     
	    			epPublishJobSubscriptions.add(subInfo);
	    			
	    			if (++subIndex==subscriptions.size()) {
	    				break;
	    			}
	    		}
	    		
	    		CNSEndpointPublishJob job;
	    		
	    		if (CMBProperties.getInstance().isUseSubInfoCache()) {
	    		    job = new CNSCachedEndpointPublishJob(message, epPublishJobSubscriptions);
	    		} else {
	    		    job = new CNSEndpointPublishJob(message, epPublishJobSubscriptions);
	    		}
	    		
	    		epPublishJobs.add(job);
	    	}
    	}
        
    	return epPublishJobs;
    }
}
