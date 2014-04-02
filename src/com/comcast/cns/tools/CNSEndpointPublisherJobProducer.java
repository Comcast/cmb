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

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
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
import com.comcast.cqs.model.CQSMessage;

/**
 * This class represents the Producer that reads the CNSMessage, partitions the subscribers and
 * creates the EndpointPublishJobs
 * @author aseem, bwolf, ppang
 *
 * Class is thread-safe
 */

public class CNSEndpointPublisherJobProducer implements CNSPublisherPartitionRunnable {
	
    private static Logger logger = Logger.getLogger(CNSEndpointPublisherJobProducer.class);
    
    private static final String CNS_PRODUCER_QUEUE_NAME_PREFIX = CMBProperties.getInstance().getCNSPublishQueueNamePrefix();
    
    private static volatile boolean initialized = false; 
    
    private static volatile ICNSSubscriptionPersistence subscriptionPersistence = PersistenceFactory.getSubscriptionPersistence(); 

    private long processingDelayMillis = 10;

   /* 
    * Read the EndpointPublishQ_<m> property and ensuring they exist (create if not) 
    * @throws PersistenceException 
    */
   public static void initialize() throws PersistenceException {
	   if (initialized) {
		   return;
	   }
	   logger.info("event=initializing_cns_producer");
	   initialized = true;
   }

   public static void shutdown() {
       initialized = false;
       logger.info("event=shutdown_cns_producer");
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
		        	values.put("dataCenter", CMBProperties.getInstance().getCMBDataCenter());
	                CNSPublisher.cassandraHandler.insertRow(CMBProperties.getInstance().getCNSKeyspace(), hostAddress, "CNSWorkers", values, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, null);
	            } catch (Exception ex) {
	        		logger.warn("event=ping_glitch", ex);
	        	}
            }
	        
	        String publishJobQName = CNS_PRODUCER_QUEUE_NAME_PREFIX + partition;
	        String queueUrl = CQSHandler.getRelativeCnsInternalQueueUrl(publishJobQName);
	        
	        CQSMessage msg = null;
	        int waitTimeSecs = 0;
	        
	        if (CMBProperties.getInstance().isCQSLongPollEnabled()) {
	        	waitTimeSecs = CMBProperties.getInstance().getCMBRequestTimeoutSec();
	        }

	        List<CQSMessage> l = CQSHandler.receiveMessage(queueUrl, waitTimeSecs, 1); 

	        if (l.size() > 0) {
        		msg = l.get(0);
        	}

	        CNSWorkerMonitor.getInstance().registerCQSServiceAvailable(true);

	        if (msg != null) {
	        	
	        	// if long polling disabled and message found reset exponential backoff
	        	
	        	if (!CMBProperties.getInstance().isCQSLongPollEnabled()) {
	        		processingDelayMillis = 10;
	        	}
	        	
	            CNSMessage publishMessage = CNSMessage.parseInstance(msg.getBody());
	            
	            int messageExpirationSeconds = CMBProperties.getInstance().getCNSMessageExpirationSeconds();
	            
	            if (messageExpirationSeconds != 0 && System.currentTimeMillis() - publishMessage.getTimestamp().getTime() > messageExpirationSeconds*1000) {
	                logger.error("event=deleting_publish_job reason=message_too_old topic_arn=" + publishMessage.getTopicArn());
	                CQSHandler.deleteMessage(queueUrl, msg.getReceiptHandle());
	                CMBControllerServlet.valueAccumulator.deleteAllCounters();
	                return true; // return true to avoid backoff
	            }
	            
	            List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> subscriptions = null;
	            long t1=System.currentTimeMillis();
	            try {
	                subscriptions = getSubscriptionsForTopic(publishMessage.getTopicArn());
	            } catch (TopicNotFoundException e) {

	            	//delete this message/job since the topic was deleted.
	                
	            	logger.error("event=deleting_publish_job reason=topic_not_found topic_arn=" + publishMessage.getTopicArn());
	                CQSHandler.deleteMessage(queueUrl, msg.getReceiptHandle());
	                CMBControllerServlet.valueAccumulator.deleteAllCounters();
	                return true; // return true to avoid backoff
	            
	            } catch (Exception ex) {
	                logger.error("event=skipping_publish_job reason=error_fetching_subscriptions", ex);
	                CMBControllerServlet.valueAccumulator.deleteAllCounters();
	                return true; // return true to avoid backoff
	            }
	            logger.debug("event=get_subscription_list ms="+(System.currentTimeMillis()-t1));
	            if (subscriptions != null && subscriptions.size() > 0) {
	            	
	                messageFound = true;
	                List<CNSEndpointPublishJob> epPublishJobs = createEndpointPublishJobs(publishMessage, subscriptions);
	                
	                for (CNSEndpointPublishJob epPublishJob: epPublishJobs) {
	                	
	                	String epQueueName =  CMBProperties.getInstance().getCNSEndpointPublishQueueNamePrefix() + ((new Random()).nextInt(CMBProperties.getInstance().getCNSNumEndpointPublishJobQueues()));
	                    String epQueueUrl = CQSHandler.getRelativeCnsInternalQueueUrl(epQueueName);
	                    CQSHandler.sendMessage(epQueueUrl, epPublishJob.serialize());
	                }
	            }
	            
	            CQSHandler.deleteMessage(queueUrl, msg.getReceiptHandle());
	            
	            long ts2 = System.currentTimeMillis();
	            logger.debug("event=processed_producer_job cns_cqs_ms=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime) + " resp_ms=" + (ts2 - ts1));

	        } else {
	        	
	        	// if long polling disabled and no message found do exponential backoff
	        	
	        	if (!CMBProperties.getInstance().isCQSLongPollEnabled()) {
	        		
		        	if (processingDelayMillis < CMBProperties.getInstance().getCNSProducerProcessingMaxDelay()) {
		        		processingDelayMillis *= 2;
		        	}
		        	
	        		Thread.sleep(processingDelayMillis);
	        	}
	        	
	        }
	    
	    } catch (Exception ex) {
	    	logger.error("event=job_producer_failure", ex);
    		CQSHandler.ensureQueuesExist(CNS_PRODUCER_QUEUE_NAME_PREFIX, CMBProperties.getInstance().getCNSNumPublishJobQueues());
	    }

	    CMBControllerServlet.valueAccumulator.deleteAllCounters();
	    
	    return messageFound;
    }
    
    
    public static List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> getSubscriptionsForTopic(String topicArn) throws Exception {        
    	
    	List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> subInfoList = new ArrayList<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo>();
    	
    	if (CMBProperties.getInstance().isCNSUseSubInfoCache()) {
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
    	                subInfoList.add(new CNSEndpointPublishJob.CNSEndpointSubscriptionInfo(subscription.getProtocol(), subscription.getEndpoint(), subscription.getArn(), subscription.getRawMessageDelivery()));
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
    
    public static List<CNSEndpointPublishJob> createEndpointPublishJobs(CNSMessage message, List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> subscriptions) {

    	List<CNSEndpointPublishJob> epPublishJobs = new ArrayList<CNSEndpointPublishJob>();
    	
    	if (subscriptions != null) {
	    
    		int maxSubsPerEPPublishJob = CMBProperties.getInstance().getCNSMaxSubscriptionsPerEndpointPublishJob();
	    	int numEPPublishJobs = (int)Math.ceil(subscriptions.size()/(float)maxSubsPerEPPublishJob);
	    	int subIndex = 0;
	    	
	    	for (int i=0; i<numEPPublishJobs; i++) {
	    		
	    		List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> epPublishJobSubscriptions = new ArrayList<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo>();
	    		
	    		for (int j=0; j<maxSubsPerEPPublishJob; j++) {
	    		    
	    			CNSEndpointSubscriptionInfo subInfo;
	    		    
	    			if (CMBProperties.getInstance().isCNSUseSubInfoCache()) {
	    		        subInfo = new CNSCachedEndpointPublishJob.CNSCachedEndpointSubscriptionInfo(subscriptions.get(subIndex).protocol, subscriptions.get(subIndex).endpoint, subscriptions.get(subIndex).subArn, subscriptions.get(subIndex).rawDelivery);
	    		    } else {
	    		        subInfo = new CNSEndpointSubscriptionInfo(subscriptions.get(subIndex).protocol, subscriptions.get(subIndex).endpoint, subscriptions.get(subIndex).subArn, subscriptions.get(subIndex).rawDelivery);
	    		    }
	    		     
	    			epPublishJobSubscriptions.add(subInfo);
	    			
	    			if (++subIndex==subscriptions.size()) {
	    				break;
	    			}
	    		}
	    		
	    		CNSEndpointPublishJob job;
	    		
	    		if (CMBProperties.getInstance().isCNSUseSubInfoCache()) {
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
