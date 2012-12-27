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
package com.comcast.cns.test.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.model.CNSEndpointPublishJob;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.cns.tools.CNSEndpointPublisherJobProducer;

public class CNSEndpointPublisherProducerTest {
    static Logger logger = Logger.getLogger(CNSEndpointPublisherProducerTest.class);
    
    final private static String testUserId = "testUserId";
    final private static String publishQNamePrefix = CMBProperties.getInstance().getCnsPublishQueueNamePrefix();
    final private static String epPublishQNamePrefix = CMBProperties.getInstance().getCnsEndpointPublishQueueNamePrefix();
    private static AtomicInteger messageSeq = new AtomicInteger(0);
    
    static boolean useSubInfoCacheSetting;
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CNSEndpointPublisherJobProducer.TestInterface.setSQS(new TestAmazonSQS());
        CNSEndpointPublisherJobProducer.TestInterface.setSubscriptionPersistence(new TestSubscriptionPersistence());
        CNSEndpointPublisherJobProducer.TestInterface.setInitialized(true);
        useSubInfoCacheSetting = CMBProperties.getInstance().isUseSubInfoCache();
        CMBProperties.getInstance().setUseSubInfoCache(false);
   }

    @After
    public void tearDown() {
        CNSEndpointPublisherJobProducer.TestInterface.setInitialized(false);
        CMBProperties.getInstance().setUseSubInfoCache(useSubInfoCacheSetting);

    }
    
    static class TestAmazonSQS implements AmazonSQS {
        private volatile Map<String, String> publishJobs = new ConcurrentHashMap<String, String>();
        private volatile Map<String, String> epPublishJobs = new ConcurrentHashMap<String, String>();

        public Map<String, String> getPublishJobs() {
        	return publishJobs;
        }

        public void clearPublishJobs() {
        	publishJobs.clear();
        }

        public Map<String, String> getEPPublishJobs() {
        	return epPublishJobs;
        }

        public void clearEPPublishJobs() {
        	epPublishJobs.clear();
        }

         @Override
         public void setEndpoint(String endpoint)
                 throws IllegalArgumentException {
             // TODO Auto-generated method stub
             
         }

         @Override
         public void setQueueAttributes(
                 SetQueueAttributesRequest setQueueAttributesRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             
         }

         @Override
         public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(
                 ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             return null;
         }

         @Override
         public void changeMessageVisibility(
                 ChangeMessageVisibilityRequest changeMessageVisibilityRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             
         }

         @Override
         public GetQueueUrlResult getQueueUrl(
                 GetQueueUrlRequest getQueueUrlRequest)
                 throws AmazonServiceException, AmazonClientException {
             
             GetQueueUrlResult res = new GetQueueUrlResult();
             res.setQueueUrl(getQueueUrlRequest.getQueueName());
             return res;
         }

         @Override
         public void removePermission(
                 RemovePermissionRequest removePermissionRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             
         }

         @Override
         public GetQueueAttributesResult getQueueAttributes(
                 GetQueueAttributesRequest getQueueAttributesRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             return null;
         }

         @Override
         public SendMessageBatchResult sendMessageBatch(
                 SendMessageBatchRequest sendMessageBatchRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             return null;
         }

         @Override
         public void deleteQueue(DeleteQueueRequest deleteQueueRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub

         }

         @Override
         public SendMessageResult sendMessage(
                 SendMessageRequest sendMessageRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
        	 String messageId = Integer.toString(messageSeq.incrementAndGet());
        	 Map<String, String> jobQ = null;
             if (sendMessageRequest.getQueueUrl().startsWith(publishQNamePrefix)) {
            	 jobQ = publishJobs;
             } else if (sendMessageRequest.getQueueUrl().startsWith(epPublishQNamePrefix)) {
            	 jobQ = epPublishJobs;
             }
             jobQ.put(messageId, sendMessageRequest.getMessageBody());
             SendMessageResult res = new SendMessageResult();
             res.setMessageId(messageId);
             return res;
         }

         @Override
         public ReceiveMessageResult receiveMessage(
                 ReceiveMessageRequest receiveMessageRequest)
                 throws AmazonServiceException, AmazonClientException {
             
        	 Map<String, String> jobQ = null;
             if (receiveMessageRequest.getQueueUrl().startsWith(publishQNamePrefix)) {
            	 jobQ = publishJobs;
             } else if (receiveMessageRequest.getQueueUrl().startsWith(epPublishQNamePrefix)) {
            	 jobQ = epPublishJobs;
             }
        	 ReceiveMessageResult res = new ReceiveMessageResult();
        	 List<Message> messages = new ArrayList<Message>();
             for(Map.Entry<String, String> entry: jobQ.entrySet()) {
        		 Message msg = new Message();
        		 msg.setBody(entry.getValue());
        		 msg.setMessageId(entry.getKey());
        		 msg.setReceiptHandle(entry.getKey());
        		 messages.add(msg);
        		 jobQ.remove(entry.getKey());
        		 break;
        	 }
             res.setMessages(messages);
             return res;            
         }

         @Override
         public ListQueuesResult listQueues(ListQueuesRequest listQueuesRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             return null;
         }

         @Override
         public DeleteMessageBatchResult deleteMessageBatch(
                 DeleteMessageBatchRequest deleteMessageBatchRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             return null;
         }

         @Override
         public CreateQueueResult createQueue(
                 CreateQueueRequest createQueueRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             return null;
         }

         @Override
         public void addPermission(AddPermissionRequest addPermissionRequest)
                 throws AmazonServiceException, AmazonClientException {
             // TODO Auto-generated method stub
             
         }

         @Override
         public void deleteMessage(DeleteMessageRequest deleteMessageRequest) throws AmazonServiceException, AmazonClientException {
        	 Map<String, String> jobQ = null;
             if (deleteMessageRequest.getQueueUrl().startsWith(publishQNamePrefix)) {
            	 jobQ = publishJobs;
             } else if (deleteMessageRequest.getQueueUrl().startsWith(epPublishQNamePrefix)) {
            	 jobQ = epPublishJobs;
             }
             jobQ.remove(deleteMessageRequest.getReceiptHandle());
         }

         @Override
         public ListQueuesResult listQueues() throws AmazonServiceException,
                 AmazonClientException {
             // TODO Auto-generated method stub
             return null;
         }

         @Override
         public void shutdown() {
             // TODO Auto-generated method stub
             
         }

         @Override
         public ResponseMetadata getCachedResponseMetadata(
                 AmazonWebServiceRequest request) {
             // TODO Auto-generated method stub
             return null;
         }
         
     }

    static class TestSubscriptionPersistence implements ICNSSubscriptionPersistence {

    	private volatile Map<String, List<CNSSubscription>> topicSubscriptions = new ConcurrentHashMap<String, List<CNSSubscription>>();
    	
    	public void addTopicAndSubscriptions(String topicArn, List<CNSSubscription> subscriptions) {
    		topicSubscriptions.put(topicArn, subscriptions);
    	}
    	
    	public void clearAllTopicsAndSubscriptions() {
    		topicSubscriptions.clear();
    	}
    	
		@Override
		public CNSSubscription subscribe(String endpoint,
				CnsSubscriptionProtocol protocol, String topicArn, String userId)
				throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public CNSSubscription getSubscription(String arn) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<CNSSubscription> listSubscriptions(String nextToken,
				CnsSubscriptionProtocol protocol, String userId)
				throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<CNSSubscription> listAllSubscriptions(String nextToken,
				CnsSubscriptionProtocol protocol, String userId)
				throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<CNSSubscription> listSubscriptionsByTopic(String nextToken,
				String topicArn, CnsSubscriptionProtocol protocol)
				throws Exception {
			return listSubscriptionsByTopic(nextToken, topicArn, protocol, 100);
		}

		@Override
		public List<CNSSubscription> listSubscriptionsByTopic(String nextToken,
				String topicArn, CnsSubscriptionProtocol protocol, int pageSize)
				throws Exception {
			// protocol and pageSize
			if (nextToken==null) {
				return topicSubscriptions.get(topicArn);
			} else {
				return null;
			}
		}

		@Override
		public List<CNSSubscription> listAllSubscriptionsByTopic(
				String nextToken, String topicArn,
				CnsSubscriptionProtocol protocol) throws Exception {
			return listSubscriptionsByTopic(nextToken, topicArn, protocol);
		}

		@Override
		public CNSSubscription confirmSubscription(
				boolean authenticateOnUnsubscribe, String token, String topicArn)
				throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void unsubscribe(String arn) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void unsubscribeAll(String topicArn) throws Exception {
			// TODO Auto-generated method stub
			
		}

		@Override
		public long getCountSubscription(String topicArn, String columnName)
				throws Exception {
			// TODO Auto-generated method stub
			return 0;
		}
    	
    }

    @Test
    public void testNoMessages() throws Exception {

    	TestAmazonSQS testSqs = (TestAmazonSQS) CNSEndpointPublisherJobProducer.TestInterface.getSQS();
    	testSqs.clearPublishJobs();
    	testSqs.clearEPPublishJobs();
    	        
        CNSEndpointPublisherJobProducer producer = new CNSEndpointPublisherJobProducer();
        producer.run(0);
        
        Thread.sleep(1000);
        
        assertEquals(0, testSqs.getEPPublishJobs().size());
    }

    @Test
    public void testOnePublishJobProcessedPerRun() throws Exception {

    	TestAmazonSQS testSqs = (TestAmazonSQS) CNSEndpointPublisherJobProducer.TestInterface.getSQS();
    	testSqs.clearPublishJobs();
    	testSqs.clearEPPublishJobs();
    	
    	String topicArn = "testTopicArn";
    	int maxSubsPerEPJob = CMBProperties.getInstance().getMaxSubscriptionsPerEPPublishJob();
    	if (maxSubsPerEPJob < 1) 
    		fail("Expected maxSubscriptionsPerEPPublishJob to be at least 1");
    	int numSubsToCreate = maxSubsPerEPJob;
    	createSubscriptionsForTopicArn(topicArn, numSubsToCreate, true);
    	createPublishJob(topicArn);
    	createPublishJob(topicArn);

        assertEquals(2, testSqs.getPublishJobs().size());
        assertEquals(0, testSqs.getEPPublishJobs().size());

        CNSEndpointPublisherJobProducer producer = new CNSEndpointPublisherJobProducer();
        producer.run(0);
        
        Thread.sleep(1000);
        
        assertEquals(1, testSqs.getPublishJobs().size());
        assertEquals(1, testSqs.getEPPublishJobs().size());

        producer.run(0);
        
        Thread.sleep(1000);
        
        assertEquals(0, testSqs.getPublishJobs().size());
        assertEquals(2, testSqs.getEPPublishJobs().size());
    }

    @Test
    public void testMessageForTopicWithNumSubsLessThanMaxNumSubPerEPJob() throws Exception {

    	TestAmazonSQS testSqs = (TestAmazonSQS) CNSEndpointPublisherJobProducer.TestInterface.getSQS();
    	testSqs.clearPublishJobs();
    	testSqs.clearEPPublishJobs();
    	
    	String topicArn = "testTopicArn";
    	int maxSubsPerEPJob = CMBProperties.getInstance().getMaxSubscriptionsPerEPPublishJob();
    	if (maxSubsPerEPJob < 2) 
    		fail("Expected maxSubscriptionsPerEPPublishJob to be at least 2");
    	int numSubsToCreate = maxSubsPerEPJob - 1;
    	createSubscriptionsForTopicArn(topicArn, numSubsToCreate, true);
    	createPublishJob(topicArn);
        
        CNSEndpointPublisherJobProducer producer = new CNSEndpointPublisherJobProducer();
        producer.run(0);
        
        Thread.sleep(1000);
        
        assertEquals(0, testSqs.getPublishJobs().size());
        assertEquals(1, testSqs.getEPPublishJobs().size());
        int numSubsPublished = 0;
        for (Map.Entry<String, String> entry: testSqs.getEPPublishJobs().entrySet()) {
        	CNSEndpointPublishJob job = CNSEndpointPublishJob.parseInstance(entry.getValue());
        	numSubsPublished += job.getSubInfos().size();
        }
        assertEquals(numSubsToCreate, numSubsPublished);
    }

    @Test
    public void testMessageForTopicWithNumSubsEqualMaxNumSubPerEPJob() throws Exception {

    	TestAmazonSQS testSqs = (TestAmazonSQS) CNSEndpointPublisherJobProducer.TestInterface.getSQS();
    	testSqs.clearPublishJobs();
    	testSqs.clearEPPublishJobs();
    	
    	String topicArn = "testTopicArn";
    	int maxSubsPerEPJob = CMBProperties.getInstance().getMaxSubscriptionsPerEPPublishJob();
    	if (maxSubsPerEPJob < 1) 
    		fail("Expected maxSubscriptionsPerEPPublishJob to be at least 1");
    	int numSubsToCreate = maxSubsPerEPJob;
    	createSubscriptionsForTopicArn(topicArn, numSubsToCreate, true);
    	createPublishJob(topicArn);
        
        CNSEndpointPublisherJobProducer producer = new CNSEndpointPublisherJobProducer();
        producer.run(0);
        
        Thread.sleep(1000);
        
        assertEquals(0, testSqs.getPublishJobs().size());
        assertEquals(1, testSqs.getEPPublishJobs().size());
        int numSubsPublished = 0;
        for (Map.Entry<String, String> entry: testSqs.getEPPublishJobs().entrySet()) {
        	CNSEndpointPublishJob job = CNSEndpointPublishJob.parseInstance(entry.getValue());
        	numSubsPublished += job.getSubInfos().size();
        }
        assertEquals(numSubsToCreate, numSubsPublished);
    }

    @Test
    public void testMessageForTopicWithNumSubsGreaterThanMaxNumSubPerEPJob() throws Exception {

    	TestAmazonSQS testSqs = (TestAmazonSQS) CNSEndpointPublisherJobProducer.TestInterface.getSQS();
    	testSqs.clearPublishJobs();
    	testSqs.clearEPPublishJobs();
    	
    	String topicArn = "testTopicArn";
    	int maxSubsPerEPJob = CMBProperties.getInstance().getMaxSubscriptionsPerEPPublishJob();
    	if (maxSubsPerEPJob < 1) 
    		fail("Expected maxSubscriptionsPerEPPublishJob to be at least 1");
    	int numSubsToCreate = maxSubsPerEPJob + 1;
    	createSubscriptionsForTopicArn(topicArn, numSubsToCreate, true);
    	createPublishJob(topicArn);
        
        CNSEndpointPublisherJobProducer producer = new CNSEndpointPublisherJobProducer();
        producer.run(0);
        
        Thread.sleep(1000);
        
        assertEquals(0, testSqs.getPublishJobs().size());
        assertEquals(2, testSqs.getEPPublishJobs().size());
        int numSubsPublished = 0;
        for (Map.Entry<String, String> entry: testSqs.getEPPublishJobs().entrySet()) {
        	CNSEndpointPublishJob job = CNSEndpointPublishJob.parseInstance(entry.getValue());
        	numSubsPublished += job.getSubInfos().size();
        }
        assertEquals(numSubsToCreate, numSubsPublished);

        
    }
    
    @Test
    public void testMessageForTopicWithNoSubs() throws Exception {

    	TestAmazonSQS testSqs = (TestAmazonSQS) CNSEndpointPublisherJobProducer.TestInterface.getSQS();
    	testSqs.clearPublishJobs();
    	testSqs.clearEPPublishJobs();
    	
    	String topicArn = "testTopicArn";
    	int maxSubsPerEPJob = CMBProperties.getInstance().getMaxSubscriptionsPerEPPublishJob();
    	if (maxSubsPerEPJob < 1) 
    		fail("Expected maxSubscriptionsPerEPPublishJob to be at least 1");
    	int numSubsToCreate = 0;
    	createSubscriptionsForTopicArn(topicArn, numSubsToCreate, true);
    	createPublishJob(topicArn);
        
        CNSEndpointPublisherJobProducer producer = new CNSEndpointPublisherJobProducer();
        producer.run(0);
        
        Thread.sleep(1000);
        
        assertEquals(0, testSqs.getPublishJobs().size());
        assertEquals(0, testSqs.getEPPublishJobs().size());  	    	
    }
    
    void createSubscriptionsForTopicArn(String topicArn, int numberOfSubscriptions, boolean clearAllTopicsAndSubscriptions) {
    	TestSubscriptionPersistence testSubscriptionPersistence = (TestSubscriptionPersistence) CNSEndpointPublisherJobProducer.TestInterface.getSubscriptionPersistence();
    	if (clearAllTopicsAndSubscriptions) testSubscriptionPersistence.clearAllTopicsAndSubscriptions();
    	List<CNSSubscription> subscriptions = new ArrayList<CNSSubscription>();
    	for(int i=0; i<numberOfSubscriptions; i++) {
    		CNSSubscription subscription = new CNSSubscription("http://comcast.com/endpoint/"+i, CnsSubscriptionProtocol.http, topicArn, testUserId);
    		subscription.setArn("subscriptionArn-"+i);
    		subscriptions.add(subscription);
    	}
    	testSubscriptionPersistence.addTopicAndSubscriptions(topicArn, subscriptions);
    }
    
    void createPublishJob(String topicArn) {
    	CNSMessage cnsMsg = new CNSMessage();
        cnsMsg.generateMessageId();
        cnsMsg.setTimestamp(new Date());
        cnsMsg.setUserId(testUserId);
        cnsMsg.setMessage("test message");
        cnsMsg.setTopicArn(topicArn);
        CNSEndpointPublisherJobProducer.TestInterface.getSQS().sendMessage(new SendMessageRequest(publishQNamePrefix, cnsMsg.serialize()));
    }
}
