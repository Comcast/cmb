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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.io.IEndpointPublisher;
import com.comcast.cns.model.CNSEndpointPublishJob;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSRetryPolicy;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.model.CNSEndpointPublishJob.CNSEndpointSubscriptionInfo;
import com.comcast.cns.model.CNSRetryPolicy.CnsBackoffFunction;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.persistence.ICNSAttributesPersistence;
import com.comcast.cns.tools.CNSEndpointPublisherJobConsumer;
import com.comcast.cns.tools.CNSPublishJob;
import com.comcast.cns.tools.CNSWorkerMonitor;
import com.amazonaws.services.sqs.model.Message;

public class CNSEndpointPublisherConsumerTest {

	static Logger logger = Logger.getLogger(CNSEndpointPublisherConsumerTest.class);
    
    static AmazonSQS origInst;
    static boolean useSubInfoCacheSetting;
    
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        CNSEndpointPublisherJobConsumer.initialize();
        origInst = CNSEndpointPublisherJobConsumer.TestInterface.getSQS();
        CNSEndpointPublisherJobConsumer.TestInterface.clearDeliveryHandlerQueue();
        CNSEndpointPublisherJobConsumer.TestInterface.clearReDeliveryHandlerQueue();
        useSubInfoCacheSetting = CMBProperties.getInstance().isCNSUseSubInfoCache();
        CMBProperties.getInstance().setCNSUseSubInfoCache(false);
   }
    
    @After
    public void tearDown() {
        CNSEndpointPublisherJobConsumer.TestInterface.clearDeliveryHandlerQueue();
        CNSEndpointPublisherJobConsumer.TestInterface.clearReDeliveryHandlerQueue();
        CNSEndpointPublisherJobConsumer.shutdown();
        CNSEndpointPublisherJobConsumer.TestInterface.setAmazonSQS(origInst);
        CNSEndpointPublisherJobConsumer.TestInterface.setTestQueueLimit(null);
        CNSPublishJob.testPublisher = null;         
        CMBProperties.getInstance().setCNSUseSubInfoCache(useSubInfoCacheSetting);
    }

        
    static class TestAmazonSQS implements AmazonSQS {
       volatile int numRecvMsgCallsBeforeMessage = 0;
       volatile String messageBody;
       volatile String messageId;
       volatile String receiptHandle;
       volatile String deleteMessageReceiptHandle;
       volatile boolean infiniteMessagesToRecv = false;
       volatile int numRecvMsgCalls = 0;
       
       volatile int changeMessageVisibilityDelay = 0;

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

            changeMessageVisibilityDelay += changeMessageVisibilityRequest.getVisibilityTimeout();
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
            return null;
        }

        @Override
        public ReceiveMessageResult receiveMessage(
                ReceiveMessageRequest receiveMessageRequest)
                throws AmazonServiceException, AmazonClientException {
            
            numRecvMsgCalls++;
            ReceiveMessageResult res = new ReceiveMessageResult();
            
            if (infiniteMessagesToRecv || numRecvMsgCallsBeforeMessage-- == 0) {
                Message msg = new Message();
                msg.setBody(messageBody);
                msg.setMessageId(messageId);
                msg.setReceiptHandle(receiptHandle);
                res.setMessages(Arrays.asList(msg));
            } else {
                res.setMessages(Collections.<Message> emptyList());
            }
            
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
            deleteMessageReceiptHandle = deleteMessageRequest.getReceiptHandle();
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
    
    public static class TestEndpointPublisher implements IEndpointPublisher {

        public volatile boolean error = false;
        public volatile int totalSent = 0;
        public volatile boolean dontCareEndPointToMessage = false;
        
        public volatile int numFailuresBeforeSuccess = 0;
        public volatile int numTries = 0;
        public HashMap<Integer, Long> numTryToTS = new HashMap<Integer, Long>();
        public volatile long sleepBeforeSending = 0;
        
        HashMap<String, String> endpointToMessage = new HashMap<String, String>();

        public TestEndpointPublisher(HashMap<String, String> endPointToMessageP) {
            endpointToMessage = endPointToMessageP;
        }
        
        String endpoint = null;
        
        @Override
        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public String getEndpoint() {
            return endpoint;
        }

        String msg;

        @Override
        public void setMessage(String message) {
            msg = message;
            
        }

        @Override
        public String getMessage() {
            return msg;
        }

        @Override
        public void setUser(User user) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public User getUser() {
            // TODO Auto-generated method stub
            return null;
        }

        String subject;
        
        @Override
        public void setSubject(String subject) {
            this.subject = subject;
            
        }

        @Override
        public String getSubject() {
            return subject;
        }

        @Override
        public void send() throws Exception {
        
            logger.debug("TestEndpointPublisher send called");
            numTryToTS.put(numTries, System.currentTimeMillis());
            
            if (numTries++ < numFailuresBeforeSuccess) {
                throw new Exception("debug failed");
            }
            
            if (!dontCareEndPointToMessage) {
            
                if (!endpointToMessage.containsKey(endpoint)) {
                    fail("No response specified for endpoint:" + endpoint);
                }
                
                if (!endpointToMessage.get(endpoint).equals(msg)) {
                    fail("Message for endpoint" + endpoint + " expected to be:" + endpointToMessage.get(endpoint) + ", but got:" + msg);
                }
            }
            
            logger.debug("sleeping for " + sleepBeforeSending + " MS");
            Thread.sleep(sleepBeforeSending);
            totalSent++;
            logger.debug("this=" + this + " totalSent=" + totalSent + " endpoint=" + endpoint);
        }
    }
    
    class TestAttributePersustence implements ICNSAttributesPersistence {

        CNSSubscriptionAttributes subAttr;
        String subArn;
        
        @Override
        public void setTopicAttributes(CNSTopicAttributes topicAttributes, String topicArn) throws Exception {
        }

        @Override
        public CNSTopicAttributes getTopicAttributes(String topicArn) throws Exception {
            return null;
        }

        @Override
        public void setSubscriptionAttributes(CNSSubscriptionAttributes subscriptionAtributes, String subscriptionArn) throws Exception {
            subAttr = subscriptionAtributes;
            subArn = subscriptionArn;            
        }

        @Override
        public CNSSubscriptionAttributes getSubscriptionAttributes(String subscriptionArn) throws Exception {
            return subAttr;
        }
    }

    
    @Test
    public void testOnePassWithMessage() throws Exception {
        TestAmazonSQS testSqs = new TestAmazonSQS();
        CNSEndpointPublisherJobConsumer.TestInterface.setAmazonSQS(testSqs);

        CNSMessage msg = new CNSMessage();
        msg.generateMessageId();
        msg.setUserId("testUserId");
        msg.setTimestamp(new Date());
        msg.setMessage("test message");
        msg.setMessageStructure(null);
        msg.setSubject("test subject"); //will only be applicable for email
        msg.setTopicArn("testTopicArm");

        CNSEndpointSubscriptionInfo subInfo = new CNSEndpointSubscriptionInfo(CnsSubscriptionProtocol.http, "test-endpointOnePass", "test-sub-arn");
        CNSEndpointPublishJob epPubJob = new CNSEndpointPublishJob(msg, Arrays.asList(subInfo));
        
        HashMap<String, String> endpointtToMsg = new HashMap<String, String>();
        endpointtToMsg.put("test-endpointdefault", "test-messagedefault");
        endpointtToMsg.put("test-endpointemail", "test-messageemail");
        endpointtToMsg.put("test-endpointemail_json", "test-messageemail_json");
        endpointtToMsg.put("test-endpointhttp", "test-messagehttp");
        endpointtToMsg.put("test-endpointhttps", "test-messagehttps");
        endpointtToMsg.put("test-endpointcqs", "test-messagecqs");
        
        TestEndpointPublisher testPublisher = new TestEndpointPublisher(endpointtToMsg); //will accumulate all notifications
        testPublisher.dontCareEndPointToMessage = true;
        CNSPublishJob.testPublisher = testPublisher;         
        
        testSqs.messageBody = epPubJob.serialize();
        testSqs.messageId = "test-message-id1";
        testSqs.receiptHandle = "test-receiptHandle";
        
        CNSEndpointPublisherJobConsumer consumer = new CNSEndpointPublisherJobConsumer();
        consumer.run(0);
        
        Thread.sleep(1000);
        //ensure one http notification was sent
        if (testPublisher.totalSent != 1) {
            fail("Expected 1 notification. Got:" +testPublisher.totalSent);
        }
        
        //ensure deleteMessage was called on the EndpointPublishJob
        if (testSqs.deleteMessageReceiptHandle == null || !testSqs.deleteMessageReceiptHandle.equals("test-receiptHandle")) {
            fail("Expected to delete message from SQS. DeleteMessage was not called");
        }
    }
    
    @Test
    public void testOnePassWithNoMessage() throws Exception {
        //add job to PublishJobQ
        TestAmazonSQS testSqs = new TestAmazonSQS();
        CNSEndpointPublisherJobConsumer.TestInterface.setAmazonSQS(testSqs);
        
        HashMap<String, String> endpointtToMsg = new HashMap<String, String>();
        endpointtToMsg.put("test-endpointdefault", "test-messagedefault");
        endpointtToMsg.put("test-endpointemail", "test-messageemail");
        endpointtToMsg.put("test-endpointemail_json", "test-messageemail_json");
        endpointtToMsg.put("test-endpointhttp", "test-messagehttp");
        endpointtToMsg.put("test-endpointhttps", "test-messagehttps");
        endpointtToMsg.put("test-endpointcqs", "test-messagecqs");
        
        TestEndpointPublisher testPublisher = new TestEndpointPublisher(endpointtToMsg); //will accumulate all notifications
        testPublisher.dontCareEndPointToMessage = true;
        CNSPublishJob.testPublisher = testPublisher;         
        
        testSqs.numRecvMsgCallsBeforeMessage = 1;
        
        CNSEndpointPublisherJobConsumer consumer = new CNSEndpointPublisherJobConsumer();
        consumer.run(0);
        
        Thread.sleep(1000);
        //ensure one http notification was sent
        if (testPublisher.totalSent != 0) {
            fail("Expected 0 notification. Got:" +testPublisher.totalSent);
        }
        
        //ensure deleteMessage was called on the EndpointPublishJob
        if (testSqs.deleteMessageReceiptHandle != null) {
            fail("Expected to not get any delete message from SQS. DeleteMessage was called");
        }        
    }
    
    @Test
    public void testOnePassWithMessagesOverLoaded() throws Exception {
        TestAmazonSQS testSqs = new TestAmazonSQS();
        CNSEndpointPublisherJobConsumer.TestInterface.setAmazonSQS(testSqs);
        CNSEndpointPublisherJobConsumer.TestInterface.setTestQueueLimit(1);

        CNSMessage msg = new CNSMessage();
        msg.generateMessageId();
        msg.setTimestamp(new Date());
        msg.setUserId("testUserId");
        msg.setMessage("test message");
        msg.setMessageStructure(null);
        msg.setSubject("test subject"); //will only be applicable for email
        msg.setTopicArn("testTopicArm");

        CNSEndpointSubscriptionInfo subInfo = new CNSEndpointSubscriptionInfo(CnsSubscriptionProtocol.http, "test-endpointOnePassOverloaded", "test-sub-arn");
        CNSEndpointPublishJob epPubJob = new CNSEndpointPublishJob(msg, Arrays.asList(subInfo));
        
        HashMap<String, String> endpointtToMsg = new HashMap<String, String>();
        endpointtToMsg.put("test-endpointdefault", "test-messagedefault");
        endpointtToMsg.put("test-endpointemail", "test-messageemail");
        endpointtToMsg.put("test-endpointemail_json", "test-messageemail_json");
        endpointtToMsg.put("test-endpointhttp", "test-messagehttp");
        endpointtToMsg.put("test-endpointhttps", "test-messagehttps");
        endpointtToMsg.put("test-endpointcqs", "test-messagecqs");
        
        TestEndpointPublisher testPublisher = new TestEndpointPublisher(endpointtToMsg); //will accumulate all notifications
        testPublisher.dontCareEndPointToMessage = true;
        testPublisher.sleepBeforeSending = 20000;
        CNSPublishJob.testPublisher = testPublisher;         
        
        testSqs.messageBody = epPubJob.serialize();
        testSqs.messageId = "test-message-id1";
        testSqs.receiptHandle = "test-receiptHandle";
        testSqs.infiniteMessagesToRecv = true;
        
        CNSEndpointPublisherJobConsumer consumer = new CNSEndpointPublisherJobConsumer();

        for (int i = 0; i < CMBProperties.getInstance().getCNSNumPublisherDeliveryHandlers() + 2; i++) {
            consumer.run(0);
        }

        //check server is overloaded and consumer is no longer polling for messages
        //Note: sometimes the numRecvMsgCalls may be off by one
        if (testSqs.numRecvMsgCalls != CMBProperties.getInstance().getCNSNumPublisherDeliveryHandlers() && 
                testSqs.numRecvMsgCalls != CMBProperties.getInstance().getCNSNumPublisherDeliveryHandlers() + 1 &&
                testSqs.numRecvMsgCalls != CMBProperties.getInstance().getCNSNumPublisherDeliveryHandlers() - 1) {
            fail("Expected num recv calls=" + CMBProperties.getInstance().getCNSNumPublisherDeliveryHandlers() + " got:" + testSqs.numRecvMsgCalls);
        }
    }    
    
    @Test
    public void testRetryNoDelay() throws Exception {

        HashMap<String, String> endpointtToMsg = new HashMap<String, String>();
        endpointtToMsg.put("test-endpointdefault", "test message");
        endpointtToMsg.put("test-endpointemail", "test message");
        endpointtToMsg.put("test-endpointemail_json", "test message");
        endpointtToMsg.put("test-endpointhttp", "test message");
        endpointtToMsg.put("test-endpointhttps", "test message");
        endpointtToMsg.put("test-endpointcqs", "test message");
        TestEndpointPublisher testPub = new TestEndpointPublisher(endpointtToMsg); //will accumulate all notifications
        testPub.dontCareEndPointToMessage = true;

        CNSPublishJob.testPublisher = testPub;

        PersistenceFactory.cnsAttributePersistence = new TestAttributePersustence();
        CNSSubscriptionAttributes subAttr = new CNSSubscriptionAttributes();

        CNSSubscriptionDeliveryPolicy deliveryPolicy = new CNSSubscriptionDeliveryPolicy();
        CNSRetryPolicy retryPolicy = new CNSRetryPolicy();
        retryPolicy.setNumNoDelayRetries(3);
        retryPolicy.setNumRetries(3);

        deliveryPolicy.setHealthyRetryPolicy(retryPolicy);
        subAttr.setEffectiveDeliveryPolicy(deliveryPolicy);

        PersistenceFactory.cnsAttributePersistence.setSubscriptionAttributes(subAttr, "test-arn");

        //-----above is all setup
        CNSMessage msg = new CNSMessage();
        msg.generateMessageId();
        msg.setTimestamp(new Date());
        msg.setUserId("test");
        msg.setMessage("test message" );
        msg.setSubject("test subject"); //will only be applicable for email
        msg.setTopicArn("testTopicArm");
        User user = new User("test-user-id", "test-user-name", "test-hashed-password", "test-access-key", "test-access-secret");

        testPub.numFailuresBeforeSuccess = 2;

        AtomicInteger endpointPublishJobCount = new AtomicInteger(100);
        CNSPublishJob job = new CNSPublishJob(msg, user, CnsSubscriptionProtocol.http, "http://bogus", "test-sub-arn", "test-queue-url", "test-receipt-handle", endpointPublishJobCount);
        job.doRetry(testPub, CnsSubscriptionProtocol.http, "http://bogus", "test-sub-arn");

        if (testPub.totalSent != 1) {
            fail("should have sent one right away. But didn't");
        }

        testPub = new TestEndpointPublisher(endpointtToMsg); //will accumulate all notifications
        testPub.dontCareEndPointToMessage = true;
        testPub.numFailuresBeforeSuccess = 3; //set num-failures past the numRetries and see publisher declare failure and give up

        job.doRetry(testPub, CnsSubscriptionProtocol.http, "http://bogus", "test-sub-arn");

        if (testPub.totalSent != 0) {
            fail("should have sent one right away. But didn't");
        }
    }
    
    @Test
    public void testRetryMinDelays() throws Exception {
        
        CMBProperties.getInstance().publishJobQueueSizeLimit = 10;
        HashMap<String, String> endpointtToMsg = new HashMap<String, String>();
        endpointtToMsg.put("test-endpointdefault", "test message");
        endpointtToMsg.put("test-endpointemail", "test message");
        endpointtToMsg.put("test-endpointemail_json", "test message");
        endpointtToMsg.put("test-endpointhttp", "test message");
        endpointtToMsg.put("test-endpointhttps", "test message");
        endpointtToMsg.put("test-endpointcqs", "test message");
        TestEndpointPublisher testPub = new TestEndpointPublisher(endpointtToMsg); //will accumulate all notifications
        testPub.dontCareEndPointToMessage = true;
        testPub.numFailuresBeforeSuccess = 2; //set num-failures past the numRetries and see publisher declare failure and give up

        CNSPublishJob.testPublisher = testPub;
        
        PersistenceFactory.cnsAttributePersistence = new TestAttributePersustence();
        CNSSubscriptionAttributes subAttr = new CNSSubscriptionAttributes();
        
        CNSSubscriptionDeliveryPolicy deliveryPolicy = new CNSSubscriptionDeliveryPolicy();
        CNSRetryPolicy retryPolicy = new CNSRetryPolicy();
        deliveryPolicy.setHealthyRetryPolicy(retryPolicy);
        subAttr.setEffectiveDeliveryPolicy(deliveryPolicy);
        
        PersistenceFactory.cnsAttributePersistence.setSubscriptionAttributes(subAttr, "test-arn");

        retryPolicy.setNumNoDelayRetries(0);
        retryPolicy.setNumMinDelayRetries(3);
        retryPolicy.setMinDelayTarget(1); //1 sec each
        retryPolicy.setNumRetries(3);
        
        CNSMessage msg = new CNSMessage();
        msg.generateMessageId();
        msg.setTimestamp(new Date());
        msg.setUserId("test");
        msg.setMessage("test message" );
        msg.setSubject("test subject"); //will only be applicable for email
        msg.setTopicArn("testTopicArm");
        User user = new User("test-user-id", "test-user-name", "test-hashed-password", "test-access-key", "test-access-secret");

        AtomicInteger endpointPublishJobCount = new AtomicInteger(100); //never delete
        CNSPublishJob job = new CNSPublishJob(msg, user, CnsSubscriptionProtocol.http, "http://bogus", "test-sub-arn", "test-queue-url", "test-receipt-handle", endpointPublishJobCount);
        job.doRetry(testPub, CnsSubscriptionProtocol.http, "http://bogus", "test-sub-arn");
        
        Thread.sleep(5000);
        
        //check that it took 2 tries, 1 second each => atleast 2 seconds before one notification was wsent
                
        if (testPub.totalSent != 1) {
            fail("Should have sent one notification. Instead got" + testPub.totalSent);
        }
        
        if (job.numRetries != 3) {
            fail("should have made 3 retries. Instead got" + job.numRetries);
        }
        
        //check that each of the retries were atleast 1 second and no more than 2 seconds apart.
        if (testPub.numTryToTS.size() != 3) {
            fail("Should have had three tries");
        }
        
        for (int i = 0; i < testPub.numTryToTS.size() - 1; i++) {

            if ((testPub.numTryToTS.get(i + 1) - testPub.numTryToTS.get(i)) < 1000 || (testPub.numTryToTS.get(i + 1) - testPub.numTryToTS.get(i)) > 1500) {
                fail("Time delay betwee try " + i + " and the next one was either less than 1 sec or more than 2");
            }
        }
    }
    
    @Test
    public void testBackOfFunctionLinear() throws Exception {
        
        int delay = com.comcast.cns.util.Util.getNextRetryDelay(1, 10, 0, 10, CnsBackoffFunction.linear); 
        
        if (delay != 0) {
            fail("expected 1 sec delay. Got" + delay);
        }
        
        delay = com.comcast.cns.util.Util.getNextRetryDelay(10, 10, 5, 260, CnsBackoffFunction.linear); 
        
        if ( delay < 259 || delay > 261) {
            fail("expected 260 sec delay with tolerence of 1 sec. Got" + delay);
        }
    }
    
    @Test
    public void testBackOfFunctionExponential() throws Exception {
        
        if (com.comcast.cns.util.Util.getNextRetryDelay(1, 10, 5, 260, CnsBackoffFunction.exponential) != 5) {
            fail("expected 5 sec delay");
        }
        
        int delay = com.comcast.cns.util.Util.getNextRetryDelay(10, 10, 5, 260, CnsBackoffFunction.exponential); 
        
        if ( delay < 259 || delay > 261) {
            fail("expected 260 sec delay with tolerence of 1 sec. Got" + delay);
        }
    }
    
    @Test
    public void testBackOfFunctionGeometric() throws Exception {
        
        int delay = com.comcast.cns.util.Util.getNextRetryDelay(1, 10, 5, 260, CnsBackoffFunction.geometric);
        
        if (delay != 5) {
            fail("Expected min to be 5. Got:" + delay);
        }
        
        delay = com.comcast.cns.util.Util.getNextRetryDelay(10, 10, 5, 260, CnsBackoffFunction.geometric);
        
        if ( delay < 259 || delay > 261) {
            fail("expected 260 sec delay with tolerence of 1 sec. Got" + delay);
        }
    }
    
    @Test
    public void testBackOfFunctionArithmetic() throws Exception {
        
        int delay = com.comcast.cns.util.Util.getNextRetryDelay(1, 10, 5, 260, CnsBackoffFunction.arithmetic);
        
        if (delay != 5) {
            fail("Expected min to be 5. Got:" + delay);
        }
        
        delay = com.comcast.cns.util.Util.getNextRetryDelay(10, 10, 5, 260, CnsBackoffFunction.arithmetic);
        
        if ( delay < 259 || delay > 261) {
            fail("expected 260 sec delay with tolerence of 1 sec. Got" + delay);
        }
        
        delay = com.comcast.cns.util.Util.getNextRetryDelay(5, 5, 1, 20, CnsBackoffFunction.arithmetic);
        logger.info("delay=" + delay);
    }
    
    //Test that each of the backoff functions are bounded in the order:
    //linear > arithmetic > geometric > exponential
    @Test
    public void testRetryBackOffBounds() throws Exception {

        HashMap<CnsBackoffFunction, Integer> fnToTotalBackoff = new HashMap<CnsBackoffFunction, Integer>();
        
        for (int j = 0; j < CnsBackoffFunction.values().length; j++) {
            
            CnsBackoffFunction fn = CnsBackoffFunction.values()[j];
            
            for (int i = 1; i <= 5; i++) {
                
                int delay = com.comcast.cns.util.Util.getNextRetryDelay(i, 5, 1, 10, fn);
                
                if (!fnToTotalBackoff.containsKey(fn)) {
                    fnToTotalBackoff.put(fn, 0);
                }
                
                fnToTotalBackoff.put(fn, fnToTotalBackoff.get(fn) + delay);
            }
        }
        
        //compare descending order
        
        int lastMaxDelay = Integer.MAX_VALUE;
        
        for (int j = 0; j < CnsBackoffFunction.values().length; j++) {
            
            CnsBackoffFunction fn = CnsBackoffFunction.values()[j];
            int totalDelay = fnToTotalBackoff.get(fn);
            logger.debug("totalDelay=" + totalDelay);
            
            if (totalDelay > lastMaxDelay) {
                fail("Expected the current fn's delay to be less than last max-delay./ current delay=" + totalDelay + " last max=" + lastMaxDelay);
            }
            
            lastMaxDelay = totalDelay;
        }        
    }


    @Test
    public void testRetryBackoff() throws Exception {

        TestAmazonSQS testSqs = new TestAmazonSQS();
        CNSEndpointPublisherJobConsumer.TestInterface.setAmazonSQS(testSqs);

        CMBProperties.getInstance().publishJobQueueSizeLimit = 10;
        HashMap<String, String> endpointtToMsg = new HashMap<String, String>();
        endpointtToMsg.put("test-endpointdefault", "test message");
        endpointtToMsg.put("test-endpointemail", "test message");
        endpointtToMsg.put("test-endpointemail_json", "test message");
        endpointtToMsg.put("test-endpointhttp", "test message");
        endpointtToMsg.put("test-endpointhttps", "test message");
        endpointtToMsg.put("test-endpointcqs", "test message");
        
        PersistenceFactory.cnsAttributePersistence = new TestAttributePersustence();
        CNSSubscriptionAttributes subAttr = new CNSSubscriptionAttributes();
        
        CNSSubscriptionDeliveryPolicy deliveryPolicy = new CNSSubscriptionDeliveryPolicy();
        CNSRetryPolicy retryPolicy = new CNSRetryPolicy();
        deliveryPolicy.setHealthyRetryPolicy(retryPolicy);
        subAttr.setEffectiveDeliveryPolicy(deliveryPolicy);
        
        PersistenceFactory.cnsAttributePersistence.setSubscriptionAttributes(subAttr, "test-arn");

        retryPolicy.setNumNoDelayRetries(0);
        retryPolicy.setNumMinDelayRetries(0);
        retryPolicy.setMinDelayTarget(1); 
        retryPolicy.setMaxDelayTarget(10); //10 seconds max
        retryPolicy.setNumRetries(5);
        retryPolicy.setNumMaxDelayRetries(0);
        
        CNSMessage msg = new CNSMessage();
        msg.generateMessageId();
        msg.setTimestamp(new Date());
        msg.setUserId("test");
        msg.setMessage("test message" );
        msg.setSubject("test subject"); //will only be applicable for email
        msg.setTopicArn("testTopicArm");
        User user = new User("test-user-id", "test-user-name", "test-hashed-password", "test-access-key", "test-access-secret");

        //run the job in each type of backoff function and check that the total time elapsed is
        //max for linear followed by arithmetic followeed by linear followed by exponential
     
        //exponential fn

        TestEndpointPublisher testPub = new TestEndpointPublisher(endpointtToMsg); //will accumulate all notifications
        testPub.dontCareEndPointToMessage = true;
        testPub.numFailuresBeforeSuccess = 4; //set num-failures past the numRetries and see publisher declare failure and give up
        CNSPublishJob.testPublisher = testPub;
        
        retryPolicy.setBackOffFunction(CnsBackoffFunction.exponential);

        long totalDelay = (retryPolicy.getMaxDelayTarget() * retryPolicy.getNumRetries() * 1000) / 2;
        
        CNSPublishJob.testPublisher = testPub;

        AtomicInteger endpointPublishJobCount = new AtomicInteger(100); //never delete
        CNSPublishJob job = new CNSPublishJob(msg, user, CnsSubscriptionProtocol.http, "http://bogusBackoff", "test-sub-arn", "test-queue-url", "test-receipt-handle", endpointPublishJobCount);

        job.doRetry(testPub, CnsSubscriptionProtocol.http, "http://bogusBackoff", "test-sub-arn");
        
        Thread.sleep(25000);                
        
        //assert that each of the jobs sent the notification
        
        if (testPub.totalSent != 1) {
            fail("Should have sent 1 notif. Got" + testPub.totalSent + " for backofffn:" + CnsBackoffFunction.exponential.name());
        }
        
        if (testPub.numTryToTS.size() != 5) {
            fail("Expected 5 retries. Instead got:" + testPub.numTryToTS.size());
        }
        
        if ((testPub.numTryToTS.get(4).longValue() - testPub.numTryToTS.get(0).longValue()) > totalDelay) {
            fail("Expected the total delay in callbackfn=" + CnsBackoffFunction.exponential.name() + " to be less than the last callbackfn's delay of:" + totalDelay + " fn's delay=" + 
                    (testPub.numTryToTS.get(4).longValue() - testPub.numTryToTS.get(0).longValue()));
        }
        
        totalDelay = (testPub.numTryToTS.get(4).longValue() - testPub.numTryToTS.get(0).longValue());        
    }


    @Test
    public void testBadEndpointMetric() throws Exception {

        CNSWorkerMonitor.getInstance().clearBadEndpointsState();
        CNSWorkerMonitor.getInstance().registerBadEndpoint("test-endpoint", 1, 1, "test-topic");
        CNSWorkerMonitor.getInstance().registerBadEndpoint("test-endpoint", 0, 1, "test-topic");
        CNSWorkerMonitor.getInstance().registerBadEndpoint("test-endpoint2", 0, 1, "test-topic");
        
        if (CNSWorkerMonitor.getInstance().getErrorRateForEndpoints().size() != 1) {
            fail("Expected 1 bad endpoint. got:" + CNSWorkerMonitor.getInstance().getErrorRateForEndpoints());
        }        
        
        logger.info("metric=" + CNSWorkerMonitor.getInstance().getErrorRateForEndpoints());        
    }

}

