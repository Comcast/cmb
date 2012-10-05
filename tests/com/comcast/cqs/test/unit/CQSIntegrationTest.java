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
package com.comcast.cqs.test.unit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.persistence.CQSMessagePartitionedCassandraPersistence;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
import com.comcast.cqs.util.CQSErrorCodes;

public class CQSIntegrationTest {

    private static Logger logger = Logger.getLogger(CQSIntegrationTest.class);

    private AmazonSQS sqs = null;
    private AmazonSNS sns = null;
    private HashMap<String, String> attributeParams = new HashMap<String, String>();
    private User user = null;
    private User user1 = null;
    private User user2 = null;
    private List<String> randomQueueUrls = null;
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 
    
    @Before
    public void setup() throws Exception {
    	
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
        
        randomQueueUrls = new ArrayList<String>();

        try {
        	
            IUserPersistence userPersistence = new UserCassandraPersistence();
 
            user = userPersistence.getUserByName("cqs_unit_test");

            if (user == null) {
                user = userPersistence.createUser("cqs_unit_test", "cqs_unit_test");
            }

            BasicAWSCredentials credentialsUser = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());

            user1 = userPersistence.getUserByName("cqs_unit_test_1");

            if (user1 == null) {
                user1 = userPersistence.createUser("cqs_unit_test_1", "cqs_unit_test_1");
            }

            user2 = userPersistence.getUserByName("cqs_unit_test_2");

            if (user2 == null) {
                user2 = userPersistence.createUser("cqs_unit_test_2", "cqs_unit_test_2");
            }

            BasicAWSCredentials credentialsUser1 = new BasicAWSCredentials(user1.getAccessKey(), user1.getAccessSecret());

            sqs = new AmazonSQSClient(credentialsUser);
            sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());

            sns = new AmazonSNSClient(credentialsUser1);
            sns.setEndpoint(CMBProperties.getInstance().getCNSServerUrl());
            
        } catch (Exception ex) {
            logger.error("setup failed", ex);
            fail("setup failed: "+ex);
            return;
        }
        
        attributeParams.put("MessageRetentionPeriod", "600");
        attributeParams.put("VisibilityTimeout", "30");
    }
    
    @Test
    public void testSubscribeConfirmPublish() {
    	
    	try {
    	
	    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
	        
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
	        createQueueRequest.setAttributes(attributeParams);
	        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	        
	        randomQueueUrls.add(queueUrl);
	        
	        logger.info("Created queue " + queueUrl);
	        
	        AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setQueueUrl(queueUrl);
	        addPermissionRequest.setActions(Arrays.asList("SendMessage"));
	        addPermissionRequest.setLabel(UUID.randomUUID().toString());
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user1.getUserId()));        
	        sqs.addPermission(addPermissionRequest);
	        
	        String topicName = "TSTT" + randomGenerator.nextLong();
	        	
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
			CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
			String topicArn = createTopicResult.getTopicArn();
			
			logger.info("Created topic " + topicArn + ", now subscribing and confirming");
			
			SubscribeRequest subscribeRequest = new SubscribeRequest();
			String queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			subscribeRequest.setEndpoint(queueArn);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			SubscribeResult subscribeResult = sns.subscribe(subscribeRequest);
			String subscriptionArn = subscribeResult.getSubscriptionArn();
			
			if (subscriptionArn.equals("pending confirmation")) {
				
				Thread.sleep(500);
				
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
				
				List<Message> messages = receiveMessageResult.getMessages();
				
				if (messages != null && messages.size() == 1) {
					
	    			JSONObject o = new JSONObject(messages.get(0).getBody());
	    			
	    			if (!o.has("SubscribeURL")) {
	    				fail("Message is not a confirmation messsage");
	    			}
	    			
	    			String subscriptionUrl = o.getString("SubscribeURL");
	    			
	    		    URL confirmationEndpoint = new URL(subscriptionUrl);
	    		    URLConnection conn = confirmationEndpoint.openConnection();
	    		    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	    		    String inputLine;
	    		    String response = "";

    		        while ((inputLine = in.readLine()) != null) {
	    		        response += inputLine;
    		        }
	    		    
    		        logger.info(response);
    		        
    		        in.close();
    		        
    				DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
    				deleteMessageRequest.setQueueUrl(queueUrl);
    				deleteMessageRequest.setReceiptHandle(messages.get(0).getReceiptHandle());
    				
    				sqs.deleteMessage(deleteMessageRequest);
				
				} else {
					fail("No confirmation message found");
				}
			} else {
				fail("No confirmation requested");
			}
			
			logger.info("Publishing message to " + topicArn);
			
			PublishRequest publishRequest = new PublishRequest();
			String messageText = "quamvis sint sub aqua, sub aqua maledicere temptant";
			publishRequest.setMessage(messageText);
			publishRequest.setSubject("unit test message");
			publishRequest.setTopicArn(topicArn);
			sns.publish(publishRequest);
			
			Thread.sleep(2000);

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(1);
			ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
			
			List<Message> messages = receiveMessageResult.getMessages();
			
			if (messages != null && messages.size() == 1) {
				String messageBody = messages.get(0).getBody();
				assertTrue(messageBody.contains(messageText));
			} else {
				fail("No messages found");
			}
			
			DeleteTopicRequest  deleteTopicRequest = new DeleteTopicRequest(topicArn);
			sns.deleteTopic(deleteTopicRequest);
			
    	} catch (Exception ex) {
    		fail(ex.toString());
    	}
    }
    
    @Test
    public void testGetSetQueueAttributes() throws PersistenceException, InterruptedException {
    	
    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
        createQueueRequest.setAttributes(attributeParams);
        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(queueUrl);
        
        logger.info("Created queue " + queueUrl + ", now setting attributes");
        
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest();
        getQueueAttributesRequest.setQueueUrl(queueUrl);
        getQueueAttributesRequest.setAttributeNames(Arrays.asList("VisibilityTimeout", "MessageRetentionPeriod", "All"));
        GetQueueAttributesResult result = sqs.getQueueAttributes(getQueueAttributesRequest);
        assertTrue(result.getAttributes().get("MessageRetentionPeriod").equals("600"));
        assertTrue(result.getAttributes().get("VisibilityTimeout").equals("30"));
        
        SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest();
        setQueueAttributesRequest.setQueueUrl(queueUrl);
        HashMap<String, String> attributes = new HashMap<String, String>();
        attributes.put("MessageRetentionPeriod", "300");
        attributes.put("VisibilityTimeout", "80");
        attributes.put("MaximumMessageSize", "10240");
        attributes.put("DelaySeconds", "100");
        String policy = "{\"Version\":\"2008-10-17\",\"Id\":\""+queueUrl+"/SQSDefaultPolicy\",\"Statement\":[{\"Sid\":\"test\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""+user.getUserId()+"\"},\"Action\":\"CQS:SendMessage\",\"Resource\":\""+com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl)+"\"}]}";
        attributes.put("Policy", policy);
        setQueueAttributesRequest.setAttributes(attributes);
        sqs.setQueueAttributes(setQueueAttributesRequest);

        Thread.sleep(1000);
        
        result = sqs.getQueueAttributes(getQueueAttributesRequest);
        assertTrue("Expected retention period of 300 sec, instead found " + result.getAttributes().get("MessageRetentionPeriod"), result.getAttributes().get("MessageRetentionPeriod").equals("300"));
        assertTrue("Expected visibility timeout to be 80 sec, instead found " + result.getAttributes().get("VisibilityTimeout"), result.getAttributes().get("VisibilityTimeout").equals("80"));
        assertTrue("Expected max message size to be 10240, instead found " + result.getAttributes().get("MaximumMessageSize"), result.getAttributes().get("MaximumMessageSize").equals("10240"));
        assertTrue("Expected delay seconds to be 100, instead found " + result.getAttributes().get("DelaySeconds"), result.getAttributes().get("DelaySeconds").equals("100"));

        attributes = new HashMap<String, String>(){ {put("VisibilityTimeout", "100");}};
        setQueueAttributesRequest.setAttributes(attributes);
        sqs.setQueueAttributes(setQueueAttributesRequest);
        
        result = sqs.getQueueAttributes(getQueueAttributesRequest);
        assertTrue("Expected visibility timeout to be 100 sec, instead found " + result.getAttributes().get("VisibilityTimeout"), result.getAttributes().get("VisibilityTimeout").equals("100"));

        Thread.sleep(1000);
        
        // try triggering missing parameter error
        
        try {

        	setQueueAttributesRequest = new SetQueueAttributesRequest();
	        setQueueAttributesRequest.setQueueUrl(queueUrl);
	        sqs.setQueueAttributes(setQueueAttributesRequest);

        } catch (AmazonServiceException ase) {
            assertTrue("Did not receive missing parameter exception", ase.getErrorCode().equals(CQSErrorCodes.MissingParameter.getCMBCode()));
        }
        
        // try trigger unknown attribute name error
        
        try {

	        getQueueAttributesRequest = new GetQueueAttributesRequest();
	        getQueueAttributesRequest.setQueueUrl(queueUrl);
	        getQueueAttributesRequest.setAttributeNames(Arrays.asList("all"));
	        sqs.getQueueAttributes(getQueueAttributesRequest);

        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().equals(CQSErrorCodes.InvalidAttributeName.getCMBCode()));
        }

        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(queueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(queueUrl);
    }
    
    @Test
    public void testAddRemovePermission() throws PersistenceException, InterruptedException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(queueUrl);
        
        logger.info("Created queue " + queueUrl + ", now setting permissions");
        
        Thread.sleep(1000);
        
        AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
        addPermissionRequest.setQueueUrl(queueUrl);
        addPermissionRequest.setActions(Arrays.asList("SendMessage"));
        addPermissionRequest.setLabel("testLabel");
        addPermissionRequest.setAWSAccountIds(Arrays.asList(user1.getUserId(), user2.getUserId()));        
        sqs.addPermission(addPermissionRequest);

        addPermissionRequest.setLabel("testLabel2");
        addPermissionRequest.setActions(Arrays.asList("SendMessage", "GetQueueUrl"));
        sqs.addPermission(addPermissionRequest);

        Thread.sleep(1000);
        
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest();
        getQueueAttributesRequest.setAttributeNames(Arrays.asList("All"));
        getQueueAttributesRequest.setQueueUrl(queueUrl);
        GetQueueAttributesResult res = sqs.getQueueAttributes(getQueueAttributesRequest);
        res = sqs.getQueueAttributes(getQueueAttributesRequest);
        
        assertTrue("Did not find labels testLabel and testLabel2", res.toString().contains("testLabel") && res.toString().contains("testLabel2"));
        
        RemovePermissionRequest removePermissionRequest = new RemovePermissionRequest();
        removePermissionRequest.setLabel("testLabel");
        removePermissionRequest.setQueueUrl(queueUrl);
        sqs.removePermission(removePermissionRequest);
        removePermissionRequest.setLabel("testLabel2");
        sqs.removePermission(removePermissionRequest);
        
        Thread.sleep(1000);
                
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(queueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(queueUrl);
    }
 
    @Test
    public void testInvalidRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(queueUrl);
        
        logger.info("Created queue " + queueUrl + ", now trying invalid stuff");

        ICQSMessagePersistence messagePersistence = new CQSMessagePartitionedCassandraPersistence();
        messagePersistence.clearQueue(queueUrl);

        logger.info("Send a message with empty message body");
        
        try {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.setQueueUrl(queueUrl);
            sqs.sendMessage(sendMessageRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get missing parameter exception", ase.getErrorCode().equals(CQSErrorCodes.MissingParameter.getCMBCode()));
        }

        logger.info("Send a message with invalid DelaySeconds");
        
        try {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.setQueueUrl(queueUrl);
            String msg = "This is a message to test invalid delay seconds;";
            sendMessageRequest.setMessageBody(msg);
            sendMessageRequest.setDelaySeconds(1000);
            sqs.sendMessage(sendMessageRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get invalid parameter exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
        }

        logger.info("Send a very long message");
        
        try {
            
        	SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.setQueueUrl(queueUrl);
            String msg = "This is a message to test too longggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg. ";
            
            while (msg.length() < 32 * 1024) {
                msg += msg;
            }
            
            sendMessageRequest.setMessageBody(msg);
            sqs.sendMessage(sendMessageRequest);
        
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get an invalid value exception", ase.getErrorCode().equals(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
        }

        logger.info("Receive messages with invalid max number of messages");
        
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            receiveMessageRequest.setVisibilityTimeout(10);
            receiveMessageRequest.setMaxNumberOfMessages(12);
            sqs.receiveMessage(receiveMessageRequest).getMessages();
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get an invalid value exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
        }

        logger.info("Receive messages with invalid max number of messages");
        
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            receiveMessageRequest.setVisibilityTimeout(10);
            receiveMessageRequest.setMaxNumberOfMessages(0);
            sqs.receiveMessage(receiveMessageRequest).getMessages();
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get an invalid value exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
        }
        
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(queueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(queueUrl);
    }

    @Test
    public void testInvalidBatchDeleteRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(queueUrl);
        
        logger.info("Created queue " + queueUrl + ", now sending message batch");
        
        ICQSMessagePersistence messagePersistence = new CQSMessagePartitionedCassandraPersistence();
        messagePersistence.clearQueue(queueUrl);

        try {
            Thread.sleep(1000);

            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the 1st message in a batch"),
                new SendMessageBatchRequestEntry("id2", "This is the 2nd message in a batch"),
                new SendMessageBatchRequestEntry("id3", "This is the 3rd message in a batch"),
                new SendMessageBatchRequestEntry("id4", "This is the 4th message in a batch"),
                new SendMessageBatchRequestEntry("id5", "This is the 5th message in a batch"),
                new SendMessageBatchRequestEntry("id6", "This is the 6th message in a batch")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            sqs.sendMessageBatch(batchSendRequest);
            
            Thread.sleep(1000);
        } catch (AmazonServiceException ase) {
            fail(ase.toString());
        }
        
        logger.info("Receiving messages from " + queueUrl);
        
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

        List<Message> messages = new ArrayList<Message>();
        receiveMessageRequest.setVisibilityTimeout(60);
        receiveMessageRequest.setMaxNumberOfMessages(6);
        messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        
        assertTrue("Did not receive any messages", messages.size() > 0);
        
        List<DeleteMessageBatchRequestEntry> deleteMsgList = new ArrayList<DeleteMessageBatchRequestEntry>();

        int i = 0;
        
        for (Message message : messages) {
        	
            if (i == messages.size() - 1) {
                i--;
            }
            
            logger.info("MessageId:     " + message.getMessageId());
            logger.info("ReceiptHandle: " + message.getReceiptHandle());
            deleteMsgList.add(new DeleteMessageBatchRequestEntry("msg" + i, message.getReceiptHandle()));
            i++;
        }

        logger.info("Delete a batch of messages with same supplied id");
        
        try {
            DeleteMessageBatchRequest batchDeleteRequest = new DeleteMessageBatchRequest(queueUrl, deleteMsgList);
            sqs.deleteMessageBatch(batchDeleteRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Did not ids not distinct exception", ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
        }

        logger.info("Delete a batch of messages with empty receipt handle");
        
        try {
            deleteMsgList.get(0).setId("somerandomid");
            deleteMsgList.get(deleteMsgList.size() - 1).setId("some-random-id");
            deleteMsgList.get(0).setReceiptHandle("somerandomestring");
            DeleteMessageBatchRequest batchDeleteRequest = new DeleteMessageBatchRequest(queueUrl, deleteMsgList);
            sqs.deleteMessageBatch(batchDeleteRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get internal error exception", ase.getErrorCode().equals(CQSErrorCodes.InternalError.getCMBCode()));
        }
        
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(queueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(queueUrl);
    }

    @Test
    public void testInvalidBatchChangeMessageVisibilityRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(queueUrl);
        
        ICQSMessagePersistence messagePersistence = new CQSMessagePartitionedCassandraPersistence();
        messagePersistence.clearQueue(queueUrl);

        logger.info("Created queue " + queueUrl + ", now sending message batch");
        
        try {
        	
        	Thread.sleep(1000);
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the 1st message in a batch"),
                new SendMessageBatchRequestEntry("id2", "This is the 2nd message in a batch"),
                new SendMessageBatchRequestEntry("id3", "This is the 3rd message in a batch"),
                new SendMessageBatchRequestEntry("id4", "This is the 4th message in a batch"),
                new SendMessageBatchRequestEntry("id5", "This is the 5th message in a batch"),
                new SendMessageBatchRequestEntry("id6", "This is the 6th message in a batch")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            sqs.sendMessageBatch(batchSendRequest);
            Thread.sleep(1000);
            
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        }

        logger.info("Receiving messages from " + queueUrl);
        
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

        List<Message> messages = new ArrayList<Message>();
        receiveMessageRequest.setVisibilityTimeout(60);
        receiveMessageRequest.setMaxNumberOfMessages(6);
        messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        
        List<ChangeMessageVisibilityBatchRequestEntry> msgList = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>();

        int i = 0;
        
        for (Message message : messages) {
        	
            if (i == messages.size() - 1) {
                i--;
            }
            
            logger.info("MessageId:     " + message.getMessageId());
            logger.info("ReceiptHandle: " + message.getReceiptHandle());
            ChangeMessageVisibilityBatchRequestEntry entry = new ChangeMessageVisibilityBatchRequestEntry("msg" + i, message.getReceiptHandle());
            entry.setVisibilityTimeout(60);
            msgList.add(entry);
            i++;
        }

        logger.info("Change a batch of message visibility timeout with same supplied id");
        
        try {
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(queueUrl, msgList);
            sqs.changeMessageVisibilityBatch(batchRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get distinct id excpetion", ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
        }

        logger.info("Change a batch of messages with empty or invalid supplied id");
        
        try {
            msgList.get(0).setId("bad.id");
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(queueUrl, msgList);
            sqs.changeMessageVisibilityBatch(batchRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get invalid batch entry id exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getCMBCode()));
        }

        logger.info("Change a batch of messages with empty ReceiptHandle:");
        
        try {
            msgList.get(0).setId("somerandomid");
            msgList.get(msgList.size() - 1).setId("some-random-id");
            msgList.get(0).setReceiptHandle("somerandomestring");
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(queueUrl, msgList);
            sqs.changeMessageVisibilityBatch(batchRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get internal error exception", ase.getErrorCode().contains(CQSErrorCodes.InternalError.getCMBCode()));
        }
        
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(queueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(queueUrl);
    }

    @Test
    public void testInvalidBatchSendRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(myQueueUrl);
        
        ICQSMessagePersistence messagePersistence = new CQSMessagePartitionedCassandraPersistence();
        messagePersistence.clearQueue(myQueueUrl);

        logger.info("Send a batch of messages with empty supplied Id");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("id.1", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("id3", "This is the third message in a batch")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            sqs.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get invalid batch entry id exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getCMBCode()));
        }

        logger.info("Send a batch of messages with empty message");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("id2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("id3", "")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            sqs.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            fail(ase.toString());
        }

        logger.info("Send a batch of messages with same supplied id");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("1", "Test")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            sqs.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get batch entry id not distinct exception", ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
        }

        logger.info("Send a batch of messages with supplied id too long");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", "Test")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            sqs.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get invalid batch entry id", ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getCMBCode()));
        }

        logger.info("Send a batch of messages total length over 64KB");
        
        try {
            char[] chars = new char[64*1024 - 10];
            java.util.Arrays.fill(chars, 'x');
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("3", new String(chars))
            );
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            sqs.sendMessageBatch(batchSendRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get batch request too long exception", ase.getErrorCode().contains(CQSErrorCodes.BatchRequestTooLong.getCMBCode()));
        }
        
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(myQueueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(myQueueUrl);
    }

    private void displayServiceException(AmazonServiceException ase) {
    	
        logger.error("Caught an AmazonServiceException, which means your request made it to Amazon SQS, but was rejected with an error response for some reason.");
        logger.error("Error Message:    " + ase.getMessage());
        logger.error("HTTP Status Code: " + ase.getStatusCode());
        logger.error("AWS Error Code:   " + ase.getErrorCode());
        logger.error("Error Type:       " + ase.getErrorType());
        logger.error("Request ID:       " + ase.getRequestId());

        fail("AWSServiceException:"+ase);
    }

    @Test
    public void testInvalidPermissionUpdate() {
    	
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(queueUrl);

        sqs.addPermission(new AddPermissionRequest(queueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage", "DeleteMessage")));

        try {
        	sqs.addPermission(new AddPermissionRequest(queueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage")));
        } catch (Exception ex) {
        	assertTrue("Did not get label already exists exception", ex.getMessage().contains("Already exists"));
        }
        
        sqs.removePermission(new RemovePermissionRequest(queueUrl, "label1"));
        
    	sqs.addPermission(new AddPermissionRequest(queueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage")));

    	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(queueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(queueUrl);
    }

    @Test
    public void testCreateDeleteQueue() throws InterruptedException {

        String queueUrl1 = "";
        String queueUrl2 = "";

        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);

            createQueueRequest.setAttributes((new HashMap<String, String>() {
                {
                    put("MessageRetentionPeriod", "600");
                }
            }
            ));
            
            queueUrl1 = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(queueUrl1);

            sqs.receiveMessage(new ReceiveMessageRequest(queueUrl1));

            String qName2 = QUEUE_PREFIX + randomGenerator.nextLong();
            createQueueRequest = new CreateQueueRequest(qName2);
            queueUrl2 = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(queueUrl2);

            sqs.receiveMessage(new ReceiveMessageRequest(queueUrl2));

            Thread.sleep(1000);

            // List queues
            
            logger.info("Listing all queues");
            
            List<String> queueUrls = sqs.listQueues().getQueueUrls();

            for (String queueUrl : queueUrls) {
                logger.info(queueUrl);
            }
            
            assertTrue("Expected 2 queue urls but got " + queueUrls.size(), queueUrls.size() == 2);
            assertTrue("Missing queue url " + queueUrl1, queueUrls.contains(queueUrl1));
            assertTrue("Missing queue url " + queueUrl2, queueUrls.contains(queueUrl2));

            // Send a message
            
            logger.info("Sending messages to " + queueUrl1);
            
            sqs.sendMessage(new SendMessageRequest(queueUrl1, "This is my message text 1. " + (new Random()).nextInt()));
            sqs.sendMessage(new SendMessageRequest(queueUrl1, "This is my message text 2. " + (new Random()).nextInt()));
            sqs.sendMessage(new SendMessageRequest(queueUrl1, "This is my message text 3. " + (new Random()).nextInt()));
     
            Thread.sleep(1000);

            logger.info("Receiving messages from " + queueUrl1);
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl1);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(600);

            receiveMessageRequest.setMaxNumberOfMessages(2);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            assertTrue("Expected 2 messages, instead found " + messages.size(), messages.size() == 2);

            for (Message message : messages) {
            	
                logger.info("  Message");
                logger.info("    MessageId:     " + message.getMessageId());
                logger.info("    ReceiptHandle: " + message.getReceiptHandle());
                logger.info("    MD5OfBody:     " + message.getMD5OfBody());
                logger.info("    Body:          " + message.getBody());
                
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    logger.info("  Attribute");
                    logger.info("    Name:  " + entry.getKey());
                    logger.info("    Value: " + entry.getValue());
                }
            }

            List<Message> messages2 = sqs.receiveMessage(receiveMessageRequest).getMessages();

            for (Message message : messages2) {
            	
                logger.info("  Message");
                logger.info("    MessageId:     " + message.getMessageId());
                logger.info("    ReceiptHandle: " + message.getReceiptHandle());
                logger.info("    MD5OfBody:     " + message.getMD5OfBody());
                logger.info("    Body:          " + message.getBody());
                
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    logger.info("  Attribute");
                    logger.info("    Name:  " + entry.getKey());
                    logger.info("    Value: " + entry.getValue());
                }
            }
            
            assertTrue("Expected one message, instead found " + messages2.size(), messages2.size() == 1);

            if (messages.size() > 0) {
            	
                logger.info("Deleting a message");
                
                String messageRecieptHandle = messages.get(0).getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(queueUrl1, messageRecieptHandle));
            }

            DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(queueUrl1);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(queueUrl1);
            
            deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(queueUrl2);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(queueUrl2);

        } catch (AmazonServiceException ase) {
        	sqs.deleteQueue(new DeleteQueueRequest(queueUrl1));
        	fail(ase.toString());
        }
    }
    
    @Test
    public void testCreateDuplicateQueue() {

        String queueUrl = "";

        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);

            createQueueRequest.setAttributes((new HashMap<String, String>() {
                {
                    put("MessageRetentionPeriod", "60");
                }
            }
            ));

            queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(queueUrl);
            
            // Create a duplicate name queue
            
            String qUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            
            assert(qUrl.equals(queueUrl));
            
            // Create a duplicate name queue with different attribute

            createQueueRequest.setAttributes((new HashMap<String, String>() {
                {
                    put("MessageRetentionPeriod", "30");
                }
            }
            ));
            
            qUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

        } catch (AmazonServiceException ase) {
        	
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(queueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(queueUrl);

            assertTrue("Did not get queue name exists exception", ase.getErrorCode().contains(CQSErrorCodes.QueueNameExists.getCMBCode()));
        }
    }

    @Test
    public void testChangeMessageVisibility() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
    	
        try {
        	
            String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(queueUrl);
            
            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
            messagePersistence.clearQueue(queueUrl);
            
            // send message
            
            Thread.sleep(1000);
            logger.info("Sending a message to " + queueUrl);
            
            sqs.sendMessage(new SendMessageRequest(queueUrl, "This is my message text. " + (new Random()).nextInt()));
            
            Thread.sleep(1000);

            // receive message
            
            logger.info("Receiving messages from " + queueName);
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(2);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 1 message, instead found " + messages.size(), messages.size() == 1);

            String receiptHandle = null;

            for (Message message : messages) {
            
            	logger.info("  Message");
                logger.info("    MessageId:     " + message.getMessageId());
                logger.info("    ReceiptHandle: " + message.getReceiptHandle());
                receiptHandle = message.getReceiptHandle();
                logger.info("    MD5OfBody:     " + message.getMD5OfBody());
                logger.info("    Body:          " + message.getBody());
                
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    logger.info("  Attribute");
                    logger.info("    Name:  " + entry.getKey());
                    logger.info("    Value: " + entry.getValue());
                }
            }
            
            // message should be invisible now
            
            logger.info("Trying to receive message, none expected");
  
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 0 messages, instead found "+ messages.size(), messages.size() == 0);
            
            int timeoutSeconds = 10;
            int waitSeconds = 2;
            int waitedAlreadySeconds = 0;

            logger.info("Changing visibility timeout to " + timeoutSeconds);
            
            ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, timeoutSeconds);
            sqs.changeMessageVisibility(changeMessageVisibilityRequest);
            
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            
            // keep checking

            while (messages.size() == 0) {

            	if (waitedAlreadySeconds > timeoutSeconds + 2*waitSeconds) {
            		fail("Message did not become revisible after " + waitedAlreadySeconds + " seconds");
            	}
            	
            	Thread.sleep(waitSeconds*1000);
            	waitedAlreadySeconds += waitSeconds;
            	logger.info("Checking for messages for " + waitedAlreadySeconds + " seconds");
                messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            }
            

            assertTrue("Received " + messages.size() + " instead of 1 message", messages.size() == 1);
            assertTrue("Message content dorky: " + messages.get(0).getBody(), messages.get(0).getBody().startsWith("This is my message text."));
            
            // delete message
            
            logger.info("Deleting message with receipt handle " + messages.get(0).getReceiptHandle());
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messages.get(0).getReceiptHandle()));

            // delete queue
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(queueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(queueUrl);
            
        } catch (AmazonServiceException ase) {
            fail(ase.toString());
        }
    }
    
    @Test
    public void testSendDeleteMessage() throws InterruptedException, PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
    	
        try {
        	
            String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            createQueueRequest.setAttributes(attributeParams);
            String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(queueUrl);
            
            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
            messagePersistence.clearQueue(queueUrl);

            // send a batch of messages
            
            Thread.sleep(1000);

            logger.info("Sending batch messages to " + queueUrl);
            SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, "This is a test message");
            SendMessageResult sendResult = sqs.sendMessage(sendMessageRequest);
            
            assertNotNull("Message id is null", sendResult.getMessageId());

            Thread.sleep(1000);

            // receive messages
            
            logger.info("Receiving messages from " + queueUrl);

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(1);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            
            assertTrue("Expected one message, instead got " + messages.size(), messages.size() == 1);

            for (Message message : messages) {
            	
                logger.info("  Message");
                logger.info("    MessageId:     " + message.getMessageId());
                logger.info("    ReceiptHandle: " + message.getReceiptHandle());
                logger.info("    MD5OfBody:     " + message.getMD5OfBody());
                logger.info("    Body:          " + message.getBody());
                
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    logger.info("  Attribute");
                    logger.info("    Name:  " + entry.getKey());
                    logger.info("    Value: " + entry.getValue());
                }
            }
            
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, messages.get(0).getReceiptHandle());
            sqs.deleteMessage(deleteMessageRequest);
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(queueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(queueUrl);

        } catch (AmazonServiceException ase) {
            fail(ase.toString());
        }
    }
    
    @Test
    public void testSendDeleteMessageBatch() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
    	
        try {
        	
            String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            createQueueRequest.setAttributes(attributeParams);
            String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(queueUrl);
            
            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
            messagePersistence.clearQueue(queueUrl);
            
            Thread.sleep(1000);

            // send batch of messages

            logger.info("Sending batch messages to " + queueUrl);
            
            SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(queueUrl);
            List<SendMessageBatchRequestEntry> sendEntryList = new ArrayList<SendMessageBatchRequestEntry>();

            for (int i = 0; i < 5; i++) {
                sendEntryList.add(new SendMessageBatchRequestEntry("msg_" + i, "This is a test message: batch " + i));
            }
            
            sendMessageBatchRequest.setEntries(sendEntryList);
            sqs.sendMessageBatch(sendMessageBatchRequest);
            
            Thread.sleep(1000);

            // receive messages
            
            logger.info("Receiving messages from " + queueUrl);
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 5 messages, instead found " + messages.size(), messages.size() == 5);

            List<DeleteMessageBatchRequestEntry> deleteEntryList = new ArrayList<DeleteMessageBatchRequestEntry>();

            int i = 0;
            
            for (Message message : messages) {
            	
                logger.info("  Message");
                logger.info("    MessageId:     " + message.getMessageId());
                logger.info("    ReceiptHandle: " + message.getReceiptHandle());
                logger.info("    MD5OfBody:     " + message.getMD5OfBody());
                logger.info("    Body:          " + message.getBody());
                
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    logger.info("  Attribute");
                    logger.info("    Name:  " + entry.getKey());
                    logger.info("    Value: " + entry.getValue());
                }
                
                i++;
                
                deleteEntryList.add(new DeleteMessageBatchRequestEntry(i + "", message.getReceiptHandle()));
            }
            
            DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(queueUrl);
            deleteMessageBatchRequest.setEntries(deleteEntryList);
            sqs.deleteMessageBatch(deleteMessageBatchRequest);
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(queueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(queueUrl);
            
        } catch (AmazonServiceException ase) {
            fail(ase.toString());
        }       
    }

    @Test
    public void testChangeMessageVisibilityBatch() throws InterruptedException, PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
    	
        try {
        	
            String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            createQueueRequest.setAttributes(attributeParams);
            String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(queueUrl);
            
            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
            messagePersistence.clearQueue(queueUrl);
            
            Thread.sleep(1000);

            // send batch of messages

            logger.info("Sending batch messages to " + queueUrl);
            
            SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(queueUrl);
            List<SendMessageBatchRequestEntry> sendEntryList = new ArrayList<SendMessageBatchRequestEntry>();

            for (int i = 0; i < 5; i++) {
                sendEntryList.add(new SendMessageBatchRequestEntry("msg_" + i, "This is a test message: batch " + i));
            }
            
            sendMessageBatchRequest.setEntries(sendEntryList);
            sqs.sendMessageBatch(sendMessageBatchRequest);
            
            Thread.sleep(1000);

            // receive messages
            
            logger.info("Receiving messages from " + queueUrl);
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 5 messages, received " + messages.size(), messages.size() == 5);

            // change message visibility batch to 10 sec for all messages

            int i = 0;
            
            ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
            changeMessageVisibilityBatchRequest.setQueueUrl(queueUrl);
            
            List<ChangeMessageVisibilityBatchRequestEntry> visibilityEntryList = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>();
            
            for (Message message : messages) {
            	
                logger.info("  Message");
                logger.info("    MessageId:     " + message.getMessageId());
                logger.info("    ReceiptHandle: " + message.getReceiptHandle());
                logger.info("    MD5OfBody:     " + message.getMD5OfBody());
                logger.info("    Body:          " + message.getBody());
                
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    logger.info("  Attribute");
                    logger.info("    Name:  " + entry.getKey());
                    logger.info("    Value: " + entry.getValue());
                }
                
                i++;
                
                ChangeMessageVisibilityBatchRequestEntry entry = new ChangeMessageVisibilityBatchRequestEntry(i + "", message.getReceiptHandle());
                entry.setVisibilityTimeout(10);
                visibilityEntryList.add(entry);
            }
            
            changeMessageVisibilityBatchRequest.setEntries(visibilityEntryList);
            sqs.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
            
            // check if messages invisible
            
            messages = new ArrayList<Message>();
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 0 messages, received " + messages.size(), messages.size() == 0);
            
            Thread.sleep(11000);

            // check if messages revisible
            
            messages = new ArrayList<Message>();
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 5 messages, received " + messages.size(), messages.size() == 5);
            
            // delete messages
            
            List<DeleteMessageBatchRequestEntry> deleteEntryList = new ArrayList<DeleteMessageBatchRequestEntry>();
            
            for (Message message : messages) {
            	
                logger.info("  Message");
                logger.info("    MessageId:     " + message.getMessageId());
                logger.info("    ReceiptHandle: " + message.getReceiptHandle());
                logger.info("    MD5OfBody:     " + message.getMD5OfBody());
                logger.info("    Body:          " + message.getBody());
                
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    logger.info("  Attribute");
                    logger.info("    Name:  " + entry.getKey());
                    logger.info("    Value: " + entry.getValue());
                }
                
                i++;
                
                deleteEntryList.add(new DeleteMessageBatchRequestEntry(i+"", message.getReceiptHandle()));
            }
            
            DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(queueUrl);
            deleteMessageBatchRequest.setEntries(deleteEntryList);
            sqs.deleteMessageBatch(deleteMessageBatchRequest);
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(queueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            Thread.sleep(1000);
            
            // check if messages deleted
            
            messages = new ArrayList<Message>();
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 0 messages, received " + messages.size(), messages.size() == 0);
            
            randomQueueUrls.remove(queueUrl);
            
        } catch (AmazonServiceException ase) {
            fail(ase.toString());
        }       
    }

    @After    
    public void tearDown() {
    	
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
        
        for (String queueUrl : randomQueueUrls) {
        	sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
        }
    }
}
