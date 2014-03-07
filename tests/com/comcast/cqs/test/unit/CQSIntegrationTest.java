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

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;

import org.json.JSONObject;
import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
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
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;
import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.cqs.util.CQSErrorCodes;

public class CQSIntegrationTest extends CMBAWSBaseTest {

    private static HashMap<String, String> attributeParams = new HashMap<String, String>();
    
    static {
    	attributeParams.put("MessageRetentionPeriod", "600");
    	attributeParams.put("VisibilityTimeout", "30");
    }
    
    @Test
    public void testSubscribeConfirmPublish() {
    	
    	try {
    	
	        String queueUrl = getQueueUrl(1, USR.USER1);
	        
	        AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setQueueUrl(queueUrl);
	        addPermissionRequest.setActions(Arrays.asList("SendMessage"));
	        addPermissionRequest.setLabel(UUID.randomUUID().toString());
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
	        cqs1.addPermission(addPermissionRequest);
	        
			String topicArn = getTopic(1, USR.USER2);
			
			SubscribeRequest subscribeRequest = new SubscribeRequest();
			String queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			subscribeRequest.setEndpoint(queueArn);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			SubscribeResult subscribeResult = cns2.subscribe(subscribeRequest);
			String subscriptionArn = subscribeResult.getSubscriptionArn();
			
			if (subscriptionArn.equals("pending confirmation")) {
				
				Thread.sleep(500);
				
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
				
				List<Message> messages = receiveMessageResult.getMessages();
				
				if (messages != null && messages.size() == 1) {
					
	    			JSONObject o = new JSONObject(messages.get(0).getBody());
	    			
	    			if (!o.has("SubscribeURL")) {
	    				fail("Message is not a confirmation messsage");
	    			}
	    			
	    			String subscriptionUrl = o.getString("SubscribeURL");
					CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
    		        
    				DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
    				deleteMessageRequest.setQueueUrl(queueUrl);
    				deleteMessageRequest.setReceiptHandle(messages.get(0).getReceiptHandle());
    				
    				cqs1.deleteMessage(deleteMessageRequest);
				
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
			cns2.publish(publishRequest);
			
			Thread.sleep(2000);

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(1);
			ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
			
			List<Message> messages = receiveMessageResult.getMessages();
			
			if (messages != null && messages.size() == 1) {
				String messageBody = messages.get(0).getBody();
				assertTrue(messageBody.contains(messageText));
			} else {
				fail("No messages found");
			}
			
    	} catch (Exception ex) {
    		logger.error("test failed", ex);
    		fail(ex.getMessage());
    	}
    }
    
    @Test
    public void testGetSetQueueAttributes() throws PersistenceException, InterruptedException {
    	
        String queueUrl = getQueueUrl(1, USR.USER1);
        cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest();
        getQueueAttributesRequest.setQueueUrl(queueUrl);
        getQueueAttributesRequest.setAttributeNames(Arrays.asList("VisibilityTimeout", "MessageRetentionPeriod", "All"));
        GetQueueAttributesResult result = cqs1.getQueueAttributes(getQueueAttributesRequest);
        assertTrue(result.getAttributes().get("MessageRetentionPeriod").equals("600"));
        assertTrue(result.getAttributes().get("VisibilityTimeout").equals("30"));
        
        SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest();
        setQueueAttributesRequest.setQueueUrl(queueUrl);
        HashMap<String, String> attributes = new HashMap<String, String>();
        attributes.put("MessageRetentionPeriod", "300");
        attributes.put("VisibilityTimeout", "80");
        attributes.put("MaximumMessageSize", "10240");
        attributes.put("DelaySeconds", "100");
        String policy = "{\"Version\":\"2008-10-17\",\"Id\":\""+queueUrl+"/SQSDefaultPolicy\",\"Statement\":[{\"Sid\":\"test\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""+user1.getUserId()+"\"},\"Action\":\"CQS:SendMessage\",\"Resource\":\""+com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl)+"\"}]}";
        attributes.put("Policy", policy);
        setQueueAttributesRequest.setAttributes(attributes);
        cqs1.setQueueAttributes(setQueueAttributesRequest);

        result = cqs1.getQueueAttributes(getQueueAttributesRequest);
        assertTrue("Expected retention period of 300 sec, instead found " + result.getAttributes().get("MessageRetentionPeriod"), result.getAttributes().get("MessageRetentionPeriod").equals("300"));
        assertTrue("Expected visibility timeout to be 80 sec, instead found " + result.getAttributes().get("VisibilityTimeout"), result.getAttributes().get("VisibilityTimeout").equals("80"));
        assertTrue("Expected max message size to be 10240, instead found " + result.getAttributes().get("MaximumMessageSize"), result.getAttributes().get("MaximumMessageSize").equals("10240"));
        assertTrue("Expected delay seconds to be 100, instead found " + result.getAttributes().get("DelaySeconds"), result.getAttributes().get("DelaySeconds").equals("100"));

        attributes = new HashMap<String, String>(){ {put("VisibilityTimeout", "100");}};
        setQueueAttributesRequest.setAttributes(attributes);
        cqs1.setQueueAttributes(setQueueAttributesRequest);
        
        result = cqs1.getQueueAttributes(getQueueAttributesRequest);
        assertTrue("Expected visibility timeout to be 100 sec, instead found " + result.getAttributes().get("VisibilityTimeout"), result.getAttributes().get("VisibilityTimeout").equals("100"));

        // try triggering missing parameter error
        
        try {

        	setQueueAttributesRequest = new SetQueueAttributesRequest();
	        setQueueAttributesRequest.setQueueUrl(queueUrl);
	        cqs1.setQueueAttributes(setQueueAttributesRequest);
	        fail("missing expected exception");
	        
        } catch (AmazonServiceException ase) {
            assertTrue("Did not receive missing parameter exception", ase.getErrorCode().equals(CQSErrorCodes.MissingParameter.getCMBCode()));
        }
        
        // try trigger unknown attribute name error
        
        try {

	        getQueueAttributesRequest = new GetQueueAttributesRequest();
	        getQueueAttributesRequest.setQueueUrl(queueUrl);
	        getQueueAttributesRequest.setAttributeNames(Arrays.asList("all"));
	        cqs1.getQueueAttributes(getQueueAttributesRequest);
	        fail("missing expected exception");

        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().equals(CQSErrorCodes.InvalidAttributeName.getCMBCode()));
        }
    }
    
    @Test
    public void testAddRemovePermission() throws PersistenceException, InterruptedException {
        
        String queueUrl = getQueueUrl(1, USR.USER1);
        cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));
        
        AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
        addPermissionRequest.setQueueUrl(queueUrl);
        addPermissionRequest.setActions(Arrays.asList("SendMessage"));
        addPermissionRequest.setLabel("testLabel");
        addPermissionRequest.setAWSAccountIds(Arrays.asList(user1.getUserId(), user2.getUserId()));        
        cqs1.addPermission(addPermissionRequest);

        addPermissionRequest.setLabel("testLabel2");
        addPermissionRequest.setActions(Arrays.asList("SendMessage", "GetQueueUrl"));
        cqs1.addPermission(addPermissionRequest);

        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest();
        getQueueAttributesRequest.setAttributeNames(Arrays.asList("All"));
        getQueueAttributesRequest.setQueueUrl(queueUrl);
        GetQueueAttributesResult res = cqs1.getQueueAttributes(getQueueAttributesRequest);
        res = cqs1.getQueueAttributes(getQueueAttributesRequest);
        
        assertTrue("Did not find labels testLabel and testLabel2", res.toString().contains("testLabel") && res.toString().contains("testLabel2"));
        
        RemovePermissionRequest removePermissionRequest = new RemovePermissionRequest();
        removePermissionRequest.setLabel("testLabel");
        removePermissionRequest.setQueueUrl(queueUrl);
        cqs1.removePermission(removePermissionRequest);
        removePermissionRequest.setLabel("testLabel2");
        cqs1.removePermission(removePermissionRequest);
    }
 
    @Test
    public void testInvalidRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
        String queueUrl = getQueueUrl(1, USR.USER1);
        cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

        logger.info("Send a message with empty message body");
        
        try {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.setQueueUrl(queueUrl);
            cqs1.sendMessage(sendMessageRequest);
	        fail("missing expected exception");
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
            cqs1.sendMessage(sendMessageRequest);
	        fail("missing expected exception");
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get invalid parameter exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
        }

        logger.info("Send a very long message");
        
        try {
            
        	SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.setQueueUrl(queueUrl);
            StringBuffer msg = new StringBuffer("");
            
            for  (int i=0; i<300*1024; i++) {
                msg.append("M");
            }
            
            sendMessageRequest.setMessageBody(msg.toString());
            cqs1.sendMessage(sendMessageRequest);
	        fail("missing expected exception");
        
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get an invalid value exception", ase.getErrorCode().equals(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
        }

        logger.info("Receive messages with invalid max number of messages");
        
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            receiveMessageRequest.setVisibilityTimeout(10);
            receiveMessageRequest.setMaxNumberOfMessages(12);
            cqs1.receiveMessage(receiveMessageRequest).getMessages();
	        fail("missing expected exception");
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get an invalid value exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
        }

        logger.info("Receive messages with invalid max number of messages");
        
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            receiveMessageRequest.setVisibilityTimeout(10);
            receiveMessageRequest.setMaxNumberOfMessages(0);
            cqs1.receiveMessage(receiveMessageRequest).getMessages();
	        fail("missing expected exception");
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get an invalid value exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
        }
    }

    @Test
    public void testInvalidBatchDeleteRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
        
        String queueUrl = getQueueUrl(1, USR.USER1);
        cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

        try {

        	List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the 1st message in a batch"),
                new SendMessageBatchRequestEntry("id2", "This is the 2nd message in a batch"),
                new SendMessageBatchRequestEntry("id3", "This is the 3rd message in a batch"),
                new SendMessageBatchRequestEntry("id4", "This is the 4th message in a batch"),
                new SendMessageBatchRequestEntry("id5", "This is the 5th message in a batch"),
                new SendMessageBatchRequestEntry("id6", "This is the 6th message in a batch")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            cqs1.sendMessageBatch(batchSendRequest);

        } catch (AmazonServiceException ase) {
            fail(ase.toString());
        }
        
        logger.info("Receiving messages from " + queueUrl);
        
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

        List<Message> messages = new ArrayList<Message>();
        receiveMessageRequest.setVisibilityTimeout(60);
        receiveMessageRequest.setMaxNumberOfMessages(6);
        messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
        
        assertTrue("Did not receive any messages", messages.size() > 0);
        
        List<DeleteMessageBatchRequestEntry> deleteMsgList = new ArrayList<DeleteMessageBatchRequestEntry>();

        int i = 0;
        
        for (Message message : messages) {
            logger.info("MessageId:     " + message.getMessageId());
            logger.info("ReceiptHandle: " + message.getReceiptHandle());
            deleteMsgList.add(new DeleteMessageBatchRequestEntry("msg" + i, message.getReceiptHandle()));
            i++;
        }

        try {
            DeleteMessageBatchRequest batchDeleteRequest = new DeleteMessageBatchRequest(queueUrl, deleteMsgList);
            cqs1.deleteMessageBatch(batchDeleteRequest);
        } catch (AmazonServiceException ase) {
	        fail("exception where none expected");
        }

        logger.info("Delete a batch of messages with empty receipt handle");
        
        try {
            deleteMsgList.get(0).setId("somerandomid");
            deleteMsgList.get(deleteMsgList.size() - 1).setId("some-random-id");
            deleteMsgList.get(0).setReceiptHandle("somerandomestring");
            DeleteMessageBatchRequest batchDeleteRequest = new DeleteMessageBatchRequest(queueUrl, deleteMsgList);
            cqs1.deleteMessageBatch(batchDeleteRequest);
	        fail("missing expected exception");
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get internal error exception", ase.getErrorCode().equals(CQSErrorCodes.InternalError.getCMBCode()));
        }
    }

    @Test
    public void testInvalidBatchChangeMessageVisibilityRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
        
        String queueUrl = getQueueUrl(1, USR.USER1);
        cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));
        
        Thread.sleep(1000);
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the 1st message in a batch"),
                new SendMessageBatchRequestEntry("id2", "This is the 2nd message in a batch"),
                new SendMessageBatchRequestEntry("id3", "This is the 3rd message in a batch"),
                new SendMessageBatchRequestEntry("id4", "This is the 4th message in a batch"),
                new SendMessageBatchRequestEntry("id5", "This is the 5th message in a batch"),
                new SendMessageBatchRequestEntry("id6", "This is the 6th message in a batch")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            cqs1.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
        	logger.error("test failed", ase);
        	fail("exception where none expected");
        }

        logger.info("Receiving messages from " + queueUrl);
        
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

        List<Message> messages = new ArrayList<Message>();
        receiveMessageRequest.setVisibilityTimeout(60);
        receiveMessageRequest.setMaxNumberOfMessages(6);
        messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
        
        List<ChangeMessageVisibilityBatchRequestEntry> msgList = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>();

        for (Message message : messages) {
            logger.info("MessageId:     " + message.getMessageId());
            logger.info("ReceiptHandle: " + message.getReceiptHandle());
            ChangeMessageVisibilityBatchRequestEntry entry = new ChangeMessageVisibilityBatchRequestEntry("1", message.getReceiptHandle());
            entry.setVisibilityTimeout(60);
            msgList.add(entry);
        }

        logger.info("Change a batch of message visibility timeout with same supplied id");
        
        try {
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(queueUrl, msgList);
            cqs1.changeMessageVisibilityBatch(batchRequest);
            fail("missing expected exception");
        } catch (AmazonServiceException ase) {
            //assertTrue("Did not get distinct id excpetion", ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
        }

        logger.info("Change a batch of messages with empty or invalid supplied id");
        
        try {
            msgList.get(0).setId("bad.id");
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(queueUrl, msgList);
            cqs1.changeMessageVisibilityBatch(batchRequest);
            fail("missing expected exception");
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get invalid batch entry id exception", ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getCMBCode()));
        }

        logger.info("Change a batch of messages with empty ReceiptHandle:");
        
        try {
            msgList.get(0).setId("somerandomid");
            msgList.get(msgList.size() - 1).setId("some-random-id");
            msgList.get(0).setReceiptHandle("somerandomestring");
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(queueUrl, msgList);
            cqs1.changeMessageVisibilityBatch(batchRequest);
            fail("missing expected exception");
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get internal error exception", ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
        }
    }

    @Test
    public void testInvalidBatchSendRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
    	String queueUrl = getQueueUrl(1, USR.USER1);
		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

        logger.info("Send a batch of messages with empty supplied Id");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("id.1", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("id3", "This is the third message in a batch")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            cqs1.sendMessageBatch(batchSendRequest);
            fail("missing expected exception");
            
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
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            cqs1.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            fail(ase.getMessage());
        }

        logger.info("Send a batch of messages with same supplied id");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("1", "Test")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            cqs1.sendMessageBatch(batchSendRequest);
            fail("missing expected exception");
            
        } catch (AmazonServiceException ase) {
            //assertTrue("Did not get batch entry id not distinct exception", ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
        }

        logger.info("Send a batch of messages with supplied id too long");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", "Test")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            cqs1.sendMessageBatch(batchSendRequest);
            fail("missing expected exception");
            
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get invalid batch entry id", ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getCMBCode()));
        }

        logger.info("Send a batch of messages total length over 64KB");
        
        try {
            char[] chars = new char[300*1024 - 10];
            java.util.Arrays.fill(chars, 'x');
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("3", new String(chars))
            );
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(queueUrl, messageList);
            cqs1.sendMessageBatch(batchSendRequest);
            fail("missing expected exception");
        } catch (AmazonServiceException ase) {
            assertTrue("Did not get batch request too long exception", ase.getErrorCode().contains(CQSErrorCodes.BatchRequestTooLong.getCMBCode()));
        }
    }

    @Test
    public void testInvalidPermissionUpdate() {
    	
    	String queueUrl = getQueueUrl(1, USR.USER1);
		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

        cqs1.addPermission(new AddPermissionRequest(queueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage", "DeleteMessage")));

        try {
        	cqs1.addPermission(new AddPermissionRequest(queueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage")));
        	fail("missing expected exception");
        } catch (Exception ex) {
        	assertTrue("Did not get label already exists exception", ex.getMessage().contains("Already exists"));
        }
        
        cqs1.removePermission(new RemovePermissionRequest(queueUrl, "label1"));
        
    	cqs1.addPermission(new AddPermissionRequest(queueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage")));
    }

    @Test
    public void testCreateDeleteQueue() throws InterruptedException {

         try {
        	
        	String queueUrl1 = getQueueUrl(1, USR.USER1);
			cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl1, attributeParams));
             
        	String queueUrl2 = getQueueUrl(2, USR.USER1);
			cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl1, attributeParams));

	        Thread.sleep(1000);
	
	        // List queues
	        
	        logger.info("Listing all queues");
	        
	        List<String> queueUrls = cqs1.listQueues().getQueueUrls();
	
	        for (String queueUrl : queueUrls) {
	            logger.info(queueUrl);
	        }
	        
	        assertTrue("Expected 2 queue urls but got " + queueUrls.size(), queueUrls.size() == 2);
	        assertTrue("Missing queue url " + queueUrl1, queueUrls.contains(queueUrl1));
	        assertTrue("Missing queue url " + queueUrl2, queueUrls.contains(queueUrl2));
	
	        // Send a message
	        
	        logger.info("Sending messages to " + queueUrl1);
	        
	        cqs1.sendMessage(new SendMessageRequest(queueUrl1, "This is my message text 1. " + (new Random()).nextInt()));
	        cqs1.sendMessage(new SendMessageRequest(queueUrl1, "This is my message text 2. " + (new Random()).nextInt()));
	        cqs1.sendMessage(new SendMessageRequest(queueUrl1, "This is my message text 3. " + (new Random()).nextInt()));
	 
	        logger.info("Receiving messages from " + queueUrl1);
	        
	        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl1);
	
	        List<Message> messages = new ArrayList<Message>();
	        receiveMessageRequest.setVisibilityTimeout(600);
	
	        receiveMessageRequest.setMaxNumberOfMessages(2);
	        messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
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
	
	        List<Message> messages2 = cqs1.receiveMessage(receiveMessageRequest).getMessages();
	
	        assertTrue("Expected one message, instead found " + messages2.size(), messages2.size() == 1);

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
	        
	        if (messages.size() > 0) {
	        	
	            logger.info("Deleting a message");
	            
	            String messageRecieptHandle = messages.get(0).getReceiptHandle();
	            cqs1.deleteMessage(new DeleteMessageRequest(queueUrl1, messageRecieptHandle));
	        }
	        
        } catch (AmazonServiceException ase) {
        	logger.error("test failed", ase);
        	fail(ase.getMessage());
        }
    }

    @Test
    public void testChangeMessageVisibility() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
    	
        try {
        	
            String queueUrl = getQueueUrl(1, USR.USER1);
            cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));
            
            Thread.sleep(1000);
            
            // send message
            
            logger.info("Sending a message to " + queueUrl);
            
            cqs1.sendMessage(new SendMessageRequest(queueUrl, "This is my message text. " + (new Random()).nextInt()));
            
            // receive message
            
            logger.info("Receiving messages from " + queueUrl);
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(10);
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();

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
  
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 0 messages, instead found "+ messages.size(), messages.size() == 0);
            
            int timeoutSeconds = 10;
            int waitSeconds = 2;
            int waitedAlreadySeconds = 0;

            logger.info("Changing visibility timeout to " + timeoutSeconds);
            
            ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, timeoutSeconds);
            cqs1.changeMessageVisibility(changeMessageVisibilityRequest);
            
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
            
            // keep checking

            while (messages.size() == 0) {

            	if (waitedAlreadySeconds > timeoutSeconds + 2*waitSeconds) {
            		fail("Message did not become revisible after " + waitedAlreadySeconds + " seconds");
            	}
            	
            	Thread.sleep(waitSeconds*1000);
            	waitedAlreadySeconds += waitSeconds;
            	logger.info("Checking for messages for " + waitedAlreadySeconds + " seconds");
                messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
            }
            

            assertTrue("Received " + messages.size() + " instead of 1 message", messages.size() == 1);
            assertTrue("Message content dorky: " + messages.get(0).getBody(), messages.get(0).getBody().startsWith("This is my message text."));
            
            // delete message
            
            logger.info("Deleting message with receipt handle " + messages.get(0).getReceiptHandle());
            cqs1.deleteMessage(new DeleteMessageRequest(queueUrl, messages.get(0).getReceiptHandle()));

            
        } catch (AmazonServiceException ase) {
        	logger.error("test failed", ase);
            fail(ase.getMessage());
        }
    }
    
    @Test
    public void testMessageVisibilityOnReceive() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
    	
        try {
        	
            String queueUrl = getQueueUrl(1, USR.USER1);
            
            Thread.sleep(1000);
            
            // send message
            
            logger.info("Sending a message to " + queueUrl);
            
            cqs1.sendMessage(new SendMessageRequest(queueUrl, "This is my message text. " + (new Random()).nextInt()));
            
            // receive message
            
            logger.info("Receiving messages from " + queueUrl);
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            int timeoutSeconds = 40;

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(timeoutSeconds);
            receiveMessageRequest.setMaxNumberOfMessages(10);
            receiveMessageRequest.setWaitTimeSeconds(2);
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 1 message, instead found " + messages.size(), messages.size() == 1);

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
            
            // message should be invisible now
            
            logger.info("Trying to receive message until it becomes revisible");
  
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 0 messages, instead found "+ messages.size(), messages.size() == 0);
            
            long start = System.currentTimeMillis();

            // keep checking

            int waitedAlreadySeconds = 0;
            
            while (messages.size() == 0) {
            	logger.info("Checking for messages for " + waitedAlreadySeconds + " seconds");
                messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
                waitedAlreadySeconds = (int)((System.currentTimeMillis()-start)/1000); 
            	if (waitedAlreadySeconds > (int)(timeoutSeconds*1.1)) {
            		fail("Message did not become revisible after " + waitedAlreadySeconds + " seconds");
            	}
            }
            
            assertTrue("Message became visible too soon after " + waitedAlreadySeconds + " sec", waitedAlreadySeconds < (int)(timeoutSeconds*1.1));
            assertTrue("Received " + messages.size() + " instead of 1 message", messages.size() == 1);
            assertTrue("Message content dorky: " + messages.get(0).getBody(), messages.get(0).getBody().startsWith("This is my message text."));
            
            // delete message
            
            logger.info("Deleting message with receipt handle " + messages.get(0).getReceiptHandle());
            cqs1.deleteMessage(new DeleteMessageRequest(queueUrl, messages.get(0).getReceiptHandle()));

            // check if message reappears
            
            waitedAlreadySeconds = 0;
            start = System.currentTimeMillis();
            
            for (int i=0; i<10; i++) {
            	logger.info("Checking for messages for " + waitedAlreadySeconds + " seconds");
                messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
                waitedAlreadySeconds = (int)((System.currentTimeMillis()-start)/1000); 
            	assertTrue("Message reappeared", messages.size() == 0);
            }
            
        } catch (AmazonServiceException ase) {
        	logger.error("test failed", ase);
            fail(ase.getMessage());
        }
    }

    @Test
    public void testMessageVisibilityOnReceiveLongPoll() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
    	
        try {
        	
            final String queueUrl = getQueueUrl(1, USR.USER1);
            Thread.sleep(1000);
            
    		final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            receiveMessageRequest.setVisibilityTimeout(120);
            receiveMessageRequest.setMaxNumberOfMessages(10);
            receiveMessageRequest.setWaitTimeSeconds(20);

            (new Thread() {
            	public void run() {
            		long ts1 = System.currentTimeMillis();
            		logger.info("event=lp_receive vto=120 wt=20");
                    List<Message> messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
                    assertTrue("Expected 1 message, instead found " + messages.size(), messages.size() == 1);
            		long ts2 = System.currentTimeMillis();
            		logger.info("event=message_found duration=" + (ts2-ts1));
            	}
            }).start();

            Thread.sleep(100);           
            
            // send message
            logger.info("event=send_message queue_url=" + queueUrl);
            cqs1.sendMessage(new SendMessageRequest(queueUrl, "This is my message text. " + (new Random()).nextInt()));
            Thread.sleep(100);

            // receive message
            
            // message should be invisible now
  
            List<Message> messages = null;
            
            long ts = System.currentTimeMillis();
            
            for (int i=0; i<5; i++) {
	            logger.info("event=receive");
	            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
	            if (messages.size() > 0) {
	            	logger.info("event=message_found delay=" + (System.currentTimeMillis()-ts));
	            }
	            assertTrue("Expected 0 messages, instead found "+ messages.size(), messages.size() == 0);
            }
            
            for (int i=0; i<5; i++) {
	            logger.info("event=receive");
	            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
	            if (messages.size() > 0) {
	            	logger.info("event=message_found delay=" + (System.currentTimeMillis()-ts));
	            	return;
	            }
            }
            
            fail("message not found any more");

        } catch (AmazonServiceException ase) {
        	logger.error("test failed", ase);
            fail(ase.getMessage());
        }
    }

    @Test
    public void testSendDeleteLargeMessage() throws InterruptedException, PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
    	
        try {
        	
            String queueUrl = getQueueUrl(1, USR.USER1);
            cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

            // send a batch of messages
            
            StringBuffer msg256k = new StringBuffer("");
            
            // 256k msg
            
            for (int i=0; i<256000; i++) {
            	msg256k.append("X");
            }

            logger.info("Sending large message to " + queueUrl);
            SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, msg256k.toString());
            SendMessageResult sendResult = cqs1.sendMessage(sendMessageRequest);
            
            assertNotNull("Message id is null", sendResult.getMessageId());

            StringBuffer msg280k = new StringBuffer("");
            
            // 280k msg
            
            for (int i=0; i<280000; i++) {
            	msg280k.append("X");
            }

            try {
	            logger.info("Sending too large message to " + queueUrl);
	            sendMessageRequest = new SendMessageRequest(queueUrl, msg280k.toString());
	            sendResult = cqs1.sendMessage(sendMessageRequest);
	            fail("should not accept messages > 256k");
            } catch (Exception ex) {
            	assertTrue("wrong error, expected message too long", ex.getMessage().contains("body must be shorter than"));
            }

            // receive messages
            
            logger.info("Receiving messages from " + queueUrl);

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(1);
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
            
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
            cqs1.deleteMessage(deleteMessageRequest);
            
        } catch (AmazonServiceException ase) {
        	logger.error("test failed", ase);
            fail(ase.getMessage());
        }
    }

    @Test
    public void testSendDeleteMessage() throws InterruptedException, PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
    	
        try {
        	
            String queueUrl = getQueueUrl(1, USR.USER1);
            cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

            // send a batch of messages
            
            logger.info("Sending message to " + queueUrl);
            SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, "This is a test message");
            SendMessageResult sendResult = cqs1.sendMessage(sendMessageRequest);
            
            assertNotNull("Message id is null", sendResult.getMessageId());

            // receive messages
            
            logger.info("Receiving messages from " + queueUrl);

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(10);
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
            
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
            cqs1.deleteMessage(deleteMessageRequest);
            
        } catch (AmazonServiceException ase) {
        	logger.error("test failed", ase);
            fail(ase.getMessage());
        }
    }
    
    @Test
    public void testSendDeleteMessageBatch() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {
    	
        try {
        	
            String queueUrl = getQueueUrl(1, USR.USER1);
            cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

            // send batch of messages

            logger.info("Sending batch messages to " + queueUrl);
            
            SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(queueUrl);
            List<SendMessageBatchRequestEntry> sendEntryList = new ArrayList<SendMessageBatchRequestEntry>();

            for (int i = 0; i < 5; i++) {
                sendEntryList.add(new SendMessageBatchRequestEntry("msg_" + i, "This is a test message: batch " + i));
            }
            
            sendMessageBatchRequest.setEntries(sendEntryList);
            cqs1.sendMessageBatch(sendMessageBatchRequest);
            
            Thread.sleep(1000);

            // receive messages
            
            logger.info("Receiving messages from " + queueUrl);
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(10);
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();

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
            cqs1.deleteMessageBatch(deleteMessageBatchRequest);
            
        } catch (AmazonServiceException ase) {
        	logger.error("test failed", ase);
            fail(ase.getMessage());
        }       
    }

    @Test
    public void testChangeMessageVisibilityBatch() throws InterruptedException, PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
    	
        try {
        	
            String queueUrl = getQueueUrl(1, USR.USER1);
            cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

            // send batch of messages

            logger.info("Sending batch messages to " + queueUrl);
            
            SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(queueUrl);
            List<SendMessageBatchRequestEntry> sendEntryList = new ArrayList<SendMessageBatchRequestEntry>();

            for (int i = 0; i < 5; i++) {
                sendEntryList.add(new SendMessageBatchRequestEntry("msg_" + i, "This is a test message: batch " + i));
            }
            
            sendMessageBatchRequest.setEntries(sendEntryList);
            cqs1.sendMessageBatch(sendMessageBatchRequest);
            
            Thread.sleep(1000);
            
            // receive messages
            
            logger.info("Receiving messages from " + queueUrl);
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setMaxNumberOfMessages(10);
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();

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
            cqs1.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
            
            // check if messages invisible
            
            messages = new ArrayList<Message>();
            receiveMessageRequest.setMaxNumberOfMessages(10);
            messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 0 messages, received " + messages.size(), messages.size() == 0);
            
            Thread.sleep(11000);

            // check if messages revisible
            
            messages = null;
            receiveMessageRequest.setMaxNumberOfMessages(10);
            receiveMessageRequest.setWaitTimeSeconds(1);
            while (messages == null || messages.size() == 0) {
            	logger.info("event=scanning_for_messages");
            	messages = cqs1.receiveMessage(receiveMessageRequest).getMessages();
            }

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
            cqs1.deleteMessageBatch(deleteMessageBatchRequest);
            
        } catch (AmazonServiceException ase) {
            fail(ase.toString());
        }       
    }
}
