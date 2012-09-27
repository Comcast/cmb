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
import java.util.Map;
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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
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
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cmb.common.util.Util;
import com.comcast.plaxo.cqs.persistence.CQSMessagePartitionedCassandraPersistence;
import com.comcast.plaxo.cqs.persistence.ICQSMessagePersistence;
import com.comcast.plaxo.cqs.persistence.RedisCachedCassandraPersistence;
import com.comcast.cmb.test.tools.AWSCredentialsHolder;
import com.comcast.cqs.util.CQSErrorCodes;

public class CQSIntegrationTest {

    private static Logger logger = Logger.getLogger(CQSIntegrationTest.class);

    private boolean useAmazonEndpoint = false;
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
        	
            if (useAmazonEndpoint) {
            	
               	// be sure to fill in your AWS access credentials in the AwsCredentials.properties file before running this against AWS

            	AWSCredentials awsCredentials = AWSCredentialsHolder.initAwsCredentials();
                
                sqs = new AmazonSQSClient(awsCredentials);
                sqs.setEndpoint("http://sqs.us-west-1.amazonaws.com/");
                
                sns = new AmazonSNSClient(awsCredentials);
                
            } else {
            	
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
            }
            
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
	    				fail("message is not a confirmation messsage");
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
					fail("no confirmation message found");
				}
			} else {
				fail("no confirmation requested");
			}
			
			PublishRequest publishRequest = new PublishRequest();
			String messageText = "quamvis sint sub aqua, sub aqua maledicere temptant";
			publishRequest.setMessage(messageText);
			publishRequest.setSubject("unit test message");
			publishRequest.setTopicArn(topicArn);
			PublishResult publishResponse = sns.publish(publishRequest);
			
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
				fail("no messages found");
			}
			
			DeleteTopicRequest  deleteTopicRequest = new DeleteTopicRequest(topicArn);
			sns.deleteTopic(deleteTopicRequest);
			
    	} catch (Exception ex) {
    		fail("unit test failed with exception: " + ex.getMessage());
    	}
    }
    
    @Test
    public void testGetSetQueueAttributes() throws PersistenceException {
    	
    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
        createQueueRequest.setAttributes(attributeParams);
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(myQueueUrl);
        
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest();
        getQueueAttributesRequest.setQueueUrl(myQueueUrl);
        getQueueAttributesRequest.setAttributeNames(Arrays.asList("VisibilityTimeout", "MessageRetentionPeriod", "All"));
        GetQueueAttributesResult result = sqs.getQueueAttributes(getQueueAttributesRequest);
        assertTrue(result.getAttributes().get("MessageRetentionPeriod").equals("600"));
        assertTrue(result.getAttributes().get("VisibilityTimeout").equals("30"));
        
        SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest();
        setQueueAttributesRequest.setQueueUrl(myQueueUrl);
        HashMap<String, String> attributes = new HashMap<String, String>();
        attributes.put("MessageRetentionPeriod", "300");
        attributes.put("VisibilityTimeout", "80");
        attributes.put("MaximumMessageSize", "10240");
        attributes.put("DelaySeconds", "100");
        String policy = "{\"Version\":\"2008-10-17\",\"Id\":\""+myQueueUrl+"/SQSDefaultPolicy\",\"Statement\":[{\"Sid\":\"test\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\""+user.getUserId()+"\"},\"Action\":\"CQS:SendMessage\",\"Resource\":\""+com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(myQueueUrl)+"\"}]}";
        attributes.put("Policy", policy);
        setQueueAttributesRequest.setAttributes(attributes);
        sqs.setQueueAttributes(setQueueAttributesRequest);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }
        
        result = sqs.getQueueAttributes(getQueueAttributesRequest);
        assertTrue(result.getAttributes().get("MessageRetentionPeriod").equals("300"));
        assertTrue(result.getAttributes().get("VisibilityTimeout").equals("80"));
        assertTrue(result.getAttributes().get("MaximumMessageSize").equals("10240"));
        assertTrue(result.getAttributes().get("DelaySeconds").equals("100"));

        attributes = new HashMap<String, String>(){ {put("VisibilityTimeout", "100");}};
        setQueueAttributesRequest.setAttributes(attributes);
        sqs.setQueueAttributes(setQueueAttributesRequest);
        
        result = sqs.getQueueAttributes(getQueueAttributesRequest);
        assertTrue(result.getAttributes().get("VisibilityTimeout").equals("100"));

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }
        
        // try triggering missing parameter error
        
        try {

        	setQueueAttributesRequest = new SetQueueAttributesRequest();
	        setQueueAttributesRequest.setQueueUrl(myQueueUrl);
	        sqs.setQueueAttributes(setQueueAttributesRequest);

        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().equals(CQSErrorCodes.MissingParameter.getCMBCode()));
        }
        
        // try trigger unknown attribute name error
        
        try {

	        getQueueAttributesRequest = new GetQueueAttributesRequest();
	        getQueueAttributesRequest.setQueueUrl(myQueueUrl);
	        getQueueAttributesRequest.setAttributeNames(Arrays.asList("all"));
	        sqs.getQueueAttributes(getQueueAttributesRequest);

        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().equals(CQSErrorCodes.InvalidAttributeName.getCMBCode()));
        }

        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(myQueueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(myQueueUrl);
    }
    
    @Test
    public void testAddRemovePermission() throws PersistenceException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(myQueueUrl);
        
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }
        
        AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
        addPermissionRequest.setQueueUrl(myQueueUrl);
        addPermissionRequest.setActions(Arrays.asList("SendMessage"));
        addPermissionRequest.setLabel("testLabel");
        addPermissionRequest.setAWSAccountIds(Arrays.asList(user1.getUserId(), user2.getUserId()));        
        sqs.addPermission(addPermissionRequest);

        addPermissionRequest.setLabel("testLabel2");
        addPermissionRequest.setActions(Arrays.asList("SendMessage", "GetQueueUrl"));
        sqs.addPermission(addPermissionRequest);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }
        
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest();
        getQueueAttributesRequest.setAttributeNames(Arrays.asList("All"));
        getQueueAttributesRequest.setQueueUrl(myQueueUrl);
        GetQueueAttributesResult res = sqs.getQueueAttributes(getQueueAttributesRequest);
        res = sqs.getQueueAttributes(getQueueAttributesRequest);
        
        assertTrue(res.toString().contains("testLabel") && res.toString().contains("testLabel2"));
        
        RemovePermissionRequest removePermissionRequest = new RemovePermissionRequest();
        removePermissionRequest.setLabel("testLabel");
        removePermissionRequest.setQueueUrl(myQueueUrl);
        sqs.removePermission(removePermissionRequest);
        removePermissionRequest.setLabel("testLabel2");
        sqs.removePermission(removePermissionRequest);
        
        try {
            Thread.sleep(1000);
        } catch (Exception e) {    
        }    
                
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(myQueueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(myQueueUrl);
    }
    

    @Test
    public void testInvalidRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(myQueueUrl);
        
        if (!useAmazonEndpoint) {
            ICQSMessagePersistence messagePersistence = new CQSMessagePartitionedCassandraPersistence();
            messagePersistence.clearQueue(myQueueUrl);
        }

        logger.info("Send a message with empty message body");
        
        try {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.setQueueUrl(myQueueUrl);
            sqs.sendMessage(sendMessageRequest);
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().equals(CQSErrorCodes.MissingParameter.getCMBCode()));
            //displayServiceException(ase);
        }

        logger.info("Send a message with invalid DelaySeconds");
        
        try {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.setQueueUrl(myQueueUrl);
            String msg = "This is a message to test invalid delay seconds;";
            sendMessageRequest.setMessageBody(msg);
            sendMessageRequest.setDelaySeconds(1000);
            sqs.sendMessage(sendMessageRequest);
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
            //displayServiceException(ase);
        }

        logger.info("Send a very long message");
        
        try {
            
        	SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.setQueueUrl(myQueueUrl);
            String msg = "This is a message to test too longggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg. ";
            
            while (msg.length() < 32 * 1024) {
                msg += msg;
            }
            
            sendMessageRequest.setMessageBody(msg);
            sqs.sendMessage(sendMessageRequest);
        
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().equals(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
            //displayServiceException(ase);
        }

        logger.info("Receive messages with invalid max number of messages");
        
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            receiveMessageRequest.setVisibilityTimeout(10);
            receiveMessageRequest.setMaxNumberOfMessages(12);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
            //displayServiceException(ase);
        }

        logger.info("Receive messages with invalid max number of messages");
        
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            receiveMessageRequest.setVisibilityTimeout(10);
            receiveMessageRequest.setMaxNumberOfMessages(0);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.InvalidParameterValue.getCMBCode()));
            //displayServiceException(ase);
        }
        
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(myQueueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(myQueueUrl);
    }

    @Test
    public void testInvalidBatchDeleteRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(myQueueUrl);
        
        if (!useAmazonEndpoint) {
            ICQSMessagePersistence messagePersistence = new CQSMessagePartitionedCassandraPersistence();
            messagePersistence.clearQueue(myQueueUrl);
        }

        logger.info("Send a batch of messages:");
        
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
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            SendMessageBatchResult result = sqs.sendMessageBatch(batchSendRequest);
            
            Thread.sleep(1000);
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);

        }

        logger.info("Receiving messages from " + myQueueUrl + ".\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

        List<Message> messages = new ArrayList<Message>();
        receiveMessageRequest.setVisibilityTimeout(60);
        receiveMessageRequest.setMaxNumberOfMessages(6);
        messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        
        assertTrue("Make sure we received messages we just sent: ", messages.size() > 0);
        
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

        logger.info("Delete a batch of messages with same supplied id:");
        
        try {
            DeleteMessageBatchRequest batchDeleteRequest = new DeleteMessageBatchRequest(myQueueUrl, deleteMsgList);
            DeleteMessageBatchResult deleteResult = sqs.deleteMessageBatch(batchDeleteRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Duplicate ids in DeleteMessageBatchRequest: ", ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
            //displayServiceException(ase);
        }

        //BW disabled for now
        
        /*logger.info("Delete a batch of messages with empty or invalid supplied id:");
        
        try {
            deleteMsgList.get(0).setId("bad.id");
            DeleteMessageBatchRequest batchDeleteRequest = new DeleteMessageBatchRequest(myQueueUrl, deleteMsgList);
            DeleteMessageBatchResult deleteResult = sqs.deleteMessageBatch(batchDeleteRequest);
        } catch (AmazonServiceException ase) {
            assertTrue("Invalid supplied id in DeleteMessageBatchRequest: ", ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getAwsCode()));
            //displayServiceException(ase);
        }*/

        logger.info("Delete a batch of messages with empty ReceiptHandle:");
        
        try {
            deleteMsgList.get(0).setId("somerandomid");
            deleteMsgList.get(deleteMsgList.size() - 1).setId("some-random-id");
            deleteMsgList.get(0).setReceiptHandle("somerandomestring");
            DeleteMessageBatchRequest batchDeleteRequest = new DeleteMessageBatchRequest(myQueueUrl, deleteMsgList);
            DeleteMessageBatchResult deleteResult = sqs.deleteMessageBatch(batchDeleteRequest);
            int y =0 ;
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().equals(CQSErrorCodes.InternalError.getCMBCode()));
            //displayServiceException(ase);
        }
        
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(myQueueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(myQueueUrl);
    }

    @Test
    public void testInvalidBatchChangeMessageVisibilityRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(myQueueUrl);
        
        if (!useAmazonEndpoint) {
            ICQSMessagePersistence messagePersistence = new CQSMessagePartitionedCassandraPersistence();
            messagePersistence.clearQueue(myQueueUrl);
        }

        logger.info("Send a batch of messages:");
        
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
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            SendMessageBatchResult result = sqs.sendMessageBatch(batchSendRequest);
            Thread.sleep(1000);
            
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);
        }

        logger.info("Receiving messages from " + myQueueUrl + ".\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

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

        logger.info("Change a batch of message visibility timeout with same supplied id:");
        
        try {
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(myQueueUrl, msgList);
            ChangeMessageVisibilityBatchResult result = sqs.changeMessageVisibilityBatch(batchRequest);
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
            //displayServiceException(ase);
        }

        logger.info("Change a batch of messages with empty or invalid supplied id:");
        
        try {
            msgList.get(0).setId("bad.id");
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(myQueueUrl, msgList);
            ChangeMessageVisibilityBatchResult result = sqs.changeMessageVisibilityBatch(batchRequest);
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getCMBCode()));
            //displayServiceException(ase);
        }

        logger.info("Change a batch of messages with empty ReceiptHandle:");
        
        try {
            msgList.get(0).setId("somerandomid");
            msgList.get(msgList.size() - 1).setId("some-random-id");
            msgList.get(0).setReceiptHandle("somerandomestring");
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest(myQueueUrl, msgList);
            ChangeMessageVisibilityBatchResult result = sqs.changeMessageVisibilityBatch(batchRequest);
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.InternalError.getCMBCode()));
        }
        
        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(myQueueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(myQueueUrl);
    }

    @Test
    public void testInvalidBatchSendRequest() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_PREFIX + randomGenerator.nextLong());
        createQueueRequest.setAttributes(attributeParams);
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(myQueueUrl);
        
        if (!useAmazonEndpoint) {
            ICQSMessagePersistence messagePersistence = new CQSMessagePartitionedCassandraPersistence();
            messagePersistence.clearQueue(myQueueUrl);
        }

        SendMessageBatchResult result = null;

        logger.info("Send a batch of messages with empty supplied Id");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("id.1", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("id3", "This is the third message in a batch")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            result = sqs.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getCMBCode()));
            //displayServiceException(ase);
        }

        logger.info("Send a batch of messages with empty message");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("id1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("id2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("id3", "")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            result = sqs.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        }

        logger.info("Send a batch of messages with same supplied id");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("1", "Test")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            result = sqs.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.BatchEntryIdsNotDistinct.getCMBCode()));
            //displayServiceException(ase);
        }

        logger.info("Send a batch of messages with supplied id too long");
        
        try {
        	
            List<SendMessageBatchRequestEntry> messageList = Arrays.asList(
                new SendMessageBatchRequestEntry("1", "This is the first message in a batch"),
                new SendMessageBatchRequestEntry("2", "This is the second message in a batch"),
                new SendMessageBatchRequestEntry("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", "Test")
            );
            
            SendMessageBatchRequest batchSendRequest = new SendMessageBatchRequest(myQueueUrl, messageList);
            result = sqs.sendMessageBatch(batchSendRequest);
            
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.InvalidBatchEntryId.getCMBCode()));
            //displayServiceException(ase);
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
            result = sqs.sendMessageBatch(batchSendRequest);
        } catch (AmazonServiceException ase) {
            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.BatchRequestTooLong.getCMBCode()));
            //displayServiceException(ase);
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
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        randomQueueUrls.add(myQueueUrl);

        sqs.addPermission(new AddPermissionRequest(myQueueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage", "DeleteMessage")));

        try {
        	sqs.addPermission(new AddPermissionRequest(myQueueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage")));
        } catch (Exception ex) {
        	assertTrue("should not be able to add permssion with same label", ex.getMessage().contains("Already exists"));
        }
        
        sqs.removePermission(new RemovePermissionRequest(myQueueUrl, "label1"));
        
    	sqs.addPermission(new AddPermissionRequest(myQueueUrl, "label1", Arrays.asList(user1.getUserId()), Arrays.asList("SendMessage")));

    	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
        deleteQueueRequest.setQueueUrl(myQueueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        
        randomQueueUrls.remove(myQueueUrl);
    }

    @Test
    public void testCreateDeleteQueue() {

        String myQueueUrl = "";
        String myQueueUrl2 = "";

        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);

            createQueueRequest.setAttributes((new HashMap<String, String>() {
                {
                    put("MessageRetentionPeriod", "600");
                }
            }
            ));
            
            myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl);

            sqs.receiveMessage(new ReceiveMessageRequest(myQueueUrl));

            String qName2 = QUEUE_PREFIX + randomGenerator.nextLong();
            createQueueRequest = new CreateQueueRequest(qName2);
            myQueueUrl2 = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl2);

            sqs.receiveMessage(new ReceiveMessageRequest(myQueueUrl2));

            Thread.sleep(1000);

            // List queues
            
            logger.info("Listing all queues in your account.\n");
            
            List<String> queueUrls = sqs.listQueues().getQueueUrls();

            for (String queueUrl : queueUrls) {
                logger.info("QueueUrl: " + queueUrl);
            }
            
            assertTrue("Expected 2 queue urls but got " + queueUrls.size(), queueUrls.size() == 2);
            assertTrue("Missing queue url " + myQueueUrl, queueUrls.contains(myQueueUrl));
            assertTrue("Missing queue url " + myQueueUrl2, queueUrls.contains(myQueueUrl2));

            // Send a message
            
            logger.info("Sending a message to MyQueue.\n");
            sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text 1. " + (new Random()).nextInt()));
            sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text 2. " + (new Random()).nextInt()));
            sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text 3. " + (new Random()).nextInt()));
            Thread.sleep(1000);
            
            // send message batch
            
            String messageBody = "Test message";
            
            for (int i=0; i< 10000; i++) {
            	messageBody += i;
            }

            // Receive messages
            
            logger.info("Receiving messages from MyQueue.\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(600);

            receiveMessageRequest.setMaxNumberOfMessages(2);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            assertTrue(messages.size() == 2);

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
            
            assertTrue(messages2.size() == 1);

            if (messages.size() > 0) {
                logger.info("Deleting a message.\n");
                String messageRecieptHandle = messages.get(0).getReceiptHandle();

                sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
            }

            DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(myQueueUrl);
            
            deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl2);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(myQueueUrl2);

        } catch (AmazonServiceException ase) {
        	sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
        	fail("exception=" + ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not " + "being able to access the network.", ace);
            fail("exception="+ace);
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);
        }
    }
    
    @Test
    public void testCreateDuplicateQueue() {

        String myQueueUrl = "";

        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            logger.info("Creating a new SQS queue called MyQueue.\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);

            createQueueRequest.setAttributes((new HashMap<String, String>() {
                {
                    put("MessageRetentionPeriod", "60");
                }
            }
            ));

            myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl);
            
            // Create a duplicate name queue
            
            String qUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            
            assert(qUrl.equals(myQueueUrl));
            
            // Create a duplicate name queue with different attribute

            createQueueRequest.setAttributes((new HashMap<String, String>() {
                {
                    put("MessageRetentionPeriod", "30");
                }
            }
            ));
            
            qUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

        } catch (AmazonServiceException ase) {
        	
        	// This is a proper error message
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(myQueueUrl);

            assertTrue(ase.getErrorCode().contains(CQSErrorCodes.QueueNameExists.getCMBCode()));
            //displayServiceException(ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not " + "being able to access the network.", ace);
            fail("Error Message: " + ace.getMessage());
        }
    }

    @Test
    public void testChangeMessageVisibilityTO() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
    	
        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            logger.info("Creating a new CQS queue: " + qName + ":\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
            createQueueRequest.setAttributes(attributeParams);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl);
            
            if (!useAmazonEndpoint) {
	            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
	            messagePersistence.clearQueue(myQueueUrl);
            }
            
            // Send a message
            
            Thread.sleep(1000);
            logger.info("Sending a message to " + qName + ":\n");
            sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text. " + (new Random()).nextInt()));
            Thread.sleep(1000);

            // Receive messages
            
            logger.info("Receiving messages from " + qName + ":\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(2);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            logger.info("Check if only receive one message (visibilityTO=60s):\n");
            assertTrue(messages.size() == 1);

            logger.info("Getting receipt handle:\n");
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
            
            // the message should be invisible now
            
            logger.info("Trying to receive message, none expected:\n");
            List<Message> messages2 = sqs.receiveMessage(receiveMessageRequest).getMessages();
            assertTrue(messages2.size() == 0);

            logger.info("Changing visibility timeout to 2s:\n");
            ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest(myQueueUrl, receiptHandle, 2);
            sqs.changeMessageVisibility(changeMessageVisibilityRequest);
            
            logger.info("Waiting for 3 sec:\n");
            Thread.sleep(3000);
            
            logger.info("Receiving messages from " + qName + ", expect one returned:\n");
            List<Message> messages3 = sqs.receiveMessage(receiveMessageRequest).getMessages();

            if (messages3.size() == 0) {
            	Thread.sleep(2000);
            	messages3 = sqs.receiveMessage(receiveMessageRequest).getMessages();
            }
            
            assertTrue(messages3.size() == 1);

            // Delete a message
            
            if (messages.size() > 0) {
                logger.info("Deleting a message.\n");
                String messageRecieptHandle = messages.get(0).getReceiptHandle();

                sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
            }

        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(myQueueUrl);
            
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not " + "being able to access the network.", ace);
            fail("Error Message: " + ace.getMessage());
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);
        }
    }

    @Test
    public void testChangeMessageVisibilityToLong() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
    	
        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            logger.info("Creating a new CQS queue " + qName);
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
            //createQueueRequest.setAttributes(attributeParams);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl);
            
            if (!useAmazonEndpoint) {
	            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
	            messagePersistence.clearQueue(myQueueUrl);
            }
            
            // send message
            
            Thread.sleep(1000);
            logger.info("Sending a message to " + qName);
            sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text. " + (new Random()).nextInt()));
            Thread.sleep(1000);

            // receive message
            
            logger.info("Receiving messages from " + qName );
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(2);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            logger.info("Check if only receive one message (visibilityTO=60s)");
            assertTrue(messages.size() == 1);

            logger.info("Getting receipt handle");
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
            
            // the message should be invisible now
            
            logger.info("Trying to receive message, none expected");
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            assertTrue(messages.size() == 0);
            
            int timeoutSeconds = 1500;
            int waitSeconds = 10;
            int waitedAlreadySeconds = 0;

            logger.info("Changing visibility timeout to " + timeoutSeconds);
            ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest(myQueueUrl, receiptHandle, timeoutSeconds);
            sqs.changeMessageVisibility(changeMessageVisibilityRequest);
            
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            
            // keep checking

            while (messages.size() == 0) {

            	if (waitedAlreadySeconds > timeoutSeconds + 2*waitSeconds) {
            		fail("message did not become revisible after " + waitedAlreadySeconds + " seconds");
            	}
            	
            	Thread.sleep(waitSeconds*1000);
            	waitedAlreadySeconds += waitSeconds;
            	logger.info("checking for messages for " + waitedAlreadySeconds + " seconds");
                messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            }
            

            assertTrue("received " + messages.size() + " instead of 1 message", messages.size() == 1);
            assertTrue("message content dorky: " + messages.get(0).getBody(), messages.get(0).getBody().startsWith("This is my message text."));
            
            // delete message
            
            logger.info("Deleting message with receipt handle " + messages.get(0).getReceiptHandle());
            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messages.get(0).getReceiptHandle()));

            // delete queue
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(myQueueUrl);
            
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not " + "being able to access the network.", ace);
            fail("Error Message: " + ace.getMessage());
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);
        }
    }
    
    @Test
    public void testSendDeleteMessage() {
    	
        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            logger.info("Creating a new SQS queue: " + qName + ":\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
            createQueueRequest.setAttributes(attributeParams);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl);
            
            if (!useAmazonEndpoint) {
	            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
	            messagePersistence.clearQueue(myQueueUrl);
            }

            // send a batch of messages
            
            Thread.sleep(1000);

            logger.info("Sending batch messages to " + qName + ":\n");
            SendMessageRequest sendMessageRequest = new SendMessageRequest(myQueueUrl, "This is a test message");
            SendMessageResult sendResult = sqs.sendMessage(sendMessageRequest);
            Map<String, String> idMsg = new HashMap<String, String>();
            assertNotNull(sendResult.getMessageId());
            Thread.sleep(1000);

            // receive messages
            
            logger.info("Receiving messages from " + qName + ":\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(1);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            logger.info("Check if we receive all five messages:\n");
            assertTrue(messages.size() == 1);

            logger.info("Getting receipt handle:\n");
            
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
            
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(myQueueUrl, messages.get(0).getReceiptHandle());
            sqs.deleteMessage(deleteMessageRequest);
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(myQueueUrl);

        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not " + "being able to access the network.", ace);
            fail("Error Message: " + ace.getMessage());
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);

        } catch (Exception ex) {
            logger.error("exception", ex);
            fail("exception="+ex);
        }
    }
    
    @Test
    public void testSendDeleteMessageBatch() {
    	
        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            logger.info("Creating a new SQS queue: " + qName + ":\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
            createQueueRequest.setAttributes(attributeParams);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl);
            
            if (!useAmazonEndpoint) {
	            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
	            messagePersistence.clearQueue(myQueueUrl);
            }
            
            Thread.sleep(1000);

            // send batch of messages

            logger.info("Sending batch messages to " + qName + ":\n");
            SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(myQueueUrl);
            List<SendMessageBatchRequestEntry> sendEntryList = new ArrayList<SendMessageBatchRequestEntry>();

            for (int i = 0; i < 5; i++) {
                sendEntryList.add(new SendMessageBatchRequestEntry("msg_" + i, "This is a test message: batch " + i));
            }
            
            sendMessageBatchRequest.setEntries(sendEntryList);
            SendMessageBatchResult sendBatchResult = sqs.sendMessageBatch(sendMessageBatchRequest);
            
            Thread.sleep(1000);

            // receive messages
            
            logger.info("Receiving messages from " + qName + ":\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

            List<Message> messages = new ArrayList<Message>();
            receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            logger.info("Check if we receive all five messages:\n");
            assertTrue(messages.size() == 5);

            List<DeleteMessageBatchRequestEntry> deleteEntryList = new ArrayList<DeleteMessageBatchRequestEntry>();

            logger.info("Getting receipt handle:\n");
            
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
            
            DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(myQueueUrl);
            deleteMessageBatchRequest.setEntries(deleteEntryList);
            DeleteMessageBatchResult deleteBatchResult = sqs.deleteMessageBatch(deleteMessageBatchRequest);
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            randomQueueUrls.remove(myQueueUrl);
            
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not " + "being able to access the network.", ace);
            fail("Error Message: " + ace.getMessage());
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);
        }  catch (Exception e) {
            logger.error("exception", e);
            fail("exception="+e);
        }       
    }

    @Test
    public void testChangeMessageVisibilityBatch() {
    	
        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            logger.info("Creating a new SQS queue: " + qName + ":\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
            createQueueRequest.setAttributes(attributeParams);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl);
            
            if (!useAmazonEndpoint) {
	            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
	            messagePersistence.clearQueue(myQueueUrl);
            }
            
            Thread.sleep(1000);

            // send batch of messages

            logger.info("Sending batch messages to " + qName + ":\n");
            SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(myQueueUrl);
            List<SendMessageBatchRequestEntry> sendEntryList = new ArrayList<SendMessageBatchRequestEntry>();

            for (int i = 0; i < 5; i++) {
                sendEntryList.add(new SendMessageBatchRequestEntry("msg_" + i, "This is a test message: batch " + i));
            }
            
            sendMessageBatchRequest.setEntries(sendEntryList);
            SendMessageBatchResult sendBatchResult = sqs.sendMessageBatch(sendMessageBatchRequest);
            
            Thread.sleep(1000);

            // receive messages
            
            logger.info("Receiving messages from " + qName + ":\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

            List<Message> messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            logger.info("Checking if we received all 5 messages");
            assertTrue("Expected 5 messages, received " + messages.size(), messages.size() == 5);

            // change message visibility batch to 10 sec for all messages

            int i = 0;
            
            ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
            changeMessageVisibilityBatchRequest.setQueueUrl(myQueueUrl);
            
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
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            assertTrue("Expected 0 messages, received " + messages.size(), messages.size() == 0);
            
            Thread.sleep(11000);

            // check if messages revisible
            
            messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
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
            
            DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(myQueueUrl);
            deleteMessageBatchRequest.setEntries(deleteEntryList);
            DeleteMessageBatchResult deleteBatchResult = sqs.deleteMessageBatch(deleteMessageBatchRequest);
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            Thread.sleep(1000);
            
            // check if messages deleted
            
            messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            assertTrue("Expected 0 messages, received " + messages.size(), messages.size() == 0);
            
            randomQueueUrls.remove(myQueueUrl);
            
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not " + "being able to access the network.", ace);
            fail("Error Message: " + ace.getMessage());
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);
        }  catch (Exception e) {
            logger.error("exception", e);
            fail("exception="+e);
        }       
    }

    //@Test
    public void testChangeMessageVisibilityBatchEkta() {
    	
        try {
        	
            String qName = QUEUE_PREFIX + randomGenerator.nextLong();
            logger.info("Creating a new SQS queue: " + qName + ":\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(qName);
            createQueueRequest.setAttributes(attributeParams);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            randomQueueUrls.add(myQueueUrl);
            
            if (!useAmazonEndpoint) {
	            ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
	            messagePersistence.clearQueue(myQueueUrl);
            }
            
            Thread.sleep(1000);

            // send batch of messages

            logger.info("Sending batch messages to " + qName + ":\n");
            SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(myQueueUrl);
            List<SendMessageBatchRequestEntry> sendEntryList = new ArrayList<SendMessageBatchRequestEntry>();

            for (int i = 0; i < 5; i++) {
                sendEntryList.add(new SendMessageBatchRequestEntry("msg_" + i, "msg " + i));
            }
            
            sendMessageBatchRequest.setEntries(sendEntryList);
            SendMessageBatchResult sendBatchResult = sqs.sendMessageBatch(sendMessageBatchRequest);
            
            Thread.sleep(1000);

            // receive messages
            
            logger.info("Receiving messages from " + qName + ":\n");
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);

            List<Message> messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            
            assertTrue("Expected 5 messages, received " + messages.size(), messages.size() == 5);

            // change message visibility batch to 10 sec for all messages

            ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
            changeMessageVisibilityBatchRequest.setQueueUrl(myQueueUrl);
            
            List<ChangeMessageVisibilityBatchRequestEntry> visibilityEntryList = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>();
            
            ChangeMessageVisibilityBatchRequestEntry entry = new ChangeMessageVisibilityBatchRequestEntry("1", messages.get(0).getReceiptHandle());
            entry.setVisibilityTimeout(60);
            visibilityEntryList.add(entry);

            entry = new ChangeMessageVisibilityBatchRequestEntry("2", messages.get(1).getReceiptHandle());
            entry.setVisibilityTimeout(120);
            visibilityEntryList.add(entry);

            changeMessageVisibilityBatchRequest.setEntries(visibilityEntryList);
            sqs.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
            
            // check if messages invisible
            
            messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            assertTrue("Expected 0 messages, received " + messages.size(), messages.size() == 0);
            
            Thread.sleep(41000);

            // check if messages revisible
            
            messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            
            if (messages.size() == 0) {
            	Thread.sleep(1000);
                messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            }
            
            assertTrue("Expected 3 messages, received " + messages.size(), messages.size() == 3);
            
            Thread.sleep(61000);

            // check if messages revisible
            
            messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            int attempt = 0;
            
            while (messages.size() < 4 && attempt < 10) {
            	Thread.sleep(1000);
                messages.addAll(sqs.receiveMessage(receiveMessageRequest).getMessages());
                attempt++;
            }
            
            assertTrue("Expected 4 messages, received " + messages.size(), messages.size() == 4);

            Thread.sleep(121000);

            // check if messages revisible
            
            messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            
            attempt = 0;

            while (messages.size() < 5 && attempt < 10) {
            	Thread.sleep(1000);
                messages.addAll(sqs.receiveMessage(receiveMessageRequest).getMessages());
                attempt++;
            }
            
            assertTrue("Expected 5 messages, received " + messages.size(), messages.size() == 5);
            
            List<String> messageBodies = new ArrayList<String>();
            
            for (Message m : messages) {
            	messageBodies.add(m.getBody());
            }
            
            assertTrue("Missing message bodies", messageBodies.contains("msg 0") && messageBodies.contains("msg 1") && messageBodies.contains("msg 2") && messageBodies.contains("msg 3") && messageBodies.contains("msg 4"));

            // delete messages
            
            int i = 0;
            
            List<DeleteMessageBatchRequestEntry> deleteEntryList = new ArrayList<DeleteMessageBatchRequestEntry>();
            
            for (Message message : messages) {
            	
                logger.info("  Message");
                logger.info("    MessageId:     " + message.getMessageId());
                logger.info("    ReceiptHandle: " + message.getReceiptHandle());
                logger.info("    MD5OfBody:     " + message.getMD5OfBody());
                logger.info("    Body:          " + message.getBody());
                
                for (Entry<String, String> e : message.getAttributes().entrySet()) {
                    logger.info("  Attribute");
                    logger.info("    Name:  " + e.getKey());
                    logger.info("    Value: " + e.getValue());
                }
                
                i++;
                
                deleteEntryList.add(new DeleteMessageBatchRequestEntry(i+"", message.getReceiptHandle()));
            }
            
            DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(myQueueUrl);
            deleteMessageBatchRequest.setEntries(deleteEntryList);
            DeleteMessageBatchResult deleteBatchResult = sqs.deleteMessageBatch(deleteMessageBatchRequest);
            
        	DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
            deleteQueueRequest.setQueueUrl(myQueueUrl);
            sqs.deleteQueue(deleteQueueRequest);
            
            Thread.sleep(1000);
            
            // check if messages deleted
            
            messages = new ArrayList<Message>();
            //receiveMessageRequest.setVisibilityTimeout(60);
            receiveMessageRequest.setMaxNumberOfMessages(5);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            assertTrue("Expected 0 messages, received " + messages.size(), messages.size() == 0);
            
            randomQueueUrls.remove(myQueueUrl);
            
        } catch (AmazonServiceException ase) {
            displayServiceException(ase);
        } catch (AmazonClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not " + "being able to access the network.", ace);
            fail("Error Message: " + ace.getMessage());
        } catch (InterruptedException e) {
            logger.error("exception", e);
            fail("exception="+e);
        }  catch (Exception e) {
            logger.error("exception", e);
            fail("exception="+e);
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
