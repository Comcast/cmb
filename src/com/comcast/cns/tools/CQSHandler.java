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
 */package com.comcast.cns.tools;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
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
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;

public class CQSHandler {
	
    private static Logger logger = Logger.getLogger(CQSHandler.class);
    
    private static volatile boolean initialized = false;
    private static volatile BasicAWSCredentials awsCredentials = null;
    private static volatile AmazonSQS sqs = null;
    private static ConcurrentHashMap<String, String> queueUrlCache = new ConcurrentHashMap<String, String>();
    
    public static synchronized void initialize() throws PersistenceException {
    	
    	if (initialized) {
    		return;
    	}
    	
		if (CMBProperties.getInstance().getCNSUserAccessKey() != null && CMBProperties.getInstance().getCNSUserAccessKey() != null) {
			
		    awsCredentials = new BasicAWSCredentials(CMBProperties.getInstance().getCNSUserAccessKey(), CMBProperties.getInstance().getCNSUserAccessSecret());

		} else {
    	
			IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
			User cnsInternalUser = userHandler.getUserByName(CMBProperties.getInstance().getCNSUserName());
		
		    if (cnsInternalUser == null) {	          
		    	cnsInternalUser =  userHandler.createDefaultUser();
		    }

		    awsCredentials = new BasicAWSCredentials(cnsInternalUser.getAccessKey(), cnsInternalUser.getAccessSecret());
		}
 		
        sqs = new AmazonSQSClient(awsCredentials);
        sqs.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());

        initialized = true;
    }
    
    public static void shutdown() {
    	queueUrlCache.clear();
    	initialized = false;
    }
    
    public static AmazonSQS getSQSHandler() {
    	return sqs;
    }
    
    public static void setSQSHandler(AmazonSQS sqs) {
    	CQSHandler.sqs = sqs;
    }
    
    public static void changeMessageVisibility(String queueUrl, String receiptHandle, int delaySec) {
    	
        long ts1 = System.currentTimeMillis();
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, delaySec);
        sqs.changeMessageVisibility(changeMessageVisibilityRequest);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
        logger.debug("event=change_message_visibility receipt_handle=" + receiptHandle + " delay=" + delaySec);
    }

    public static void deleteMessage(String queueUrl, Message message) {
    	
        long ts1 = System.currentTimeMillis();
        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, message.getReceiptHandle());
        sqs.deleteMessage(deleteMessageRequest);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
        logger.debug("event=delete_message receipt_handle=" + message.getReceiptHandle());
    }
    
    public static Message receiveMessage(String queueUrl, int waitTimeSeconds) {
    	
    	Message message = null;
        long ts1 = System.currentTimeMillis();
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest.setMaxNumberOfMessages(1);
        receiveMessageRequest.setVisibilityTimeout(CMBProperties.getInstance().getCNSPublishJobVisibilityTimeout());
        
        if (waitTimeSeconds > 0) {
        	receiveMessageRequest.setWaitTimeSeconds(waitTimeSeconds);
        }
        
        ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
        List<Message> msgs = receiveMessageResult.getMessages();
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
        
        if (msgs.size() > 0) {
    		message = msgs.get(0);
            logger.debug("event=received_message receipt_handle=" + message.getReceiptHandle());
        } 

        return message; 
    }
    
    public static String getQueueUrl(String queueName) {

    	if (queueUrlCache.containsKey(queueName)) {
    		return queueUrlCache.get(queueName);
    	}
    	
        String queueUrl = null;
    	long ts1 = System.currentTimeMillis();
    	GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
        GetQueueUrlResult getQueueUrlResult = sqs.getQueueUrl(getQueueUrlRequest);
        queueUrl = getQueueUrlResult.getQueueUrl();
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
		logger.info("event=get_queue_url queue_name=" + queueName + " queue_url=" + queueUrl);
        
        queueUrlCache.put(queueName, queueUrl);
        
        return queueUrl;
    }
    
    public static void sendMessage(String queueUrl, String message) {

    	long ts1 = System.currentTimeMillis();
        SendMessageResult sendMessageResult = sqs.sendMessage(new SendMessageRequest(queueUrl, message));
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
		logger.debug("event=send_message message_id=" + sendMessageResult.getMessageId());
    }
    
    public static void deleteMessage(String queueUrl, String receiptHandle) {

    	long ts1 = System.currentTimeMillis();
        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, receiptHandle);
        sqs.deleteMessage(deleteMessageRequest);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
		logger.debug("event=delete_message receipt_handle=" + receiptHandle);
    }
    
    public static synchronized void ensureQueuesExist(String queueNamePrefix, int numQueues) {

    	for (int i = 0; i < numQueues; i++) {
        	
            GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueNamePrefix + i);
            
            try {
                sqs.getQueueUrl(getQueueUrlRequest);
            } catch (AmazonServiceException ex) {
            	
                if (ex.getStatusCode() == 400) {
                	
                    CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueNamePrefix + i);
                    CreateQueueResult createQueueResponse = sqs.createQueue(createQueueRequest);
                    
                    if (createQueueResponse.getQueueUrl() == null) {
                        throw new IllegalStateException("Could not create queue with name " + queueNamePrefix + i);
                    }
                    
                    logger.info("event=created_missing_queue name=" + queueNamePrefix + i + " url=" + createQueueResponse.getQueueUrl());

                } else {
                    throw ex;
                }
            }
        }
    }
}
