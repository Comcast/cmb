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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.conn.HttpHostConnectException;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cqs.api.CQSAPI;
import com.comcast.cqs.controller.CQSCache;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.Util;

public class CQSHandler {
	
    private static Logger logger = Logger.getLogger(CQSHandler.class);
    
    private static volatile boolean initialized = false;
    private static volatile BasicAWSCredentials awsCredentials = null;
    private static volatile AmazonSQSClient sqs = null;
    private static User cnsInternal = null;
    private static boolean useInlineApiCalls = false;
    
    static {
    	try {
			initialize();
		} catch (PersistenceException ex) {
			logger.error("event=initialization_failure", ex);
		}
    }
    
    private static synchronized void initialize() throws PersistenceException {
    	
    	if (initialized) {
    		return;
    	}
    	
		IUserPersistence userHandler = PersistenceFactory.getUserPersistence();

		if (CMBProperties.getInstance().getCNSUserAccessKey() != null && CMBProperties.getInstance().getCNSUserAccessSecret() != null) {
			
			//TODO: should add user id to cmb properties file
			
		    awsCredentials = new BasicAWSCredentials(CMBProperties.getInstance().getCNSUserAccessKey(), CMBProperties.getInstance().getCNSUserAccessSecret());
			cnsInternal = userHandler.getUserByAccessKey(awsCredentials.getAWSAccessKeyId());

		} else {
		
			cnsInternal = userHandler.getUserByName(CMBProperties.getInstance().getCNSUserName());

			if (cnsInternal == null) {	          
		    	cnsInternal =  userHandler.createDefaultUser();
		    }

		    awsCredentials = new BasicAWSCredentials(cnsInternal.getAccessKey(), cnsInternal.getAccessSecret());
		}
		
        sqs = new AmazonSQSClient(awsCredentials);
        sqs.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());
        
        useInlineApiCalls = CMBProperties.getInstance().useInlineApiCalls() && CMBProperties.getInstance().getCQSServiceEnabled();

        initialized = true;
    }
    
    public static void changeMessageVisibility(String relativeQueueUrl, String receiptHandle, int visibilityTimeout) throws Exception {
    	
        long ts1 = System.currentTimeMillis();
        
        if (useInlineApiCalls) {
        	CQSAPI.changeMessageVisibility(cnsInternal.getUserId(), relativeQueueUrl, receiptHandle, visibilityTimeout);
        } else {
        	String absoluteQueueUrl = Util.getAbsoluteQueueUrlForRelativeUrl(relativeQueueUrl);
	        sqs.changeMessageVisibility(new ChangeMessageVisibilityRequest(absoluteQueueUrl, receiptHandle, visibilityTimeout));
        }
        
        long ts2 = System.currentTimeMillis();
        
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
        logger.debug("event=change_message_visibility receipt_handle=" + receiptHandle + " vto=" + visibilityTimeout);
    }

    public static List<CQSMessage> receiveMessage(String relativeQueueUrl, int waitTimeSeconds, int maxNumberOfMessages) throws Exception {
    	
    	//TODO: inline calling for long polled receive not supported yet
    	
    	List<CQSMessage> messages = new ArrayList<CQSMessage>();
        long ts1 = System.currentTimeMillis();
        
        if (useInlineApiCalls && waitTimeSeconds == 0) {
        
        	messages = CQSAPI.receiveMessages(cnsInternal.getUserId(), relativeQueueUrl, maxNumberOfMessages, null);
        	
        } else {
        
        	String absoluteQueueUrl = Util.getAbsoluteQueueUrlForRelativeUrl(relativeQueueUrl);
        	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(absoluteQueueUrl);
	        receiveMessageRequest.setMaxNumberOfMessages(maxNumberOfMessages);
	        //receiveMessageRequest.setVisibilityTimeout(CMBProperties.getInstance().getCNSPublishJobVisibilityTimeout());
	        
	        if (waitTimeSeconds > 0) {
	        	receiveMessageRequest.setWaitTimeSeconds(waitTimeSeconds);
	        }
	        
	        ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
	        List<Message> msgs = receiveMessageResult.getMessages();
	        
	        for (Message m : msgs) {
	        	messages.add(new CQSMessage(m));
	        }
        }

        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);

        return messages;
    }
    
    public static String getQueueUrl(String queueName) throws Exception {
    	
        String queueUrl = null;

        long ts1 = System.currentTimeMillis();
    	
        CQSQueue queue = CQSCache.getCachedQueue(Util.getRelativeQueueUrlForName(queueName, cnsInternal.getUserId()));
        
        if (queue != null) {
        	queueUrl = queue.getAbsoluteUrl();
        }

        long ts2 = System.currentTimeMillis();
        
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
        
        return queueUrl;
    }
    
    public static String sendMessage(String relativeQueueUrl, String message) throws Exception {

    	long ts1 = System.currentTimeMillis();
    	String receiptHandle = null;
    	
        if (useInlineApiCalls) {
        	receiptHandle = CQSAPI.sendMessage(cnsInternal.getUserId(), relativeQueueUrl, message, null);
        } else {
        	String absoluteQueueUrl = Util.getAbsoluteQueueUrlForRelativeUrl(relativeQueueUrl);
        	SendMessageResult sendMessageResult = sqs.sendMessage(new SendMessageRequest(absoluteQueueUrl, message));
        	receiptHandle = sendMessageResult.getMessageId();
        }
        
        long ts2 = System.currentTimeMillis();
        
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
		logger.debug("event=send_message message_id=" + receiptHandle);
		
		return receiptHandle;
    }
    
    public static void deleteMessage(String relativeQueueUrl, String receiptHandle) throws Exception {

    	long ts1 = System.currentTimeMillis();
    	
        if (useInlineApiCalls) {
        	CQSAPI.deleteMessage(cnsInternal.getUserId(), relativeQueueUrl, receiptHandle);
        } else {
        	String absoluteQueueUrl = Util.getAbsoluteQueueUrlForRelativeUrl(relativeQueueUrl);
        	sqs.deleteMessage(new DeleteMessageRequest(absoluteQueueUrl, receiptHandle));
        }

        long ts2 = System.currentTimeMillis();
        
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
		logger.debug("event=delete_message receipt_handle=" + receiptHandle);
    }
    
    public static synchronized void ensureQueuesExist(String queueNamePrefix, int numShards) {

    	for (int i = 0; i < numShards; i++) {
        	
            GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueNamePrefix + i);
            
            try {
                sqs.getQueueUrl(getQueueUrlRequest);
            } catch (AmazonServiceException ex) {
            	
                if (ex.getStatusCode() == 400) {
                	
                    CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueNamePrefix + i);
                    Map<String, String> attributes = new HashMap<String, String>();
                    
                    if (queueNamePrefix.startsWith(CMBProperties.getInstance().getCNSEndpointPublishQueueNamePrefix())) {
                    	attributes.put("VisibilityTimeout", CMBProperties.getInstance().getCNSEndpointPublishJobVisibilityTimeout()+"");
                    } else {
                        attributes.put("VisibilityTimeout", CMBProperties.getInstance().getCNSPublishJobVisibilityTimeout()+"");
                    }

                    createQueueRequest.setAttributes(attributes);
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
    
    public static boolean doesQueueNotExist(Exception ex) {

    	boolean doesNotExist = false;
    	
    	if (ex instanceof AmazonServiceException && ((AmazonServiceException)ex).getStatusCode() == 400) {
    		doesNotExist = true;
    	} else if (ex instanceof AmazonServiceException && ((AmazonServiceException)ex).getErrorCode().equals("NonExistentQueue")) {
    		doesNotExist = true;
    	} else if (ex.getCause() instanceof HttpHostConnectException) {
    		doesNotExist = false;
    	} else if (ex instanceof CMBException) {
    		doesNotExist = false;
    	}
    	
    	return doesNotExist;
    }
    
    public static String getRelativeCnsInternalQueueUrl(String queueName) {
    	String localQueueUrl = Util.getRelativeQueueUrlForName(queueName, cnsInternal.getUserId());
        return localQueueUrl;
    }
}
