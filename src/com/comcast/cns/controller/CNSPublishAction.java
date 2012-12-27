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
package com.comcast.cns.controller;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ExpiringCache;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cns.io.CNSSubscriptionPopulator;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;

/**
 * Publish action
 * @author bwolf, jorge, aseem
 *
 */
public class CNSPublishAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSPublishAction.class);
	
    private static ExpiringCache<String, CNSTopic> topicCache = new ExpiringCache<String, CNSTopic>(CMBProperties.getInstance().getCnsCacheSizeLimit());
    private static volatile User cnsInternalUser = null;
    private static volatile BasicAWSCredentials awsCredentials = null;
    private static volatile AmazonSQS sqs = null;
    
    public static final String CNS_PUBLISH_QUEUE_NAME_PREFIX = CMBProperties.getInstance().getCnsPublishQueueNamePrefix();
    
    private class CNSTopicCallable implements Callable<CNSTopic> {
    	
        String arn = null;
        
        public CNSTopicCallable(String arn) {
            this.arn = arn;
        }
        
        @Override
        public CNSTopic call() throws Exception {
            CNSTopic t = PersistenceFactory.getTopicPersistence().getTopic(arn);
            return t;
        }
    }

    public CNSPublishAction() {
		super("Publish");
	}
	
    /**
     * The method simply gets the information from the user and request to call publish the message passed in
     * the "Message" field in the request parameters, and publish them to all endpoints subscribed to the topic
     * designated by the "TopicArn" field in the request, then take the response and generate an XML response 
     * and put it in the parameter response
     * @param user the user for whom we are listing the subscription
     * @param request the servlet request including all the parameters for the listSubscriptions call
     * @param response the response servlet we write to.
     */
	@Override
	public boolean doAction(User user, HttpServletRequest request,	HttpServletResponse response) throws Exception {
		
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
		
    	String userId = user.getUserId();
    	String message = request.getParameter("Message");
    	String topicArn = request.getParameter("TopicArn");

    	logger.debug("event=cns_publish message=" + message + " topicArn=" + topicArn + " userId=" + userId );
    	
    	String messageStructure = null;
    	String subject = null;
    	String log = "";
    	
    	CNSMessage cnsMessage = new CNSMessage();
    	cnsMessage.generateMessageId();
    	cnsMessage.setTimestamp(new Date());
    	cnsMessage.setMessage(message);
    	
    	if (request.getParameter("MessageStructure") != null) {
    		
    		messageStructure = request.getParameter("MessageStructure");   	
    		
    		if (!messageStructure.equals("json")) {
    			logger.error("event=cns_publish status=failure errorType=InvalidParameters message=" + message + " messageStructure=" + messageStructure + " topicArn=" + topicArn + " userId=" + userId);
    			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"Invalid parameter: Invalid Message Structure parameter: " + messageStructure);
    		}
    		
    		log += " message_structure=" + messageStructure;		
    		cnsMessage.setMessageStructure(CNSMessage.CNSMessageStructure.valueOf(messageStructure));
		} 
    	
    	if (request.getParameter("Subject") != null) {
    		subject = request.getParameter("Subject");
    		log += " subject=" + messageStructure;
    		cnsMessage.setSubject(subject);
		} 
    	
    	if ((userId == null) || (message == null)) {
    		logger.error("event=cns_publish_ng status=failure errorType=InvalidParameters message=" + message + " topicArn=" + topicArn + " userId=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_ValidationError,"1 validation error detected: Value null at 'message' failed to satisfy constraint: Member must not be null");
    	}
    	
		if ((topicArn == null) || !Util.isValidTopicArn(topicArn)) {
			logger.error("event=cns_publish_ng status=failure errorType=InvalidParameters message=" + message + " topicArn=" + topicArn + " userId=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"TopicArn");
    	}
		
		CNSTopic topic = topicCache.getAndSetIfNotPresent(topicArn, new CNSTopicCallable(topicArn), CMBProperties.getInstance().getCnsCacheExpiring() * 1000); 
		
    	if (topic == null) {
    		logger.error("event=cns_publish_ng status=failure errorType=NotFound message=" + message + " topicArn=" + topicArn + " userId=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_NotFound,"Resource not found.");
    	}
		
    	cnsMessage.setUserId(topic.getUserId());
    	cnsMessage.setTopicArn(topicArn);
    	logger.debug("event=cns_publish_ng message=" + message + " topicArn=" + topicArn + " userId=" + userId  + log);
    	
    	cnsMessage.checkIsValid();
    	
    	// pick random queue, create if not exists (in expiring cache implementation)
    	
    	String queueName =  CNS_PUBLISH_QUEUE_NAME_PREFIX + (new Random()).nextInt(CMBProperties.getInstance().getNumPublishJobQs());

    	GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
    	String queueUrl = null;

    	try {

    	    GetQueueUrlResult getQueueUrlResult = sqs.getQueueUrl(getQueueUrlRequest);
    	    queueUrl = getQueueUrlResult.getQueueUrl();

    	} catch (AmazonServiceException ex) {

    	    if (ex.getStatusCode() == 400) {

    	        logger.info("event=cns_publish action=create_non_existent_queue queue_name=" + queueName);

    	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
    	        CreateQueueResult createQueueResult = sqs.createQueue(createQueueRequest);
    	        queueUrl = createQueueResult.getQueueUrl();

    	        if (queueUrl == null) {
    	            throw new IllegalStateException("Could not create queue with name " + queueName);
    	        }

    	    } else {
    	        throw ex;
    	    }
    	}

    	cnsMessage.processMessageToProtocols();
    	long ts1 = System.currentTimeMillis();
    	SendMessageResult sendMessageResult = sqs.sendMessage(new SendMessageRequest(queueUrl, cnsMessage.serialize()));
    	long ts2 = System.currentTimeMillis();
    	CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
    	logger.debug("event=cns_publish status=success message=" + message + " topicArn=" + topicArn + " userId=" + userId + " messageId=" + sendMessageResult.getMessageId() + log);
    	String res = CNSSubscriptionPopulator.getPublishResponse(cnsMessage);
    	response.getWriter().println(res);
    	return true;
	}
}
