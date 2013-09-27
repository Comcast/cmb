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
import java.util.List;
import java.util.Random;

import javax.servlet.AsyncContext;
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
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cns.io.CNSSubscriptionPopulator;
import com.comcast.cns.model.CNSEndpointPublishJob;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.model.CNSMessage.CNSMessageType;
import com.comcast.cns.tools.CNSEndpointPublisherJobProducer;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;

/**
 * Publish action
 * @author bwolf, jorge, aseem
 *
 */
public class CNSPublishAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSPublishAction.class);
	
    private static volatile User cnsInternalUser = null;
    private static volatile BasicAWSCredentials awsCredentials = null;
    private static volatile AmazonSQSClient sqs = null;
    
    public static final String CNS_PUBLISH_QUEUE_NAME_PREFIX = CMBProperties.getInstance().getCNSPublishQueueNamePrefix();
    public static final String CNS_ENDPOINT_QUEUE_NAME_PREFIX = CMBProperties.getInstance().getCNSEndpointPublishQueueNamePrefix();
    
    public CNSPublishAction() {
		super("Publish");
	}
	
    /**
     * The method simply gets the information from the user and request to call publish the message passed in
     * the "Message" field in the request parameters, and publish them to all endpoints subscribed to the topic
     * designated by the "TopicArn" field in the request, then take the response and generate an XML response 
     * and put it in the parameter response
     * @param user the user for whom we are listing the subscription
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
		if (cnsInternalUser == null) {

			IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
	        cnsInternalUser = userHandler.getUserByName(CMBProperties.getInstance().getCNSUserName());

	        if (cnsInternalUser == null) {	          
	        	cnsInternalUser =  userHandler.createDefaultUser();
	        }
		}
		
		if (awsCredentials == null) {
	        awsCredentials = new BasicAWSCredentials(cnsInternalUser.getAccessKey(), cnsInternalUser.getAccessSecret());
		}
		
		if (sqs == null) {
            sqs = new AmazonSQSClient(awsCredentials);
            sqs.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());
		}
		
    	String userId = user.getUserId();
    	String message = request.getParameter("Message");
    	String topicArn = request.getParameter("TopicArn");

    	String messageStructure = null;
    	String subject = null;
    	
    	CNSMessage cnsMessage = new CNSMessage();
    	cnsMessage.generateMessageId();
    	cnsMessage.setTimestamp(new Date());
    	cnsMessage.setMessage(message);
    	
    	if (request.getParameter("MessageStructure") != null) {
    		
    		messageStructure = request.getParameter("MessageStructure");   	
    		
    		if (!messageStructure.equals("json")) {
    			logger.error("event=cns_publish error_code=InvalidParameters message=" + message + " message_structure=" + messageStructure + " topic_arn=" + topicArn + " user_id=" + userId);
    			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"Invalid parameter: Invalid Message Structure parameter: " + messageStructure);
    		}
    		
    		cnsMessage.setMessageStructure(CNSMessage.CNSMessageStructure.valueOf(messageStructure));
		} 
    	
    	if (request.getParameter("Subject") != null) {
    		subject = request.getParameter("Subject");
    		cnsMessage.setSubject(subject);
		} 
    	
    	if ((userId == null) || (message == null)) {
    		logger.error("event=cns_publish error_code=InvalidParameters message=" + message + " topic_arn=" + topicArn + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_ValidationError,"1 validation error detected: Value null at 'message' failed to satisfy constraint: Member must not be null");
    	}
    	
		if ((topicArn == null) || !Util.isValidTopicArn(topicArn)) {
			logger.error("event=cns_publish error_code=InvalidParameters message=" + message + " topic_arn=" + topicArn + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"TopicArn");
    	}
		
		CNSTopic topic = CNSCache.getTopic(topicArn); 
		
    	if (topic == null) {
    		logger.error("event=cns_publish error_code=NotFound message=" + message + " topic_arn=" + topicArn + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_NotFound,"Resource not found.");
    	}
		
    	cnsMessage.setUserId(topic.getUserId());
    	cnsMessage.setTopicArn(topicArn);
    	cnsMessage.setMessageType(CNSMessageType.Notification);
    	
    	cnsMessage.checkIsValid();
    	cnsMessage.processMessageToProtocols();
    	
    	CNSTopicAttributes topicAttributes = CNSCache.getTopicAttributes(topicArn);
    	
    	if (CMBProperties.getInstance().isCNSBypassPublishJobQueueForSmallTopics() && topicAttributes != null && topicAttributes.getSubscriptionsConfirmed() > 0 && topicAttributes.getSubscriptionsConfirmed() <= CMBProperties.getInstance().getCNSMaxSubscriptionsPerEndpointPublishJob()) {
    		
    		// optimization: if we there's only one chunk due to few subscribers write directly into endpoint publish queue bypassing the publish job queue
    		
    		logger.debug("event=using_job_queue_overpass");
    		
    		List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> subscriptions = CNSEndpointPublisherJobProducer.getSubscriptionsForTopic(topicArn);
    		
            if (subscriptions != null && subscriptions.size() > 0) {
            	
                List<CNSEndpointPublishJob> epPublishJobs = CNSEndpointPublisherJobProducer.createEndpointPublishJobs(cnsMessage, subscriptions);
                
                if (epPublishJobs.size() != 1) {
                	logger.warn("event=unexpected_number_of_endpoint_publish_jobs count=" + epPublishJobs.size());
                }
                
                for (CNSEndpointPublishJob epPublishJob: epPublishJobs) {
                	
                	String queueName =  CNS_ENDPOINT_QUEUE_NAME_PREFIX + ((new Random()).nextInt(CMBProperties.getInstance().getCNSNumEndpointPublishJobQueues()));
        	    	String queueUrl = ensureQueueExists(queueName);

        	    	long ts1 = System.currentTimeMillis();
        	    	SendMessageResult sendMessageResult = sqs.sendMessage(new SendMessageRequest(queueUrl, epPublishJob.serialize()));
        	    	long ts2 = System.currentTimeMillis();

        	    	CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);

        	    	logger.debug("event=cns_publish subject= " + subject + " topic_arn=" + topicArn + " user_id=" + userId + " message_id=" + sendMessageResult.getMessageId() + " queue_url=" + queueUrl);
                }
            }
    		
    	} else {
    	
	    	// otherwise pick publish job queue
    		
    		logger.debug("event=going_through_job_queue_town_center");
	    	
	    	String queueName =  CNS_PUBLISH_QUEUE_NAME_PREFIX + (new Random()).nextInt(CMBProperties.getInstance().getCNSNumPublishJobQueues());
	    	String queueUrl = ensureQueueExists(queueName);
	
	    	long ts1 = System.currentTimeMillis();
	    	SendMessageResult sendMessageResult = sqs.sendMessage(new SendMessageRequest(queueUrl, cnsMessage.serialize()));
	    	long ts2 = System.currentTimeMillis();

	    	CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);

	    	logger.debug("event=cns_publish subject= " + subject + " topic_arn=" + topicArn + " user_id=" + userId + " message_id=" + sendMessageResult.getMessageId() + " queue_url=" + queueUrl);
    	}    	
    	
    	String res = CNSSubscriptionPopulator.getPublishResponse(cnsMessage);
    	response.getWriter().println(res);
    	
    	return true;
	}
	
	private String ensureQueueExists(String queueName) throws Exception {
		
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
    	
    	return queueUrl;
	}
}
