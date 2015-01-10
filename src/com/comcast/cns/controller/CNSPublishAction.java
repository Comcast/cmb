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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
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
import com.comcast.cns.tools.CQSHandler;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;
import com.comcast.cqs.controller.CQSHttpServletRequest;
import com.comcast.cqs.model.CQSMessageAttribute;
import com.comcast.cqs.util.CQSConstants;

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
		
        CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
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
    	
        Map<String, CQSMessageAttribute> messageAttributes = new HashMap<String, CQSMessageAttribute>();
        
        int index = 1;
        String messageAttributeName = request.getParameter(CQSConstants.MESSAGE_ATTRIBUTES+".entry." + index + ".Name");
        
        while (messageAttributeName != null && !messageAttributeName.equals("")) {
            String messageAttributeValue = request.getParameter(CQSConstants.MESSAGE_ATTRIBUTES+".entry." + index + ".Value.StringValue");
            if (messageAttributeValue == null || messageAttributeValue.equals("")) {
            	messageAttributeValue = request.getParameter(CQSConstants.MESSAGE_ATTRIBUTES+".entry." + index + ".Value.BinaryValue");
            }
            if (messageAttributeValue == null || messageAttributeValue.equals("")) {
            	throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Missing message attribute value " + messageAttributeName);
            }
            String messageAttributeDataType = request.getParameter(CQSConstants.MESSAGE_ATTRIBUTES+".entry." + index + ".Value.DataType");
            if (messageAttributeDataType == null || messageAttributeDataType.equals("")) {
            	throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Missing message attribute data type " + messageAttributeName);
            }
            messageAttributes.put(messageAttributeName, new CQSMessageAttribute(messageAttributeValue, messageAttributeDataType));
            index++;
            messageAttributeName = request.getParameter(CQSConstants.MESSAGE_ATTRIBUTES+".entry." + index + ".Name");
        }

        if (messageAttributes.size() > 0) {
        	cnsMessage.setMessageAttributes(messageAttributes);
        }
		
    	cnsMessage.checkIsValid();
    	
    	CNSTopicAttributes topicAttributes = CNSCache.getTopicAttributes(topicArn);
    	List<String> receiptHandles = new ArrayList<String>();
    	
    	boolean success = true;
    	
    	if (topicAttributes != null && topicAttributes.getSubscriptionsConfirmed() == 0) {
    		
    		// optimization: don't do anything if there are no confirmed subscribers
    		
    		logger.warn("event=no_confirmed_subscribers action=publish topic_arn=" + topicArn);
    	
    	} else if (CMBProperties.getInstance().isCNSBypassPublishJobQueueForSmallTopics() && topicAttributes != null && topicAttributes.getSubscriptionsConfirmed() <= CMBProperties.getInstance().getCNSMaxSubscriptionsPerEndpointPublishJob()) {
    		
    		// optimization: if there's only one chunk due to few subscribers, write directly into endpoint publish queue bypassing the publish job queue
    		
    		logger.debug("event=using_job_queue_overpass");
    		
    		List<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo> subscriptions = CNSEndpointPublisherJobProducer.getSubscriptionsForTopic(topicArn);
    		
            if (subscriptions != null && subscriptions.size() > 0) {
            	
                List<CNSEndpointPublishJob> epPublishJobs = CNSEndpointPublisherJobProducer.createEndpointPublishJobs(cnsMessage, subscriptions);
                
                if (epPublishJobs.size() != 1) {
                	logger.warn("event=unexpected_number_of_endpoint_publish_jobs count=" + epPublishJobs.size());
                }
                
                for (CNSEndpointPublishJob epPublishJob: epPublishJobs) {
            		String handle = sendMessageOnRandomShardAndCreateQueueIfAbsent(CNS_ENDPOINT_QUEUE_NAME_PREFIX, CMBProperties.getInstance().getCNSNumEndpointPublishJobQueues(), epPublishJob.serialize(), cnsInternalUser.getUserId(), messageAttributes);
            		if (handle != null && !handle.equals("")) {
            			receiptHandles.add(handle);
            		} else {
            			success = false;
            		}
                }
            }
    		
    	} else {
    	
	    	// otherwise pick publish job queue

    		logger.debug("event=going_through_job_queue_town_center");
    		
    		String handle = sendMessageOnRandomShardAndCreateQueueIfAbsent(CNS_PUBLISH_QUEUE_NAME_PREFIX, CMBProperties.getInstance().getCNSNumPublishJobQueues(), cnsMessage.serialize(), cnsInternalUser.getUserId(), messageAttributes);
    		if (handle != null && !handle.equals("")) {
    			receiptHandles.add(handle);
    		} else {
    			success = false;
    		}
    	}  
    	
    	if (!success) {
    		throw new CMBException(CMBErrorCodes.InternalError, "Failed to place message on internal cns queue");
    	}
    	
    	request.setReceiptHandles(receiptHandles);
    	
    	String out = CNSSubscriptionPopulator.getPublishResponse(receiptHandles);
        writeResponse(out, response);
    	
    	return true;
	}
	
	private String sendMessageOnRandomShardAndCreateQueueIfAbsent(String prefix, int numShards, String message, String userId, Map<String, CQSMessageAttribute> messageAttributes) {
		
		String receiptHandle = null;
		
    	long ts1 = System.currentTimeMillis();

    	String queueName =  prefix + (new Random()).nextInt(numShards);
    	String relativeQueueUrl = com.comcast.cqs.util.Util.getRelativeQueueUrlForName(queueName, userId);
    	
    	try {
    		receiptHandle = CQSHandler.sendMessage(relativeQueueUrl, message, messageAttributes);
        } catch (Exception ex) {
	    	logger.error("event=publish_failure", ex);
    		CQSHandler.ensureQueuesExist(prefix, numShards);
        }
    	
    	long ts2 = System.currentTimeMillis();

    	CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);
    	
    	return receiptHandle;
	}
}
