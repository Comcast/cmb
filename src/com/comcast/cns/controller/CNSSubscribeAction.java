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
import java.util.UUID;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSSubscriptionPopulator;
import com.comcast.cns.io.CommunicationUtils;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSMessage.CNSMessageType;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;
/**
 * Subscribe action
 * @author bwolf, jorge
 *
 */
public class CNSSubscribeAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSSubscribeAction.class);
	
	public CNSSubscribeAction() {
		super("Subscribe");
	}

    /**
     * The method simply gets the information from the user and request to call subscribe on the persistence layer, then we take
     * response and generate an XML response and put it in the parameter response
     * @param user the user for whom we are subscribing.
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

		HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

    	String userId = user.getUserId();
    	String endpoint = request.getParameter("Endpoint");
    	String protocol = request.getParameter("Protocol");
    	String topicArn = request.getParameter("TopicArn");
    	
    	if ((endpoint == null) || (protocol == null) || (userId == null) || (topicArn == null)) {
    		logger.error("event=cns_subscribe error_code=InvalidParameters user_id="+userId+ " topic_arn=" + topicArn +" endpoint=" + endpoint + " protocol=" + protocol);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
		}
    	
    	if (!Util.isValidTopicArn(topicArn)) {
    		logger.error("event=cns_subscribe error_code=InvalidParameters user_id="+userId+ " topic_arn=" + topicArn +" endpoint=" + endpoint + " protocol=" + protocol);
    		throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}
    	
    	CNSSubscription.CnsSubscriptionProtocol subProtocol = null;
    	
    	if (protocol.equals("email-json")) {
    		subProtocol = CNSSubscription.CnsSubscriptionProtocol.email_json;
    	} else if(protocol.equals("email") || protocol.equals("sqs") || protocol.equals("cqs") || protocol.equals("http") || protocol.equals("https")) {
    		subProtocol = CNSSubscription.CnsSubscriptionProtocol.valueOf(protocol);
    	} else {
    		logger.error("event=cns_subscribe error_code=InvalidParameters user_id="+userId+ " topic_arn=" + topicArn +" endpoint=" + endpoint + " protocol=" + protocol);
    		throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}

    	logger.debug("event=cns_subscribe endpoint=" + endpoint + " protocol=" + protocol + " user_id=" + userId + " topic_arn=" + topicArn);
		
    	boolean inputError = false;
    	inputError = !subProtocol.isValidEnpoint(endpoint);
    	
    	if (inputError) {
    		logger.error("event=cns_subscribe error_code=InvalidParameters user_id="+userId+ " topic_arn=" + topicArn +" endpoint=" + endpoint + " protocol=" + protocol);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}
    	
    	CNSSubscription sub = PersistenceFactory.getSubscriptionPersistence().subscribe(endpoint, subProtocol, topicArn, userId);
    	String subscriptionArn = null;
    	
    	if (sub.isConfirmed()) {
    		subscriptionArn = sub.getArn();
    	} else {
    		subscriptionArn = "pending confirmation";
    		String messageId = UUID.randomUUID().toString();
    		String json =  Util.generateConfirmationJson(topicArn, sub.getToken(), messageId);
    		String ownerUserId = PersistenceFactory.getTopicPersistence().getTopic(topicArn).getUserId();
    		User topicOwner = PersistenceFactory.getUserPersistence().getUserById(ownerUserId);   		
    		
    		try {
    			CNSMessage message = new CNSMessage();
    			message.setMessage(json);
    			message.setSubscriptionArn(sub.getArn());
    			message.setTopicArn(topicArn);
    			message.setUserId(topicOwner.getUserId());
    			message.setMessageType(CNSMessageType.SubscriptionConfirmation);
    			message.setTimestamp(new Date());
    			CommunicationUtils.sendMessage(topicOwner, subProtocol, endpoint, message, messageId, topicArn, sub.getArn(), false);
    		} catch (Exception ex) {
    			PersistenceFactory.getSubscriptionPersistence().unsubscribe(sub.getArn());
    			if (ex instanceof CMBException) {
    				throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, "Invalid parameter: " + ex.getMessage());
    			} else {
    				throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, "Invalid parameter: Unreachable endpoint " + endpoint);
    			}
    		}
    		
    		logger.info("event=cns_subscribe_confirmation_request_sent endpoint=" + endpoint + " protocol=" + protocol + " user_id=" + userId + " topic_arn=" + topicArn + " token=" + sub.getToken());
    	}
    	
    	String res = CNSSubscriptionPopulator.getSubscribeResponse(subscriptionArn);			
		response.getWriter().println(res);
		return true;
	}
}
