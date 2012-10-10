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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSSubscriptionPopulator;
import com.comcast.cns.io.CommunicationUtils;
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
     * @param request the servlet request including all the parameters for the doUnsubscribe call
     * @param response the response servlet we write to.
     */
	@Override
	public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
		
    	String userId = user.getUserId();
    	String endPoint = request.getParameter("Endpoint");
    	String protocol = request.getParameter("Protocol");
    	String topicArn = request.getParameter("TopicArn");
    	
    	if ((endPoint == null) || (protocol == null) || (userId == null) || (topicArn == null)) {
    		logger.error("event=cns_subscribe status=failure errorType=InvalidParameters userId="+userId+ " topicArn=" + topicArn +" endpoint=" + endPoint + " protocol=" + protocol);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
		}
    	
    	if (!Util.isValidTopicArn(topicArn)) {
    		logger.error("event=cns_subscribe status=failure errorType=InvalidParametersBadARN userId="+userId+ " topicArn=" + topicArn +" endpoint=" + endPoint + " protocol=" + protocol);
    		throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}
    	
    	CNSSubscription.CnsSubscriptionProtocol subProtocol = null;
    	
    	if (protocol.equals("email-json")) {
    		subProtocol = CNSSubscription.CnsSubscriptionProtocol.email_json;
    	} else if(protocol.equals("email") || protocol.equals("sqs") || protocol.equals("cqs") || protocol.equals("http") || protocol.equals("https")) {
    		subProtocol = CNSSubscription.CnsSubscriptionProtocol.valueOf(protocol);
    	} else {
    		logger.error("event=cns_subscribe status=failure errorType=InvalidParametersBadProtocol userId="+userId+ " topicArn=" + topicArn +" endpoint=" + endPoint + " protocol=" + protocol);
    		throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}

    	logger.info("event=cns_subscribe endpoint=" + endPoint + " protocol=" + protocol + " userId=" + userId + " topicArn=" + topicArn);
		
    	boolean inputError = false;
    	inputError = !subProtocol.isValidEnpoint(endPoint);
    	
    	if (inputError) {
    		logger.error("event=cns_subscribe status=failure errorType=InvalidParameters userId="+userId+ " topicArn=" + topicArn +" endpoint=" + endPoint + " protocol=" + protocol);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}
    	
    	CNSSubscription sub = PersistenceFactory.getSubscriptionPersistence().subscribe(endPoint, subProtocol, topicArn, userId);
    	String subscriptionArn = null;
    	
    	if (sub.isConfirmed()) {
    		subscriptionArn = sub.getArn();
    	} else {
    		subscriptionArn = "pending confirmation";
    		String json =  Util.generateConfirmationJson(topicArn, sub.getToken());
    		String ownerUserId = PersistenceFactory.getTopicPersistence().getTopic(topicArn).getUserId();
    		User topicOwner = PersistenceFactory.getUserPersistence().getUserById(ownerUserId);   		
    		
    		try {
    			CommunicationUtils.sendMessage(topicOwner, subProtocol, endPoint, json);
    		} catch (Exception ex) {
    			PersistenceFactory.getSubscriptionPersistence().unsubscribe(sub.getArn());
    			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, "Invalid parameter: Unreachable endpoint");
    		}
    		
    		logger.info("event=cns_subscribe_confirmation_request_sent endpoint=" + endPoint + " protocol=" + protocol + " userId=" + userId + " topicArn=" + topicArn + " token=" + sub.getToken());
    	}
    	
    	String res = CNSSubscriptionPopulator.getSubscribeResponse(subscriptionArn);			
		response.getWriter().println(res);
		return true;
	}
}
