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

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSSubscriptionPopulator;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;

/**
 * Confirm subscription action
 * @author bwolf, aseem
 *
 */
public class CNSConfirmSubscriptionAction extends CNSAction {
	
	private static Logger logger = Logger.getLogger(CNSConfirmSubscriptionAction.class);
	
	public CNSConfirmSubscriptionAction() {
		super("ConfirmSubscription");
	}
	
	/**
     * The method simply gets the information from the user and request to call confirmSubscription on the persistence layer, then we take
     * response and generate an XML response and put it in the parameter response
     * @param user the user for whom we are confirming the subscription for.
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

    	String authOnUnsubscribeStr = request.getParameter("AuthenticateOnUnsubscribe");
    	String token = request.getParameter("Token");
    	String topicArn = request.getParameter("TopicArn");
    	
    	if ((topicArn == null) || (token == null)) {
    		logger.error("event=cns_confirmsubscription error_code=InvalidParameters token=" + token + " topic_arn=" + topicArn + " authenticate_on_unsubscribe=" + authOnUnsubscribeStr);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}
    	
    	if (!Util.isValidTopicArn(topicArn)) {
    		logger.error("event=cns_confirmsubscription error_code=InvalidParameters token=" + token + " topic_arn=" + topicArn + " authenticate_on_unsubscribe=" + authOnUnsubscribeStr);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}
    	
    	boolean authenticateOnUnsubscribe = false;
		
    	if (authOnUnsubscribeStr != null) {
			if (!(authOnUnsubscribeStr.equals("true") || (authOnUnsubscribeStr.equals("false")))) {
				logger.error("event=cns_confirmsubscription error_code=InvalidParameters token=" + token + " topic_arn=" + topicArn + " authenticate_on_unsubscribe=" + authOnUnsubscribeStr);
				throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
			} else {	
				authenticateOnUnsubscribe = (authOnUnsubscribeStr.equals("true"));
			} 
		}
    	
    	logger.debug("event=cns_confirm_subscription token=" + token + " topicArn=" + topicArn);
    	CNSSubscription sub = PersistenceFactory.getSubscriptionPersistence().confirmSubscription(authenticateOnUnsubscribe, token, topicArn);
    	String out = CNSSubscriptionPopulator.getConfirmSubscriptionResponse(sub);
        writeResponse(out, response);
		return true;
	}
	
    @Override
    public boolean isAuthRequired() {
        // is implicitly authenticated by token parameter
        return false;
    }
}
