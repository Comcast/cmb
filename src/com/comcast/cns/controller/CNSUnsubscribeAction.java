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
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;

/**
 * Unsubscribe action
 * @author bwolf
 *
 */
public class CNSUnsubscribeAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSUnsubscribeAction.class);
	
	public CNSUnsubscribeAction() {
		super("Unsubscribe");
	}

    /**
     * The method simply gets the information from the user and request to call doUnsubscribe on the persistence layer, then we take
     * response and generate an XML response and put it in the parameter response
     * @param user the user for whom we are unsubscribing.
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

		String arn = request.getParameter("SubscriptionArn");
    	String userId = user.getUserId();
    	
    	if ((arn == null) || (userId == null)) {
    		logger.error("event=cns_unsubscribe status=failure errorType=InvalidParameters arn=" + arn + " userId=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
		}
    	
    	if (!Util.isValidSubscriptionArn(arn)) {
    		logger.error("event=cns_unsubscribe status=failure errorType=InvalidParameters arn=" + arn + " userId=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
    	}
    	
    	logger.debug("event=cns_unsubscribe subscriptionArn=" + arn + " userId=" + userId);
    	PersistenceFactory.getSubscriptionPersistence().unsubscribe(arn);
        String res = CNSSubscriptionPopulator.getUnsubscribeResponse();
		response.getWriter().println(res);
		return true;
	}
}
