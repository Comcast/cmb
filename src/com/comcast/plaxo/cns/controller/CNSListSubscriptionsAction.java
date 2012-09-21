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
package com.comcast.plaxo.cns.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.plaxo.cmb.common.model.CMBPolicy;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cns.io.CNSSubscriptionPopulator;
import com.comcast.plaxo.cns.model.CNSSubscription;
import com.comcast.plaxo.cns.persistence.SubscriberNotFoundException;

/**
 * List subscriptions
 * @author bwolf
 *
 */
public class CNSListSubscriptionsAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSListSubscriptionsAction.class);
	
	public CNSListSubscriptionsAction() {
		super("ListSubscriptions");
	}

    /**
     * The method simply gets the information from the user and request to call listSubscriptions, then we take
     * response and generate an XML response and put it in the parameter response
     * @param user the user for whom we are listing the subscription
     * @param request the servlet request including all the parameters for the listSubscriptions call
     * @param response the response servlet we write to.
     */
	@Override
	public boolean doAction(User user, HttpServletRequest request,	HttpServletResponse response) throws Exception {
		
    	String userId = user.getUserId();
    	String nextToken = null;
    	
		if (request.getParameter("NextToken") != null) {
			nextToken = request.getParameter("NextToken");
			logger.debug("event=cns_subscription_list next_token=" + nextToken + " userid=" + userId);
		} else {
			logger.debug("event=cns_subscription_list userid=" + userId);
		}
		
		List<CNSSubscription> subscriptions = null;
		
		try {
			subscriptions = PersistenceFactory.getSubscriptionPersistence().listSubscriptions(nextToken, null, userId);
		} catch (SubscriberNotFoundException ex) {
			throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Invalid parameter nextToken");
		}
		
		if (subscriptions.size() >= 100) {
			
			//Check if there are more
			nextToken = subscriptions.get(99).getArn();
			List<CNSSubscription> nextSubscriptions = PersistenceFactory.getSubscriptionPersistence().listSubscriptions(nextToken, null, userId);
			
			if (nextSubscriptions.size() == 0) {
				nextToken = null;
			} else {
				logger.debug("event=cns_subscriptions_listed event=next_token_created next_token=" + nextToken + " userid=" + userId);
			}
		}
		
		String res = CNSSubscriptionPopulator.getListSubscriptionResponse(subscriptions,nextToken);
		response.getWriter().println(res);
		return true;
	}
	
	@Override
	public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
		return true;
	}
}
