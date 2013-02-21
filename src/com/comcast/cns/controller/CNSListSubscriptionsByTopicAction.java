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

import java.util.List;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSSubscriptionPopulator;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.persistence.SubscriberNotFoundException;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;

/**
 * List subscriptions by topic
 * @author bwolf
 *
 */
public class CNSListSubscriptionsByTopicAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSListSubscriptionsByTopicAction.class);

	public CNSListSubscriptionsByTopicAction() {
		super("ListSubscriptionsByTopic");
	}

	/**
	 * The method simply gets the information from the user and request to call listSubscriptions, then we take
	 * response and generate an XML response and put it in the parameter response
	 * @param user the user for whom we are listing the subscription
	 * @param request the servlet request including all the parameters for the listSubscriptions call
	 * @param response the response servlet we write to.
	 */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
		String userId = user.getUserId();
		String nextToken = null;
		String topicArn = request.getParameter("TopicArn");

		if (request.getParameter("NextToken") != null) {
			nextToken = request.getParameter("NextToken");
			logger.debug("event=cns_list_topics_by_subscription next_token=" + nextToken + " topicArn=" + topicArn + " userId=" + userId);
		} else {
			logger.debug("event=cns_list_topics_by_subscription topicArn=" + topicArn + " userId=" + userId);
		}

		if ((topicArn == null) || (userId == null)) {
			logger.error("event=cns_list_topics_by_subscription status=failure errorType=InvalidParameters  topicArn=" + topicArn + " userId=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
		}

		if (!Util.isValidTopicArn(topicArn)) {
			logger.error("event=cns_list_topics_by_subscription status=failure errorType=InvalidParameters  topicArn=" + topicArn + " userId=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
		}

		List<CNSSubscription> subscriptions = null;
		
		try {
			subscriptions = PersistenceFactory.getSubscriptionPersistence().listSubscriptionsByTopic(nextToken, topicArn, null);
		} catch (SubscriberNotFoundException ex) {
			throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Invalid parameter nextToken");
		}
		
		logger.debug("num_subscriptions=" + subscriptions.size());

		if (subscriptions.size() >= 100) {
			//Check if there are more
			nextToken = subscriptions.get(99).getArn();
			List<CNSSubscription> nextSubscriptions = PersistenceFactory.getSubscriptionPersistence().listSubscriptionsByTopic(nextToken, topicArn, null);

			if (nextSubscriptions.size() ==0) {
				nextToken = null;
			} else {
				logger.debug("event=cns_subscriptionsbytopic_listed event=next_token_created next_token=" + nextToken + " userid=" + userId);
			}
		}

		String res = CNSSubscriptionPopulator.getListSubscriptionByTopicResponse(subscriptions, nextToken);
		response.getWriter().println(res);	
		return true;
	}
}
