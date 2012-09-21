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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cns.io.CNSAttributePopulator;
import com.comcast.plaxo.cns.model.CNSSubscriptionAttributes;
import com.comcast.plaxo.cns.util.CNSErrorCodes;

/**
 * Get Subsctiption Attributes
 * @author bwolf, jorge
 *
 */
public class CNSGetSubscriptionAttributesAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSGetSubscriptionAttributesAction.class);
	
	public CNSGetSubscriptionAttributesAction() {
		super("GetSubscriptionAttributes");
	}

	@Override
	public boolean doAction(User user, HttpServletRequest request,	HttpServletResponse response) throws Exception {
    	
		String userId = user.getUserId();
    	String subscriptionArn = request.getParameter("SubscriptionArn");
    	logger.debug("event=cns_get_subscription_attributes  subscriptionArn=" + subscriptionArn + " userId=" + userId);
    	
    	if ((userId == null) || (subscriptionArn == null) ) {
    		logger.error("event=cns_get_subscription_attributes status=failure errorType=InvalidParameters subscriptionArn=" + subscriptionArn + " userId=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"missing parameters");
    	}	
    	
    	CNSSubscriptionAttributes attr = PersistenceFactory.getCNSAttributePersistence().getSubscriptionAttributes(subscriptionArn);

    	String res = CNSAttributePopulator.getGetSubscriptionAttributesResponse(attr);
    	logger.info("event=cns_get_subscription_attributes status=success subscriptionArn=" + subscriptionArn + " userId=" + userId);
    	response.getWriter().println(res);
    	return true;
	}
}
