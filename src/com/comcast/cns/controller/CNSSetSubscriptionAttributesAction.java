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
import org.json.JSONObject;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSAttributePopulator;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.cns.util.CNSErrorCodes;

/**
 * Set subscription attributes
 * @author bwolf, jorge
 *
 */
public class CNSSetSubscriptionAttributesAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSListSubscriptionsByTopicAction.class);

	public CNSSetSubscriptionAttributesAction() {
		super("SetSubscriptionAttributes");
	}
	
    /**
     * The method simply gets the information from the user and request to set the subscription attributes based 
     * on what the user passed as the "AttributeName", and set that attribute to "AttributeValue"
     * 
     * @param user the user for whom we are setting the subscription attributes
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
	   	String userId = user.getUserId();
    	String attributeName = request.getParameter("AttributeName");
    	String attributeValue = request.getParameter("AttributeValue");
    	String subscriptionArn = request.getParameter("SubscriptionArn");
    	
    	logger.debug("event=cns_set_subscription_attributes attribute_name=" + attributeName + " attribute_value=" + attributeValue + " subscription_arn=" + subscriptionArn + " user_id=" + userId);
			
    	if ((userId == null) || (subscriptionArn == null) || (attributeName == null) || (attributeValue == null)) {
    		logger.error("event=cns_set_subscription_attributes error_code=InvalidParameters attribute_name=" + attributeName + " attribute_value=" + attributeValue + " subscription_arn=" + subscriptionArn + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"missing parameters");
    	}	
    	
    	CNSSubscriptionAttributes subscriptionAttributes = new CNSSubscriptionAttributes();
    	
    	if (attributeName.equals("DeliveryPolicy")) {  		
    		JSONObject json = new JSONObject(attributeValue);   		
    		CNSSubscriptionDeliveryPolicy deliveryPolicy = new CNSSubscriptionDeliveryPolicy(json);
    		subscriptionAttributes.setDeliveryPolicy(deliveryPolicy);
    	} else if (attributeName.equals("RawMessageDelivery")){
    		subscriptionAttributes.setRawMessageDelivery(Boolean.parseBoolean(attributeValue));
    	}
    	else {
    		logger.error("event=cns_set_subscription_attributes error_code=InvalidParameters attribute_name=" + attributeName + " attribute_value=" + attributeValue + " subscription_arn=" + subscriptionArn + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"AttributeName: " + attributeName + " is not a valid value");
    	}
    	
    	PersistenceFactory.getCNSAttributePersistence().setSubscriptionAttributes(subscriptionAttributes, subscriptionArn);
    	
    	String res = CNSAttributePopulator.getSetSubscriptionAttributesResponse();
    	logger.debug("event=cns_set_subscription_attributes attribute_name=" + attributeName + " attribute_value=" + attributeValue + " subscription_arn=" + subscriptionArn + " user_id=" + userId);
    	response.getWriter().println(res);
    	return true;
    }
}
