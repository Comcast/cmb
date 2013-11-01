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

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSAttributePopulator;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.model.CNSTopicDeliveryPolicy;
import com.comcast.cns.util.CNSErrorCodes;

/**
 * Set topic attributes
 * @author bwolf, jorge
 *
 */
public class CNSSetTopicAttributesAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSSetTopicAttributesAction.class);
	
	public CNSSetTopicAttributesAction() {
		super("SetTopicAttributes");
	}
	
	private boolean validateDisplayName(String displayName) {
		
		if (displayName.length() > 100) {
			return false;
		}
		
		int len = displayName.length();

		for (int i=0; i< len; i++) {
			
			char c = displayName.charAt(i);
			
			if (c < 32 || c > 126) {				
				return false;
			}
		}
		
		return true;
	}

	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
    	
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
		String userId = user.getUserId();
    	String attributeName = request.getParameter("AttributeName");
    	String attributeValue = request.getParameter("AttributeValue");
    	String topicArn = request.getParameter("TopicArn");
    	
    	if ((userId == null) || (topicArn == null) || (attributeName == null) || (attributeValue == null)) {
    		logger.error("event=cns_set_topic_attributes error_code=InvalidParameters attribute_name=" + attributeName + " attribute_value=" + attributeValue + " topic_arn=" + topicArn + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"missing parameters");
    	}    
    	
    	CNSTopicAttributes topicAttributes = new CNSTopicAttributes();

    	if (attributeName.equals("DeliveryPolicy")) {  		
    		
    		JSONObject json = new JSONObject(attributeValue);   		
    		CNSTopicDeliveryPolicy deliveryPolicy = new CNSTopicDeliveryPolicy(json);
    		topicAttributes.setDeliveryPolicy(deliveryPolicy);
    		logger.debug("event=cns_set_topic_delivery_policy topic_arn=" + topicArn + " value=" + topicAttributes.getEffectiveDeliveryPolicy());
    	
    	} else if (attributeName.equals("Policy")) {  		
    		
			// validate policy before updating
			
			try {
				new CMBPolicy(attributeValue);
			} catch (Exception ex) {
                logger.warn("event=invalid_policy topic_arn=" + topicArn + " policy=" + attributeValue, ex);
    			throw ex;
    		}

			topicAttributes.setPolicy(attributeValue);
    		logger.debug("event=cns_set_topic_policy topic_arn=" + topicArn + "  value=" + topicAttributes.getPolicy());
    	
    	} else if (attributeName.equals("DisplayName")) {  	
    		
    		if (validateDisplayName(attributeValue)) {
	    		topicAttributes.setDisplayName(attributeValue);
	    		logger.debug("event=cns_set_topic_display_name topic_arn=" + topicArn + "  value=" + attributeValue);
    		} else {
    			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"Bad Display Name");
    		}
    		
    	} else {
    		logger.error("event=cns_set_topic_attributes error_code=InvalidParameters attribute_name=" + attributeName + " attribute_value=" + attributeValue + " topic_arn=" + topicArn + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"AttributeName: " + attributeName + " is not a valid value");
    	}
    	
    	PersistenceFactory.getCNSAttributePersistence().setTopicAttributes(topicAttributes, topicArn);
    	String out = CNSAttributePopulator.getSetTopicAttributesResponse();
    	logger.debug("event=cns_set_topic_attributes attribute_name=" + attributeName + " attribute_value=" + attributeValue + " topic_arn=" + topicArn + " user_id=" + userId);
        writeResponse(out, response);
    	return true;
    }
}
