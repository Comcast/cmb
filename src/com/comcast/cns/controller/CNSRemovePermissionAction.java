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

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.io.CNSAttributePopulator;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
import com.comcast.cqs.util.Util;

/**
 * Remove permission
 * @author bwolf
 *
 */
public class CNSRemovePermissionAction extends CNSAction {
	
	private static Logger logger = Logger.getLogger(CNSAddPermissionAction.class);
	
	public CNSRemovePermissionAction() {
		super("RemovePermission");
	}

	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

		String topicArn = request.getParameter("TopicArn");
    	
    	if ((topicArn == null) ) {
    		logger.error("action=cns_add_permission status=failure event=missing_parameter_topic_arn");
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"Missing parameter TopicArn");
    	}
    	
    	CNSTopic topic = PersistenceFactory.getTopicPersistence().getTopic(topicArn);
    	
    	if (topic == null) {
    		logger.error("action=cns_add_permission status=failure event=invalid_parameter_topic_arn");
			throw new CMBException(CNSErrorCodes.CNS_NotFound,"Resource not found.");
    	}
    	
        String label = request.getParameter(CQSConstants.LABEL);

        if (!Util.isValidId(label)) {
            throw new CMBException(CQSErrorCodes.InvalidBatchEntryId, "Label " + label + " is invalid. Only alphanumeric, hyphen, and underscore are allowed. It can be at most " + CMBProperties.getInstance().getMaxMessageSuppliedIdLength() + " letters long.");
        }
        
        CMBPolicy policy = null;

		CNSTopicAttributes attributes = CNSCache.getTopicAttributes(topicArn);

        // validate policy string
        
        if (attributes.getPolicy() != null) {

        	policy = new CMBPolicy(attributes.getPolicy());
            
        	if (policy.removeStatement(label)) {
            	attributes.setPolicy(policy.toString());
        		PersistenceFactory.getCNSAttributePersistence().setTopicAttributes(attributes, topicArn);
            }
        }
        
        response.getWriter().print(CNSAttributePopulator.getRemovePermissionResponse());	
        return true;
	}
}
