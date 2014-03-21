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

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSAttributePopulator;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.util.CNSErrorCodes;

/**
 * Set Topic Attributes
 * @author bwolf, jorge
 *
 */
public class CNSGetTopicAttributesAction extends CNSAction {

	//private static Logger logger = Logger.getLogger(CNSGetTopicAttributesAction.class);
	
	public CNSGetTopicAttributesAction() {
		super("GetTopicAttributes");
	}

	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
    	String topicArn = request.getParameter("TopicArn");
    	
    	if (topicArn == null) {
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, "Missing parameters TopicArn");
    	}
    	
    	//CNSTopicAttributes attr = PersistenceFactory.getCNSAttributePersistence().getTopicAttributes(topicArn);
    	CNSTopicAttributes attr = CNSCache.getTopicAttributes(topicArn);
    	
    	if (attr == null) {
    		throw new CMBException(CNSErrorCodes.InternalError, "Unknown topic with arn " + topicArn);
    	}
    	
    	String out = CNSAttributePopulator.getGetTopicAttributesResponse(attr);
    	
        writeResponse(out, response);

        return true;
    }
}
