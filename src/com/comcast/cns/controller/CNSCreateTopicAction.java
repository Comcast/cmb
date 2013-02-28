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
import com.comcast.cns.io.CNSTopicPopulator;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.util.CNSErrorCodes;

/**
 * Create Topic
 * @author bwolf, jorge
 */
public class CNSCreateTopicAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSCreateTopicAction.class);

	public CNSCreateTopicAction() {
		super("CreateTopic");
	}

	/*
     * doCreateTopic creates the topic by calling the CNS Topic Persistence layer to create the queue, and if no Exceptions occur
     * then output the new topics data as XML to the HttpServletResponse response
     * @param user The user that requests this topic
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
    	
		String displayName = request.getParameter("Name");
		String name = request.getParameter("Name");
	
		String userId = user.getUserId();
		logger.debug("event=cns_topic_create name=" + name + " display_name=" + displayName + " user_id=" + userId);

		if (name == null || userId == null) {
			logger.error("event=cns_topic_create error_code=InvalidParameters name=" + name + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
		}
		
		CNSTopic newTopic;
		newTopic = PersistenceFactory.getTopicPersistence().createTopic(name, displayName, userId);
		String topicArn = newTopic.getArn();
		String res = CNSTopicPopulator.getCreateTopicResponse(topicArn);
		response.getWriter().print(res);
		return true;
	}
	
	@Override
	public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
		return true;
	}
}
