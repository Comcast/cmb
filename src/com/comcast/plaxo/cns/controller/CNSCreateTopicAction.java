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

import com.comcast.plaxo.cmb.common.model.CMBPolicy;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cns.io.CNSTopicPopulator;
import com.comcast.plaxo.cns.model.CNSTopic;
import com.comcast.plaxo.cns.util.CNSErrorCodes;

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
     * @param request the  request including all the parameters, variable "Name" is expected to create a name for the topic
     * @param response the response servlet we write the response to.
     */
	@Override
	public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
    	
		String displayName = request.getParameter("Name");
		String name = request.getParameter("Name");
	
		String userId = user.getUserId();
		logger.debug("event=cns_topic_create name=" + name + " display_name=" + displayName + " userid=" + userId);

		if (name == null || userId == null) {
			logger.error("event=cns_topic_create status=failure errorType=InvalidParameters name=" + name + " userid=" + userId);
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
