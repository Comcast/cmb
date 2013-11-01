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
import com.comcast.cns.io.CNSTopicPopulator;
import com.comcast.cns.util.CNSErrorCodes;

/**
 * Delete topic
 * @author bwolf
 *
 */
public class CNSDeleteTopicAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSDeleteTopicAction.class);

	public CNSDeleteTopicAction() {
		super("DeleteTopic");
	}
	
    /**
     * Delete the topic with the topicArn in the request variable "TopicArn".  Send the response out as XML to response. 
     * @param user The user who owns the topic we are deleting
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
    	String arn = request.getParameter("TopicArn");
    	String userId = user.getUserId();
    	logger.debug("event=cns_topic_delete arn=" + arn + " userid=" + userId);
    	
		if ((arn == null) || (userId == null)) {
			logger.error("event=cns_topic_delete errro_code=InvalidParameters topic_arn=" + arn + " user_id=" + userId);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"request parameter does not comply with the associated constraints.");
		}
		
		PersistenceFactory.getTopicPersistence().deleteTopic(arn);			
		String out = CNSTopicPopulator.getDeleteTopicResponse();
        writeResponse(out, response);
		return true;
	}
}
