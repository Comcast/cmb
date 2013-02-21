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
package com.comcast.cqs.controller;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cqs.io.CQSQueuePopulator;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSErrorCodes;

/**
 * Get queue url
 * @author bwolf, vvenkatraman, baosen
 *
 */
public class CQSGetQueueUrlAction extends CQSAction {
	
	public CQSGetQueueUrlAction() {
		super("GetQueueUrl");
	}		

	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
		String qName = request.getParameter("QueueName");

        if (qName == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "QueueName not found");
        }

        String ownerId = request.getParameter("QueueOwnerAWSAccountId");

        if (ownerId == null) {
            ownerId = user.getUserId();
        }

        CQSQueue queue = PersistenceFactory.getQueuePersistence().getQueue(ownerId, qName);

        if (queue == null) {
            throw new CMBException(CQSErrorCodes.NonExistentQueue, "Queue not found with name=" + qName + " ownerId=" + ownerId);
        }
        
        if (!ownerId.equals(user.getUserId())) {
	        
        	CMBPolicy policy = new CMBPolicy(queue.getPolicy());
	        
        	if (!policy.isAllowed(user, "CQS:" + this.actionName)) {
	            throw new CMBException(CMBErrorCodes.AccessDenied, "You don't have permission for " + this.actionName);
	        }
        }

        String out = CQSQueuePopulator.getQueueUrlResponse(queue);

        response.getWriter().print(out);
        
        return true;
	}
	
	@Override
	public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) {
		return true;
	}

}
