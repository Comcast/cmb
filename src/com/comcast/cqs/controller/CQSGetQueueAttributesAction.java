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

import java.util.List;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cqs.io.CQSQueuePopulator;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.Util;

/**
 * Set queue attributes
 * @author aseem, baosen, bwolf
 *
 */
public class CQSGetQueueAttributesAction extends CQSAction {
	
	public CQSGetQueueAttributesAction() {
		super("GetQueueAttributes");
	}		

	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
	    
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

		CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
        String ownerId = request.getParameter("QueueOwnerAWSAccountId");

        if (ownerId == null) {
            ownerId = user.getUserId();
        }
        
        if (!ownerId.equals(user.getUserId())) {

        	CMBPolicy policy = new CMBPolicy(queue.getPolicy());
	        
        	if (!policy.isAllowed(user, "CQS:" + this.actionName)) {
	            throw new CMBException(CMBErrorCodes.AccessDenied, "You don't have permission for " + this.actionName);
	        }
        }
        
        List<String> attributesList = Util.fillGetAttributesRequests(request);
        
        for (String attribute : attributesList) {
        
        	if (!attribute.equals("All") && !attribute.equals(CQSConstants.VISIBILITY_TIMEOUT) && !attribute.equals(CQSConstants.POLICY) && !attribute.equals(CQSConstants.QUEUE_ARN)  
                && !attribute.equals(CQSConstants.MAXIMUM_MESSAGE_SIZE) && !attribute.equals(CQSConstants.MESSAGE_RETENTION_PERIOD) && !attribute.equals(CQSConstants.DELAY_SECONDS) 
                && !attribute.equals(CQSConstants.APPROXIMATE_NUMBER_OF_MESSAGES)) {
                throw new CMBException(CMBErrorCodes.InvalidAttributeName, "Unknown attribute " + attribute);
            }
        }

        String out = CQSQueuePopulator.getQueueAttributesResponse(queue, attributesList);
        response.getWriter().print(out);
        
        return true;
	}
}