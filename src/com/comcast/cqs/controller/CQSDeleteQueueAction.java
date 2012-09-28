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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cqs.io.CQSQueuePopulator;
import com.comcast.cqs.model.CQSQueue;

/**
 * Delete queue
 * @author baosen, vvenkatraman, bwolf
 *
 */
public class CQSDeleteQueueAction extends CQSAction {
	
	public CQSDeleteQueueAction() {
		super("DeleteQueue");
	}		

	@Override
	public boolean doAction(User user, HttpServletRequest request,	HttpServletResponse response) throws Exception {
	    CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
        PersistenceFactory.getQueuePersistence().deleteQueue(queue.getRelativeUrl());
        PersistenceFactory.getCQSMessagePersistence().clearQueue(queue.getRelativeUrl());
        
        String out = CQSQueuePopulator.getDeleteQueueResponse();

        response.getWriter().print(out);
        
        return true;
	}
}
