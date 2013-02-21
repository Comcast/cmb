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

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSQueue;
/**
 * Clear queue
 * @author vvenkatraman, bwolf
 *
 */
public class CQSClearQueueAction extends CQSAction {
	
	public CQSClearQueueAction() {
		super("ClearQueue");
	}

	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

		HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

		CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
		PersistenceFactory.getCQSMessagePersistence().clearQueue(queue.getRelativeUrl());
		String out = CQSMessagePopulator.getClearQueueResponse();
		response.getWriter().println(out);
		
		return true;
	}
}
