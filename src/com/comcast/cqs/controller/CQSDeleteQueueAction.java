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
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

	    CQSQueue queue = CQSCache.getCachedQueue(user, request);
	    
	    int numberOfShards = queue.getNumberOfShards();
	    
        PersistenceFactory.getQueuePersistence().deleteQueue(queue.getRelativeUrl());
        
        // clear all shards in redis
        
        for (int shard=0; shard<numberOfShards; shard++) {
            PersistenceFactory.getCQSMessagePersistence().clearQueue(queue.getRelativeUrl(), shard);
        }
        
        String out = CQSQueuePopulator.getDeleteQueueResponse();
        response.getWriter().print(out);
        
        return true;
	}
}
