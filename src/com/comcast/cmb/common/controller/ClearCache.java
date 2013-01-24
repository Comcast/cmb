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
package com.comcast.cmb.common.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cqs.controller.CQSAction;
import com.comcast.cqs.persistence.RedisPayloadCacheCassandraPersistence;

/**
 * C
 * @author aseem
 */
public class ClearCache extends CQSAction {

    public ClearCache() {
        super("ClearCache");
    }
    
    @Override
    public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    	return true;
    }

    @Override
    public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {       
    	RedisPayloadCacheCassandraPersistence.flushAll();
        response.setStatus(HttpServletResponse.SC_OK);
        response.getOutputStream().print("<ClearCacheResponse>OK</ClearCacheResponse>");
        response.flushBuffer();
        return true;
    }
}
