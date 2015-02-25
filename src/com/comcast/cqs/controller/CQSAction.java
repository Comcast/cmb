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

import com.comcast.cmb.common.controller.Action;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.Util;

/**
 * Class represents all CQS actions
 * @author vvenkatraman, bwolf
 *
 */
public abstract class CQSAction extends Action {
    
    public CQSAction(String actionName) {
        super(actionName);
    }

    public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
        
        String absoluteQueueUrl = request.getRequestURL().toString();
        
        if (request.getRequestURI() == null || request.getRequestURI().equals("") || request.getRequestURI().equals("/")) {
        	absoluteQueueUrl = request.getParameter(CQSConstants.QUEUE_URL);
        }
        
        String queueUserId = Util.getUserIdForAbsoluteQueueUrl(absoluteQueueUrl);
        
        if (user.getUserId().equals(queueUserId)) {
            return true;
        }
        
        if (policy == null) {
            return false;
        }
        
        return policy.isAllowed(user, service + ":" + this.actionName);
    }
}
