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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cqs.persistence.RedisPayloadCacheCassandraPersistence;
import com.comcast.cqs.util.CQSErrorCodes;

/**
 * C
 * @author aseem, bwolf
 */
public class CQSManageServiceAction extends CQSAction {

	private static Logger logger = Logger.getLogger(CQSClearQueueAction.class);

	public CQSManageServiceAction() {
        super("ManageService");
    }
    
    @Override
    public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    	return true;
    }

    @Override
    public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
    	
		String task = request.getParameter("Task");

		if (task == null || task.equals("")) {
			logger.error("event=cqs_manage_service status=failure errorType=InvalidParameters details=missing_parameter_task");
			throw new CMBException(CQSErrorCodes.MissingParameter,"Request parameter Task missing.");
		}
		
		if (task.equals("ClearCache")) {

	    	RedisPayloadCacheCassandraPersistence.flushAll();
	        return true;
        
		} else if (task.equals("ClearAPIStats")) {

            CMBControllerServlet.callStats = new ConcurrentHashMap<String, AtomicLong>();
            CMBControllerServlet.callFailureStats = new ConcurrentHashMap<String, AtomicLong>();

            return true;
			
		} else {
			logger.error("event=cqs_manage_service status=failure errorType=InvalidParameterValue parameter=Task valid_values=ClearCache,ClearAPIStats");
			throw new CMBException(CNSErrorCodes.InvalidParameterValue,"Request parameter Task missing is invalid. Valid values are ClearQueues and RemoveRecord.");
		}
    }
}
