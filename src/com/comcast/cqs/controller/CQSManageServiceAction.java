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

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSPopulator;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cqs.io.CQSPopulator;
import com.comcast.cqs.util.CQSErrorCodes;

/**
 * @author aseem, bwolf
 */
public class CQSManageServiceAction extends CQSAction {

	private static Logger logger = Logger.getLogger(CQSPurgeQueueAction.class);
	
    public static final String CQS_API_SERVERS = "CQSAPIServers";
    public CQSManageServiceAction() {
        super("ManageService");
    }
    
    @Override
    public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    	return true;
    }

    @Override
    public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
    	
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
    	
		String task = request.getParameter("Task");

		if (task == null || task.equals("")) {
			logger.error("event=cqs_manage_service error_code=missing_parameter_task");
			throw new CMBException(CQSErrorCodes.MissingParameter,"Request parameter Task missing.");
		}
		
		String host = request.getParameter("Host");
		
		if (task.equals("ClearCache")) {

			PersistenceFactory.getCQSMessagePersistence().flushAll();
	    	String out = CQSPopulator.getResponseMetadata();
            writeResponse(out, response);
	        return true;
        
		} else if (task.equals("ClearAPIStats")) {

            CMBControllerServlet.initStats();
            String out = CQSPopulator.getResponseMetadata();
            writeResponse(out, response);
	    	return true;
			
		} else if (task.equals("RemoveRecord")) {
			
			DurablePersistenceFactory.getInstance().delete(AbstractDurablePersistence.CQS_KEYSPACE, CQS_API_SERVERS, host, null, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
			String out = CNSPopulator.getResponseMetadata();
            writeResponse(out, response);
			return true;
			
		} else {
			logger.error("event=cqs_manage_service error_code=invalid_task_parameter valid_values=ClearCache,ClearAPIStats,RemoveRecord");
			throw new CMBException(CNSErrorCodes.InvalidParameterValue,"Request parameter Task is missing or invalid. Valid values are ClearCache, ClearAPIStats, RemoveRecord.");
		}
    }
}
