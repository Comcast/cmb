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

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.io.CNSPopulator;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cqs.io.CQSPopulator;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
import com.comcast.cqs.util.CQSErrorCodes;

/**
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

	    	RedisCachedCassandraPersistence.flushAll();
	    	response.getWriter().println(CQSPopulator.getResponseMetadata());
	        return true;
        
		} else if (task.equals("ClearAPIStats")) {

            CMBControllerServlet.initStats();
	    	response.getWriter().println(CQSPopulator.getResponseMetadata());
	    	return true;
			
		} else if (task.equals("RemoveRecord")) {
			
			CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCQSKeyspace());
			ColumnFamilyTemplate<String, String> usersTemplate = new ThriftColumnFamilyTemplate<String, String>(cassandraHandler.getKeySpace(HConsistencyLevel.QUORUM), "CQSAPIServers", StringSerializer.get(), StringSerializer.get());
			cassandraHandler.delete(usersTemplate, host, null);
			
	    	response.getWriter().println(CNSPopulator.getResponseMetadata());
			
			return true;
			
		} else {
			logger.error("event=cqs_manage_service error_code=invalid_task_parameter valid_values=ClearCache,ClearAPIStats,RemoveRecord");
			throw new CMBException(CNSErrorCodes.InvalidParameterValue,"Request parameter Task is missing or invalid. Valid values are ClearQueues, ClearAPIStats, RemoveRecord.");
		}
    }
}
