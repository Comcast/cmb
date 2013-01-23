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

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.controller.CQSAction;
import com.comcast.cqs.persistence.RedisPayloadCacheCassandraPersistence;

/**
 * Provide a basic health-check URL for load-balancers to hit to monitor whether service is up and version
 * @author aseem
 */
public class HealthCheckShallow extends CQSAction {

    public HealthCheckShallow() {
        super("HealthCheck");
    }

    @Override
    public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {        
        
    	
    	boolean healthy = true;
        
        StringBuffer sb = new StringBuffer("");
        
        sb.append("<HealthCheckResponse>\n");
        sb.append("\t<Version>" + CMBControllerServlet.VERSION + "</Version>\n");
        
        try {
        	if (RedisPayloadCacheCassandraPersistence.isAlive()) {
        		sb.append("\t<Redis>OK</Redis>\n");
        	} else {
        		sb.append("\t<Redis>Some or all shards down.</Redis>\n");
        		healthy = false;
        	}
        } catch (Exception ex) {
    		sb.append("\t<Redis>Cache unavailable: "+ex.getMessage()+"</Redis>\n");
    		healthy = false;
        }
        
        try {
        	
        	CassandraPersistence cassandra = new CassandraPersistence(CMBProperties.getInstance().getCMBCommonKeyspace());
        	
        	if (cassandra.isAlive()) {
        		sb.append("\t<Cassandra>OK</Cassandra>\n");
        	} else {
        		sb.append("\t<Cassandra>Ring unavailable.</Cassandra>\n");
        		healthy = false;
        	}
        } catch (Exception ex) {
    		sb.append("\t<Cassandra>Ring unavailable: "+ex.getMessage()+"</Cassandra>\n");
    		healthy = false;
        }

        sb.append("</HealthCheckResponse>");
        
    	if (healthy) {
    		response.setStatus(HttpServletResponse.SC_OK);
    	} else {
    		response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    	}

    	response.getOutputStream().print(sb.toString());
        response.flushBuffer();
        
        return true;
    }
    
    @Override
    public boolean isAuthRequired() {
        return false;
    }
}
