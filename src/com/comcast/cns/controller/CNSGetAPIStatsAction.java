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
package com.comcast.cns.controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cqs.io.CQSAPIStatsPopulator;
import com.comcast.cqs.model.CQSAPIStats;
import com.comcast.cqs.util.CQSAPIStatWrapper;
/**
 * Subscribe action
 * @author bwolf, jorge
 *
 */
public class CNSGetAPIStatsAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSGetAPIStatsAction.class);
	
	public CNSGetAPIStatsAction() {
		super("GetAPIStats");
	}
	
	@Override
    public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    	return true;
    }

    /**
     * Get various stats about active cns workers
     * @param user the user for whom we are subscribing.
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
		HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
		HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
		//if parameter includes GetDataCenter, only get Data center name and return
		String  subTask= request.getParameter("SubTask");
		if((subTask!=null)&&(subTask.equals("GetDataCenter"))){
	    	String out = CQSAPIStatsPopulator.getGetAPIStatsDataCenterResponse(CQSAPIStatWrapper.getCNSDataCenterNames());	
	        writeResponse(out, response);
	    	return true;
		}

		List<CQSAPIStats> statsList = null;
		String dataCenter = request.getParameter("DataCenter");
    	if(dataCenter == null || dataCenter.length() == 0){
    		statsList = CQSAPIStatWrapper.getCNSAPIStats();
    	} else{
    		statsList = CQSAPIStatWrapper.getCNSAPIStatsByDataCenter(dataCenter);
    	}
		
		for (CQSAPIStats stats : statsList) {
			
			if (System.currentTimeMillis() - stats.getTimestamp() >= 5*60*1000) {
				
				stats.addStatus("STALE");
				
			} else if (stats.getJmxPort() > 0) {
			
				JMXConnector jmxConnector = null;
				String url = null;

				try {

					String host = stats.getIpAddress();  
					
					if (host.contains(":")) {
						host = host.substring(0, host.indexOf(":"));
					}

					long port = stats.getJmxPort();
					url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";

					JMXServiceURL serviceUrl = new JMXServiceURL(url);
					jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);

					MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();

					ObjectName cqsAPIMonitor = new ObjectName("com.comcast.cns.controller:type=CNSMonitorMBean");

					@SuppressWarnings("unchecked")
					Map<String, AtomicLong> callStats = (Map<String, AtomicLong>)mbeanConn.getAttribute(cqsAPIMonitor, "CallStats");
					
					stats.setCallStats(callStats);
					
					@SuppressWarnings("unchecked")
					Map<String, AtomicLong> callFailureStats = (Map<String, AtomicLong>)mbeanConn.getAttribute(cqsAPIMonitor, "CallFailureStats");

					stats.setCallFailureStats(callFailureStats);

				} catch (Exception ex) {

					logger.warn("event=failed_to_connect_to_jmx_server url=" + url, ex);
					stats.addStatus("JMX UNAVAILABLE");

				} finally {

					if (jmxConnector != null) {
						jmxConnector.close();
					}
				}
				
				if (stats.getStatus() == null) {
					stats.addStatus("OK");
				}
			}
		}

    	String out = CQSAPIStatsPopulator.getGetAPIStatsResponse(statsList);	
        writeResponse(out, response);

    	return true;
	}
}
