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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
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
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.io.CNSWorkerStatsPopulator;
import com.comcast.cns.model.CNSWorkerStats;
import com.comcast.cns.tools.CNSWorkerMonitor;
import com.comcast.cns.util.CNSWorkerStatWrapper;
/**
 * Subscribe action
 * @author bwolf, jorge
 *
 */
public class CNSGetWorkerStatsAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSGetWorkerStatsAction.class);
	
	static {
		//register JMX Bean
		try{
	        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
	        ObjectName name = new ObjectName("com.comcast.cns.tools:type=CNSWorkerMonitorMBean");
	        
	        if (!mbs.isRegistered(name)) {
	            mbs.registerMBean(CNSWorkerMonitor.getInstance(), name);
	        }
		} catch (Exception ex){
			logger.error("event=failed_to_register_jmx_Bean", ex);
		}
	}
	
	public CNSGetWorkerStatsAction() {
		super("GetWorkerStats");
	}
	
    @Override
    public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    	return true;
    }
    
    public static List<CNSWorkerStats> getWorkerStats(String dataCenter) throws IOException, PersistenceException {
    	
    	List<CNSWorkerStats> statsList = null;
    	
    	if(dataCenter == null || dataCenter.length() == 0){
    		statsList = CNSWorkerStatWrapper.getCassandraWorkerStats();
    	} else{
    		statsList = CNSWorkerStatWrapper.getCassandraWorkerStatsByDataCenter(dataCenter);
    	}
		for (CNSWorkerStats stats : statsList) {
			
			long now = System.currentTimeMillis();
			
			if (stats.getJmxPort() > 0 && (now-stats.getConsumerTimestamp()<5*60*1000 || now-stats.getProducerTimestamp()<5*60*1000)) {
			
				JMXConnector jmxConnector = null;
				String url = null;

				try {

					String host = stats.getIpAddress();  
					long port = stats.getJmxPort();
					url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";
					
					JMXServiceURL serviceUrl = new JMXServiceURL(url);
					jmxConnector = JMXConnectorFactory.connect(serviceUrl);

					MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();

					//Set<ObjectName> beanSet = mbeanConn.queryNames(null, null);

					ObjectName cnsWorkerMonitor = new ObjectName("com.comcast.cns.tools:type=CNSWorkerMonitorMBean");

					Integer deliveryQueueSize = (Integer)mbeanConn.getAttribute(cnsWorkerMonitor, "DeliveryQueueSize");
					stats.setDeliveryQueueSize(deliveryQueueSize);
					
					Integer redeliveryQueueSize = (Integer)mbeanConn.getAttribute(cnsWorkerMonitor, "RedeliveryQueueSize");
					stats.setRedeliveryQueueSize(redeliveryQueueSize);
					
					Boolean consumerOverloaded = (Boolean)mbeanConn.getAttribute(cnsWorkerMonitor, "ConsumerOverloaded");
					stats.setConsumerOverloaded(consumerOverloaded);
					
					Integer numPublishedMessages = (Integer)mbeanConn.getAttribute(cnsWorkerMonitor, "RecentNumberOfPublishedMessages");
					stats.setNumPublishedMessages(numPublishedMessages);
					
					@SuppressWarnings("unchecked")
					Map<String, Integer> errorCountForEndpoints = (Map<String, Integer>)mbeanConn.getAttribute(cnsWorkerMonitor, "RecentErrorCountForEndpoints");
					stats.setErrorCountForEndpoints(errorCountForEndpoints);

					Boolean cqsServiceAvailable = (Boolean)mbeanConn.getAttribute(cnsWorkerMonitor, "CQSServiceAvailable");
					stats.setCqsServiceAvailable(cqsServiceAvailable);
					
					Integer numPooledHttpConnections = (Integer)mbeanConn.getAttribute(cnsWorkerMonitor, "PublishHttpPoolSize");
					stats.setNumPooledHttpConnections(numPooledHttpConnections);

				} catch (Exception ex) {

					logger.warn("event=failed_to_connect_to_jmx_server url=" + url, ex);

				} finally {

					if (jmxConnector != null) {
						jmxConnector.close();
					}
				}
			}
		}
		
		return statsList;
    }

    /**
     * Get various stats about active cns workers
     * @param user the user for whom we are subscribing.
     * @param asyncContext
     */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
		String dataCenter = request.getParameter("DataCenter");
        List<CNSWorkerStats> statsList = getWorkerStats(dataCenter);
    	String out = CNSWorkerStatsPopulator.getGetWorkerStatsResponse(statsList);	
        writeResponse(out, response);

    	return true;
	}
}
