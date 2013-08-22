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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Row;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.io.CNSWorkerStatsPopulator;
import com.comcast.cns.model.CNSWorkerStats;
/**
 * Subscribe action
 * @author bwolf, jorge
 *
 */
public class CNSGetWorkerStatsAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSGetWorkerStatsAction.class);
	
	public CNSGetWorkerStatsAction() {
		super("GetWorkerStats");
	}
	
    @Override
    public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    	return true;
    }
    
    public static List<CNSWorkerStats> getWorkerStats() throws IOException {
    	
		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCNSKeyspace());
		
		List<Row<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows("CNSWorkers", null, 1000, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getConsistencyLevel());
		List<CNSWorkerStats> statsList = new ArrayList<CNSWorkerStats>();
		
		if (rows != null) {
			
			for (Row<String, String, String> row : rows) {
				
				CNSWorkerStats stats = new CNSWorkerStats();
				stats.setIpAddress(row.getKey());
				
				if (row.getColumnSlice().getColumnByName("producerTimestamp") != null) {
					stats.setProducerTimestamp(Long.parseLong(row.getColumnSlice().getColumnByName("producerTimestamp").getValue()));
				}
				
				if (row.getColumnSlice().getColumnByName("consumerTimestamp") != null) {
					stats.setConsumerTimestamp(Long.parseLong(row.getColumnSlice().getColumnByName("consumerTimestamp").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("jmxport") != null) {
					stats.setJmxPort(Long.parseLong(row.getColumnSlice().getColumnByName("jmxport").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("mode") != null) {
					stats.setMode(row.getColumnSlice().getColumnByName("mode").getValue());
				}

				if (row.getColumnSlice().getColumnByName("dataCenter") != null) {
					stats.setDataCenter(row.getColumnSlice().getColumnByName("dataCenter").getValue());
				}

				if (stats.getDataCenter() != null && stats.getDataCenter().equals(CMBProperties.getInstance().getCMBDataCenter())) {
					statsList.add(stats);
				}
			}
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
					
					Integer numPublishedMessages = (Integer)mbeanConn.getAttribute(cnsWorkerMonitor, "NumPublishedMessages");
					stats.setNumPublishedMessages(numPublishedMessages);
					
					@SuppressWarnings("unchecked")
					Map<String, Integer> errorCountForEndpoints = (Map<String, Integer>)mbeanConn.getAttribute(cnsWorkerMonitor, "RecentErrorCountForEndpoints");
					stats.setErrorCountForEndpoints(errorCountForEndpoints);

					Boolean cqsServiceAvailable = (Boolean)mbeanConn.getAttribute(cnsWorkerMonitor, "CQSServiceAvailable");
					stats.setCqsServiceAvailable(cqsServiceAvailable);
					
					Integer numPooledHttpConnections = (Integer)mbeanConn.getAttribute(cnsWorkerMonitor, "NumPooledHttpConnections");
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
        List<CNSWorkerStats> statsList = getWorkerStats();
    	String res = CNSWorkerStatsPopulator.getGetWorkerStatsResponse(statsList);	
		response.getWriter().println(res);

    	return true;
	}
}
