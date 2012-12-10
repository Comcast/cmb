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

import java.util.ArrayList;
import java.util.List;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.beans.Row;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.io.CNSWorkerStatsPopulator;
import com.comcast.cns.model.CNSWorkerStats;
import com.comcast.cns.util.CNSErrorCodes;
/**
 * Subscribe action
 * @author bwolf
 *
 */
public class CNSClearWorkerQueuesAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSGetWorkerStatsAction.class);
	
	public CNSClearWorkerQueuesAction() {
		super("ClearWorkerQueues");
	}

    /**
     * Get various stats about active cns workers
     * @param user the user for whom we are subscribing.
     * @param request the servlet request including all the parameters for the doUnsubscribe call
     * @param response the response servlet we write to.
     */
	@Override
	public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
		
		String host = request.getParameter("Host");

		if (host == null || host.equals("")) {
    		logger.error("event=cns_clear_worker_queues status=failure errorType=InvalidParameters details=missing_parameter_host");
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"Request parameter Host missing.");
		}
		
		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCMBCNSKeyspace());
		
		List<Row<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows("CNSWorkers", null, 1000, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), HConsistencyLevel.QUORUM);
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

				statsList.add(stats);
			}
		}
		
		for (CNSWorkerStats stats : statsList) {
			
			if (stats.getIpAddress().equals(host) && stats.getJmxPort() > 0) {
			
				JMXConnector jmxConnector = null;
				String url = null;

				try {

					long port = stats.getJmxPort();
					url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";

					JMXServiceURL serviceUrl = new JMXServiceURL(url);
					jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);

					MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
					ObjectName cnsWorkerMonitor = new ObjectName("com.comcast.cns.controller:type=CNSMonitorMBean");
					CNSMonitorMBean mbeanProxy = JMX.newMBeanProxy(mbeanConn, cnsWorkerMonitor,	CNSMonitorMBean.class, false);
					
					mbeanProxy.clearWorkerQueues();
					
			    	String res = CNSWorkerStatsPopulator.getGetWorkerClearQueuesResponse();	
					response.getWriter().println(res);

			    	return true;

				} finally {

					if (jmxConnector != null) {
						jmxConnector.close();
					}
				}
			}
		}
		
		throw new CMBException(CMBErrorCodes.NotFound, "Cannot clear worker queues: Host " + host + " not found.");
	}
}
