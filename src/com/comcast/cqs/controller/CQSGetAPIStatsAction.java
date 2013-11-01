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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
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
import com.comcast.cqs.io.CQSAPIStatsPopulator;
import com.comcast.cqs.model.CQSAPIStats;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
/**
 * Subscribe action
 * @author bwolf, jorge
 *
 */
public class CQSGetAPIStatsAction extends CQSAction {

	private static Logger logger = Logger.getLogger(CQSGetAPIStatsAction.class);
	
	public CQSGetAPIStatsAction() {
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
		
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCQSKeyspace());
		
		List<Row<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows("CQSAPIServers", null, 1000, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		List<CQSAPIStats> statsList = new ArrayList<CQSAPIStats>();
		
		if (rows != null) {
			
			for (Row<String, String, String> row : rows) {
				
				CQSAPIStats stats = new CQSAPIStats();
				stats.setIpAddress(row.getKey());
				
				if (row.getColumnSlice().getColumnByName("timestamp") != null) {
					stats.setTimestamp(Long.parseLong(row.getColumnSlice().getColumnByName("timestamp").getValue()));
				}
				
				if (row.getColumnSlice().getColumnByName("jmxport") != null) {
					stats.setJmxPort(Long.parseLong(row.getColumnSlice().getColumnByName("jmxport").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("port") != null) {
					stats.setLongPollPort(Long.parseLong(row.getColumnSlice().getColumnByName("port").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("dataCenter") != null) {
					stats.setDataCenter(row.getColumnSlice().getColumnByName("dataCenter").getValue());
				}
				
				if (row.getColumnSlice().getColumnByName("serviceUrl") != null) {
					stats.setServiceUrl(row.getColumnSlice().getColumnByName("serviceUrl").getValue());
				}
				
				if (row.getColumnSlice().getColumnByName("redisServerList") != null) {
					stats.setRedisServerList(row.getColumnSlice().getColumnByName("redisServerList").getValue());
				}

				if (stats.getIpAddress().contains(":")) {
					statsList.add(stats);
				}
			}
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

					//String cassandraNodes = (String)mbeanConn.getAttribute(cqsAPIMonitor, "CassandraNodes");
					
					Set<ObjectInstance> objectInstances = mbeanConn.queryMBeans(new ObjectName("me.prettyprint.cassandra.*:*"), null);

					if (objectInstances.size() == 1) {
					
						//ObjectName hectorMonitor = new ObjectName("me.prettyprint.cassandra.service_cmb:ServiceType=CMB,MonitorType=hector");
						ObjectName hectorMonitor = ((ObjectInstance)objectInstances.toArray()[0]).getObjectName();
						
						String cassandraNodes = "";

						@SuppressWarnings("unchecked")
						List<String> knownHosts = (List<String>)mbeanConn.getAttribute(hectorMonitor, "KnownHosts");
						
						for (String knownHost : knownHosts) {
							cassandraNodes += knownHost + " ";
						}
						
						stats.setCassandraNodes(cassandraNodes);
					}
					
					ObjectName cqsAPIMonitor = new ObjectName("com.comcast.cqs.controller:type=CQSMonitorMBean");
					
					String cassandraClusterName = (String)mbeanConn.getAttribute(cqsAPIMonitor, "CassandraClusterName");
					stats.setCassandraClusterName(cassandraClusterName);


					Long numberOfLongPollReceives = (Long)mbeanConn.getAttribute(cqsAPIMonitor, "NumberOfLongPollReceives");
					stats.setNumberOfLongPollReceives(numberOfLongPollReceives);
					
					Integer numberOfRedisShards = (Integer)mbeanConn.getAttribute(cqsAPIMonitor, "NumberOfRedisShards");
					stats.setNumberOfRedisShards(numberOfRedisShards);

					@SuppressWarnings("unchecked")
					List<Map<String, String>> redisShardInfos = (List<Map<String, String>>)mbeanConn.getAttribute(cqsAPIMonitor, "RedisShardInfos");
					
					try {
						for (Map<String, String> shardInfo : redisShardInfos) {
							if (shardInfo.containsKey("db0")) {
								long numKeys = Long.parseLong(shardInfo.get("db0").split(",")[0].split("=")[1]);
								stats.setNumberOfRedisKeys(numKeys);
							}
						}
					} catch (Exception ex) {
						logger.warn("event=failed_to_get_redis_shard_info url=" + url, ex);
						stats.addStatus("REDIS SHARD INFO UNAVAILABLE");
					}
					
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
				
				try {
					if (!RedisCachedCassandraPersistence.isAlive()) {
						stats.addStatus("REDIS UNAVAILABLE");
					}
				} catch (Exception ex) {
					stats.addStatus("REDIS UNAVAILABLE");
				}
				
	        	try {
				
					CassandraPersistence cassandra = new CassandraPersistence(CMBProperties.getInstance().getCMBKeyspace());
		        	
		        	if (!cassandra.isAlive()) {
						stats.addStatus("CASSANDRA UNAVAILABLE");
		        	}

	        	} catch (Exception ex) {
					stats.addStatus("CASSANDRA UNAVAILABLE");
	        	}
			}
			
			if (stats.getStatus() == null) {
				stats.addStatus("OK");
			}
		}

    	String out = CQSAPIStatsPopulator.getGetAPIStatsResponse(statsList);	
        writeResponse(out, response);

    	return true;
	}
}
