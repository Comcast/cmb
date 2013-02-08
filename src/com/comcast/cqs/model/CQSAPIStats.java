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
package com.comcast.cqs.model;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CQSAPIStats {
	
	private String dataCenter;
	
	private String ipAddress;
	
	private long timestamp;
	
	private long jmxPort;
	
	private long longPollPort;
	
	private long numberOfLongPollReceives;

	private long numberOfRedisKeys;
	
	private int numberOfRedisShards;
	
	private Map<String, AtomicLong> callStats;
	
	private Map<String, AtomicLong> callFailureStats; 
	
	private String serviceUrl;
	
	private String redisServerList;
	
	private String status;
	
	private String cassandraClusterName;
	
	private String cassandraNodes;
	
	public String getCassandraClusterName() {
		return cassandraClusterName;
	}

	public void setCassandraClusterName(String cassandraClusterName) {
		this.cassandraClusterName = cassandraClusterName;
	}

	public String getCassandraNodes() {
		return cassandraNodes;
	}

	public void setCassandraNodes(String cassandraNodes) {
		this.cassandraNodes = cassandraNodes;
	}

	public String getStatus() {
		return status;
	}

	public void addStatus(String status) {
		
		if (this.status == null) {
			this.status = status;
		} else {
			this.status += ", " + status;
		}
	}

	public String getRedisServerList() {
		return redisServerList;
	}

	public void setRedisServerList(String redisServerList) {
		this.redisServerList = redisServerList;
	}

	public String getServiceUrl() {
		return serviceUrl;
	}

	public void setServiceUrl(String serviceUrl) {
		this.serviceUrl = serviceUrl;
	}

	public Map<String, AtomicLong> getCallStats() {
		return callStats;
	}

	public void setCallStats(Map<String, AtomicLong> callStats) {
		this.callStats = callStats;
	}

	public Map<String, AtomicLong> getCallFailureStats() {
		return callFailureStats;
	}

	public void setCallFailureStats(Map<String, AtomicLong> callFailureStats) {
		this.callFailureStats = callFailureStats;
	}

	public int getNumberOfRedisShards() {
		return numberOfRedisShards;
	}

	public void setNumberOfRedisShards(int numberOfRedisShards) {
		this.numberOfRedisShards = numberOfRedisShards;
	}

	public long getNumberOfRedisKeys() {
		return numberOfRedisKeys;
	}

	public void setNumberOfRedisKeys(long numberOfRedisKeys) {
		this.numberOfRedisKeys = numberOfRedisKeys;
	}

	public long getNumberOfLongPollReceives() {
		return numberOfLongPollReceives;
	}

	public void setNumberOfLongPollReceives(long numberOfLongPollReceives) {
		this.numberOfLongPollReceives = numberOfLongPollReceives;
	}

	public long getLongPollPort() {
		return longPollPort;
	}

	public void setLongPollPort(long longPollPort) {
		this.longPollPort = longPollPort;
	}

	public String getDataCenter() {
		return dataCenter;
	}

	public void setDataCenter(String dataCenter) {
		this.dataCenter = dataCenter;
	}


	public long getJmxPort() {
		return jmxPort;
	}

	public void setJmxPort(long jmxPort) {
		this.jmxPort = jmxPort;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
