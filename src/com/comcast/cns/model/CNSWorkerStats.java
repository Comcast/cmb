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
package com.comcast.cns.model;

import java.util.Map;

public class CNSWorkerStats {
	
	private String ipAddress;
	
	private long producerTimestamp;
	
	private long consumerTimestamp;
	
	private String mode;

	private long jmxPort;
	
	private boolean consumerOverloaded;
	
	private int deliveryQueueSize;
	
	private int redeliveryQueueSize;
	
	private long numPublishedMessages;
	
	private Map<String, String> errorRateForEndpoints;

	public Map<String, String> getErrorRateForEndpoints() {
		return errorRateForEndpoints;
	}

	public void setErrorRateForEndpoints(Map<String, String> errorRateForEndpoints) {
		this.errorRateForEndpoints = errorRateForEndpoints;
	}

	public long getNumPublishedMessages() {
		return numPublishedMessages;
	}

	public void setNumPublishedMessages(long numPublishedMessages) {
		this.numPublishedMessages = numPublishedMessages;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public long getJmxPort() {
		return jmxPort;
	}

	public void setJmxPort(long jmxPort) {
		this.jmxPort = jmxPort;
	}

	public boolean isConsumerOverloaded() {
		return consumerOverloaded;
	}

	public void setConsumerOverloaded(boolean consumerOverloaded) {
		this.consumerOverloaded = consumerOverloaded;
	}

	public int getDeliveryQueueSize() {
		return deliveryQueueSize;
	}

	public void setDeliveryQueueSize(int deliverQueueSize) {
		this.deliveryQueueSize = deliverQueueSize;
	}

	public int getRedeliveryQueueSize() {
		return redeliveryQueueSize;
	}

	public void setRedeliveryQueueSize(int redeliveryQueueSize) {
		this.redeliveryQueueSize = redeliveryQueueSize;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public long getProducerTimestamp() {
		return producerTimestamp;
	}

	public void setProducerTimestamp(long timestamp) {
		this.producerTimestamp = timestamp;
	}
	
	public boolean isProducerActive() {
		
		if (System.currentTimeMillis() - this.producerTimestamp > 120*1000) {
			return false;
		} else {
			return true;
		}
	}

	public long getConsumerTimestamp() {
		return consumerTimestamp;
	}

	public void setConsumerTimestamp(long timestamp) {
		this.consumerTimestamp = timestamp;
	}
	
	public boolean isConsumerActive() {
		
		if (System.currentTimeMillis() - this.consumerTimestamp > 120*1000) {
			return false;
		} else {
			return true;
		}
	}
}
