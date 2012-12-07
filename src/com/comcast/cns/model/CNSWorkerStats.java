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

public class CNSWorkerStats {
	
	private String ipAddress;
	
	private long timestamp;
	
	private boolean consumerOverloaded;
	
	private int deliveryQueueSize;
	
	private int redeliveryQueueSize;

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

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public boolean isActive() {
		
		if (System.currentTimeMillis() - this.timestamp > 120*1000) {
			return false;
		} else {
			return true;
		}
	}
}
