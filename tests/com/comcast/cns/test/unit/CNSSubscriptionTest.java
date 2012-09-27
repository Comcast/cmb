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
package com.comcast.cns.test.unit;

public class CNSSubscriptionTest {
	
	private String topicArn;
	private String protocol;
	private String subscriptionArn;
	private String owner;
	private String endpoint;
	
	/*
	 * setTopicArn, allows user to set the topic for this testing tool
	 * @param topicArn
	 */
	public void setTopicArn(String topicArn) {
		this.topicArn = topicArn;
	}
	
	public String getTopicArn() {
		return topicArn;
	}
	
	/*
	 * setProtocol, allows user to set the protocol for this testing tool
	 * @param protocol
	 */
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	
	public String getProtocol() {
		return protocol;
	}
	
	/*
	 * setSubscriptionArn, allows user to set the subscriptionArn for this testing tool
	 * @param subscriptionArn
	 */
	public void setSubscriptionArn(String subscriptionArn) {
		this.subscriptionArn = subscriptionArn;
	}
	
	public String getSubscriptionArn() {
		return subscriptionArn;
	}
	
	public void setOwner(String owner) {
		this.owner = owner;
	}
	
	public String getOwner() {
		return owner;
	}
	
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	
	public String getEndpoint() {
		return endpoint;
	}
}
