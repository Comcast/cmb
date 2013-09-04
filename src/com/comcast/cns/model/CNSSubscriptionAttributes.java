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

/**
 * represents Subscription Attributes
 * @author bwolf, jorge
 *
 * Class is not thread-safe. Caller must ensure thread-safety
 */
public class CNSSubscriptionAttributes {
	
	private CNSSubscriptionDeliveryPolicy effectiveDeliveryPolicy;

	private CNSSubscriptionDeliveryPolicy deliveryPolicy;
	
	

	private String userId;
	
	private boolean confirmationWasAuthenticated;
	
	private String topicArn;
	
	private String subscriptionArn;
	
	public CNSSubscriptionAttributes() {
	}
	
	public CNSSubscriptionAttributes(String topicArn, String subscriptionArn, String userId) {
		
		this.topicArn = topicArn;
		this.subscriptionArn = subscriptionArn;
		this.userId = userId;
		this.deliveryPolicy = null;//new CNSSubscriptionDeliveryPolicy();
	}
	
	public CNSSubscriptionDeliveryPolicy getEffectiveDeliveryPolicy() {
		return effectiveDeliveryPolicy;
	}

	public void setEffectiveDeliveryPolicy(CNSSubscriptionDeliveryPolicy effectiveDeliveryPolicy) {
		this.effectiveDeliveryPolicy = effectiveDeliveryPolicy;
	}

	public CNSSubscriptionDeliveryPolicy getDeliveryPolicy() {
		return deliveryPolicy;
	}

	public void setDeliveryPolicy(CNSSubscriptionDeliveryPolicy deliveryPolicy) {
		this.deliveryPolicy = deliveryPolicy;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public boolean isConfirmationWasAuthenticated() {
		return confirmationWasAuthenticated;
	}

	public void setConfirmationWasAuthenticated(boolean confirmationWasAuthenticated) {
		this.confirmationWasAuthenticated = confirmationWasAuthenticated;
	}

	public String getTopicArn() {
		return topicArn;
	}

	public void setTopicArn(String topicArn) {
		this.topicArn = topicArn;
	}

	public String getSubscriptionArn() {
		return subscriptionArn;
	}

	public void setSubscriptionArn(String subscriptionArn) {
		this.subscriptionArn = subscriptionArn;
	}
	
	@Override
	public String toString() {
		return "user_id=" + userId + " topci_arn=" + topicArn + " subscription_arn=" + subscriptionArn + 
				" effective_delivery_policy=" + effectiveDeliveryPolicy  + 
				" delivery_policy=" + deliveryPolicy + 
				" confirmation_was_authenticated=" + confirmationWasAuthenticated;
	}

}
