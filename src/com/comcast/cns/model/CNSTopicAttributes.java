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
 * represents topic attributes
 * @author bwolf, jorge
 *
 * Class is not thread-safe. Caller must ensure thread-safety
 */
public class CNSTopicAttributes {
	
	private CNSTopicDeliveryPolicy effectiveDeliveryPolicy;
	
	private CNSTopicDeliveryPolicy deliveryPolicy;

	private String userId;
	
	private long subscriptionsPending;
	
	private long subscriptionsConfirmed;
	
	private long subscriptionsDeleted;
	
	// for now policy is just a string, will inflate when we tackle permission stories
	
	private String policy;
	
	private String topicArn;
	
	private String displayName;
	
	public CNSTopicAttributes() {
	}
	
	public CNSTopicAttributes(String topicArn, String userId) {

		this.topicArn = topicArn;
		this.userId = userId;
		this.deliveryPolicy = new CNSTopicDeliveryPolicy();
		this.subscriptionsPending = 0;
		this.subscriptionsConfirmed = 0;
		this.subscriptionsDeleted = 0;
	}
	
	public CNSTopicDeliveryPolicy getEffectiveDeliveryPolicy() {
		return effectiveDeliveryPolicy;
	}

	public void setEffectiveDeliveryPolicy(CNSTopicDeliveryPolicy effectiveDeliveryPolicy) {
		this.effectiveDeliveryPolicy = effectiveDeliveryPolicy;
	}

	public CNSTopicDeliveryPolicy getDeliveryPolicy() {
		return deliveryPolicy;
	}

	public void setDeliveryPolicy(CNSTopicDeliveryPolicy deliveryPolicy) {
		this.deliveryPolicy = deliveryPolicy;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public long getSubscriptionsPending() {
		return subscriptionsPending;
	}

	public void setSubscriptionsPending(long subscriptionsPending) {
		this.subscriptionsPending = subscriptionsPending;
	}

	public long getSubscriptionsConfirmed() {
		return subscriptionsConfirmed;
	}

	public void setSubscriptionsConfirmed(long subscriptionsConfirmed) {
		this.subscriptionsConfirmed = subscriptionsConfirmed;
	}

	public long getSubscriptionsDeleted() {
		return subscriptionsDeleted;
	}

	public void setSubscriptionsDeleted(long subscriptionsDeleted) {
		this.subscriptionsDeleted = subscriptionsDeleted;
	}

	public String getPolicy() {
		return policy;
	}

	public void setPolicy(String policy) {
		this.policy = policy;
	}

	public String getTopicArn() {
		return topicArn;
	}

	public void setTopicArn(String topicArn) {
		this.topicArn = topicArn;
	}
	
	@Override
	public String toString() {
		return "subscriptions_pending=" + subscriptionsPending + " subscriptions_confirmed=" + subscriptionsConfirmed + " subscriptionsDeleted=" + subscriptionsDeleted + " user_id=" + userId + " topci_arn=" + topicArn + " delivery_policy=" + policy + "effective_delivery_policy=" + effectiveDeliveryPolicy;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
}
