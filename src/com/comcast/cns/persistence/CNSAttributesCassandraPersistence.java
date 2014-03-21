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
package com.comcast.cns.persistence;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.comcast.cmb.common.persistence.AbstractCassandraPersistence;
import com.comcast.cmb.common.persistence.AbstractCassandraPersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.AbstractCassandraPersistence.CmbColumnSlice;
import com.comcast.cmb.common.persistence.CassandraPersistenceFactory;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cns.controller.CNSCache;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.model.CNSTopicDeliveryPolicy;

/**
 * Provide Cassandra Persistence for topic attributes
 * @author bwolf, vvenkatraman, jorge, tina
 *
 */
public class CNSAttributesCassandraPersistence implements ICNSAttributesPersistence {
	
	private static final String columnFamilyTopicAttributes = "CNSTopicAttributes";
	private static final String columnFamilySubscriptionAttributes = "CNSSubscriptionAttributes";
	private static final String columnFamilyTopicStats = "CNSTopicStats";
	private static Logger logger = Logger.getLogger(CNSAttributesCassandraPersistence.class);
	private static final AbstractCassandraPersistence cassandraHandler = CassandraPersistenceFactory.getInstance();
	
	public CNSAttributesCassandraPersistence() {
	}

	@Override
	public void setTopicAttributes(CNSTopicAttributes topicAttributes, String topicArn) throws Exception {
		
		cassandraHandler.insertRow(AbstractCassandraPersistence.CNS_KEYSPACE, topicArn, columnFamilyTopicAttributes, getColumnValues(topicAttributes), CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, null);
		
		if (topicAttributes.getDisplayName() != null) {
			PersistenceFactory.getTopicPersistence().updateTopicDisplayName(topicArn, topicAttributes.getDisplayName());
		}
		
		CNSCache.removeTopicAttributes(topicArn);
	}

	private Map<String, String> getColumnValues(CNSTopicAttributes topicAttributes) {
		
		Map<String, String> columnVals = new HashMap<String, String>();
		
		if (topicAttributes.getUserId() != null) {
			columnVals.put("userId", topicAttributes.getUserId());
		}
		
		if (topicAttributes.getTopicArn() != null) {
			columnVals.put("topicArn", topicAttributes.getTopicArn());
		}
		
		if (topicAttributes.getSubscriptionsPending() > -1) {
			columnVals.put("subscriptionPending", topicAttributes.getSubscriptionsPending()+"");
		}
		
		if (topicAttributes.getSubscriptionsDeleted() > -1) {
			columnVals.put("subscriptionDeleted", topicAttributes.getSubscriptionsDeleted()+"");
		}
		
		if (topicAttributes.getSubscriptionsConfirmed() > -1) {
			columnVals.put("subscriptionConfirmed", topicAttributes.getSubscriptionsConfirmed()+"");
		}
		
		// currently only accept delivery policy parameter, fill up with defaults if elements are missing and return as both delivery policy and effective delivery policy
		
		/*if (topicAttributes.getEffectiveDeliveryPolicy() != null) {
			columnVals.put("effectiveDeliveryPolicy", topicAttributes.getEffectiveDeliveryPolicy().toString());
		}*/
		
		if (topicAttributes.getDeliveryPolicy() != null) {
			columnVals.put("deliveryPolicy", topicAttributes.getDeliveryPolicy().toString());
		}

		if (topicAttributes.getPolicy() != null) {
			columnVals.put("policy", topicAttributes.getPolicy().toString());
		}
		
		return columnVals;
	}

	@Override
	public CNSTopicAttributes getTopicAttributes(String topicArn) throws Exception {
		
		CNSTopicAttributes topicAttributes = new CNSTopicAttributes();
		topicAttributes.setTopicArn(topicArn);
		
		CmbColumnSlice<String, String> slice = cassandraHandler.readColumnSlice(AbstractCassandraPersistence.CNS_KEYSPACE, columnFamilyTopicAttributes, topicArn, null, null, 10, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		
		if (slice != null) {
			
			if (slice.getColumnByName("policy") != null) {
				topicAttributes.setPolicy(slice.getColumnByName("policy").getValue());
			}
			
			CNSTopicDeliveryPolicy deliveryPolicy = null;
			
			if (slice.getColumnByName("deliveryPolicy") != null) {
				deliveryPolicy = new CNSTopicDeliveryPolicy(new JSONObject(slice.getColumnByName("deliveryPolicy").getValue()));
			} else {
				deliveryPolicy = new CNSTopicDeliveryPolicy();
			}
			
			topicAttributes.setEffectiveDeliveryPolicy(deliveryPolicy);		
			topicAttributes.setDeliveryPolicy(deliveryPolicy);

			if (slice.getColumnByName("userId") != null) {
				topicAttributes.setUserId(slice.getColumnByName("userId").getValue());
			}
			
			topicAttributes.setDisplayName(PersistenceFactory.getTopicPersistence().getTopic(topicArn).getDisplayName());
		}
		
		long subscriptionConfirmedCount = cassandraHandler.getCounter(AbstractCassandraPersistence.CNS_KEYSPACE, columnFamilyTopicStats, topicArn, "subscriptionConfirmed", CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		topicAttributes.setSubscriptionsConfirmed(subscriptionConfirmedCount);
		
		long subscriptionPendingCount = cassandraHandler.getCounter(AbstractCassandraPersistence.CNS_KEYSPACE, columnFamilyTopicStats, topicArn, "subscriptionPending", CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		topicAttributes.setSubscriptionsPending(subscriptionPendingCount);
		
		long subscriptionDeletedCount = cassandraHandler.getCounter(AbstractCassandraPersistence.CNS_KEYSPACE, columnFamilyTopicStats, topicArn, "subscriptionDeleted", CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		topicAttributes.setSubscriptionsDeleted(subscriptionDeletedCount);

		return topicAttributes;
	}

	@Override
	public void setSubscriptionAttributes(CNSSubscriptionAttributes subscriptionAtributes, String subscriptionArn) throws Exception {

		cassandraHandler.insertRow(AbstractCassandraPersistence.CNS_KEYSPACE, subscriptionArn, columnFamilySubscriptionAttributes, getColumnValues(subscriptionAtributes), CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, null);
		String topicArn = com.comcast.cns.util.Util.getCnsTopicArn(subscriptionArn);
		CNSCache.removeTopicAttributes(topicArn);
	}

	private Map<String, String> getColumnValues(CNSSubscriptionAttributes subscriptionAttributes) {
		
		Map<String, String> colVals = new HashMap<String, String>();
		
		if (subscriptionAttributes.getDeliveryPolicy() != null) {
			colVals.put("deliveryPolicy", subscriptionAttributes.getDeliveryPolicy().toString());
		}
		
		
		// currently only accept delivery policy parameter, fill up with defaults if elements are missing and return as both delivery policy and effective delivery policy
		
		/*if (subscriptionAtributes.getEffectiveDeliveryPolicy() != null) {
			colVals.put("effectiveDeliveryPolicy", subscriptionAtributes.getEffectiveDeliveryPolicy().toString());
		}*/
		
		if (subscriptionAttributes.getSubscriptionArn() != null) {
			colVals.put("subscriptionArn", subscriptionAttributes.getSubscriptionArn());
		}
		
		if (subscriptionAttributes.getTopicArn() != null) {
			colVals.put("topicArn", subscriptionAttributes.getTopicArn());
		}
		
		if (subscriptionAttributes.getUserId() != null) {
			colVals.put("userId", subscriptionAttributes.getUserId());
		}
		
		return colVals;
	}

	@Override
	public CNSSubscriptionAttributes getSubscriptionAttributes(String subscriptionArn) throws Exception {
		
		CNSSubscriptionAttributes subscriptionAttributes = null;
		CmbColumnSlice<String, String> slice = cassandraHandler.readColumnSlice(AbstractCassandraPersistence.CNS_KEYSPACE, columnFamilySubscriptionAttributes, subscriptionArn, null, null, 10, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		
		if (slice != null) {
			
			subscriptionAttributes = new CNSSubscriptionAttributes();
			
			if (slice.getColumnByName("confirmationWasAuthenticated") != null) {
				subscriptionAttributes.setConfirmationWasAuthenticated(Boolean.getBoolean(slice.getColumnByName("confirmationWasAuthenticated").getValue()));
			}
			
			if (slice.getColumnByName("deliveryPolicy") != null) {
				subscriptionAttributes.setDeliveryPolicy(new CNSSubscriptionDeliveryPolicy(new JSONObject(slice.getColumnByName("deliveryPolicy").getValue())));
			}
			
			// if "ignore subscription override" is checked, get effective delivery policy from topic delivery policy, otherwise 
			// get effective delivery policy from subscription delivery policy
			
			CNSSubscription subscription = PersistenceFactory.getSubscriptionPersistence().getSubscription(subscriptionArn);
			
			if (subscription == null) {
				throw new SubscriberNotFoundException("Subscription not found. arn=" + subscriptionArn);
			}
			
			CNSTopicAttributes topicAttributes = getTopicAttributes(subscription.getTopicArn());
			
			if (topicAttributes != null) {
				
				CNSTopicDeliveryPolicy topicEffectiveDeliveryPolicy = topicAttributes.getEffectiveDeliveryPolicy();
				
				if (topicEffectiveDeliveryPolicy != null) {
					
					if (topicEffectiveDeliveryPolicy.isDisableSubscriptionOverrides() || subscriptionAttributes.getDeliveryPolicy() == null) {
						CNSSubscriptionDeliveryPolicy effectiveDeliveryPolicy = new CNSSubscriptionDeliveryPolicy();
						effectiveDeliveryPolicy.setHealthyRetryPolicy(topicEffectiveDeliveryPolicy.getDefaultHealthyRetryPolicy());
						effectiveDeliveryPolicy.setSicklyRetryPolicy(topicEffectiveDeliveryPolicy.getDefaultSicklyRetryPolicy());
						effectiveDeliveryPolicy.setThrottlePolicy(topicEffectiveDeliveryPolicy.getDefaultThrottlePolicy());
						subscriptionAttributes.setEffectiveDeliveryPolicy(effectiveDeliveryPolicy);
					} else {
						subscriptionAttributes.setEffectiveDeliveryPolicy(subscriptionAttributes.getDeliveryPolicy());
					}
				}
			}
			
			if (slice.getColumnByName("topicArn") != null) {
				subscriptionAttributes.setTopicArn(slice.getColumnByName("topicArn").getValue());
			}

			if (slice.getColumnByName("userId") != null) {
				subscriptionAttributes.setUserId(slice.getColumnByName("userId").getValue());
			}
			
			subscriptionAttributes.setSubscriptionArn(subscriptionArn);
		}
		
		return subscriptionAttributes;
	}
}
