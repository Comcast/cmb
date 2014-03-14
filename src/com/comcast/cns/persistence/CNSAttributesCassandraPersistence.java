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

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;

import com.comcast.cmb.common.persistence.AbstractCassandraPersistence;
import com.comcast.cmb.common.persistence.CassandraPersistenceFactory;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
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
	private static final String KEYSPACE = CMBProperties.getInstance().getCNSKeyspace();
	private static final AbstractCassandraPersistence cassandraHandler = CassandraPersistenceFactory.getInstance(KEYSPACE);
	
	public CNSAttributesCassandraPersistence() {
	}

	@Override
	public void setTopicAttributes(CNSTopicAttributes topicAttributes, String topicArn) throws Exception {
		
		cassandraHandler.insertOrUpdateRow(topicArn, columnFamilyTopicAttributes, getColumnValues(topicAttributes), CMBProperties.getInstance().getWriteConsistencyLevel());
		
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
		
		ColumnSlice<String, String> slice = cassandraHandler.readColumnSlice(columnFamilyTopicAttributes, topicArn, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		
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
		
		long subscriptionConfirmedCount = cassandraHandler.getCounter(columnFamilyTopicStats, topicArn, "subscriptionConfirmed", StringSerializer.get(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		topicAttributes.setSubscriptionsConfirmed(subscriptionConfirmedCount);
		
		long subscriptionPendingCount = cassandraHandler.getCounter(columnFamilyTopicStats, topicArn, "subscriptionPending", StringSerializer.get(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		topicAttributes.setSubscriptionsPending(subscriptionPendingCount);
		
		long subscriptionDeletedCount = cassandraHandler.getCounter(columnFamilyTopicStats, topicArn, "subscriptionDeleted", StringSerializer.get(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		topicAttributes.setSubscriptionsDeleted(subscriptionDeletedCount);

		return topicAttributes;
	}

	@Override
	public void setSubscriptionAttributes(CNSSubscriptionAttributes subscriptionAtributes, String subscriptionArn) throws Exception {
		cassandraHandler.insertOrUpdateRow(subscriptionArn, columnFamilySubscriptionAttributes, getColumnValues(subscriptionAtributes), CMBProperties.getInstance().getWriteConsistencyLevel());
		String topicArn = com.comcast.cns.util.Util.getCnsTopicArn(subscriptionArn);
		CNSCache.removeTopicAttributes(topicArn);
	}

	private Map<String, String> getColumnValues(CNSSubscriptionAttributes subscriptionAtributes) {
		
		Map<String, String> colVals = new HashMap<String, String>();
		
		if (subscriptionAtributes.getDeliveryPolicy() != null) {
			colVals.put("deliveryPolicy", subscriptionAtributes.getDeliveryPolicy().toString());
		}
		
		
		// currently only accept delivery policy parameter, fill up with defaults if elements are missing and return as both delivery policy and effective delivery policy
		
		/*if (subscriptionAtributes.getEffectiveDeliveryPolicy() != null) {
			colVals.put("effectiveDeliveryPolicy", subscriptionAtributes.getEffectiveDeliveryPolicy().toString());
		}*/
		
		if (subscriptionAtributes.getSubscriptionArn() != null) {
			colVals.put("subscriptionArn", subscriptionAtributes.getSubscriptionArn());
		}
		
		if (subscriptionAtributes.getTopicArn() != null) {
			colVals.put("topicArn", subscriptionAtributes.getTopicArn());
		}
		
		if (subscriptionAtributes.getUserId() != null) {
			colVals.put("userId", subscriptionAtributes.getUserId());
		}
		
		return colVals;
	}

	@Override
	public CNSSubscriptionAttributes getSubscriptionAttributes(String subscriptionArn) throws Exception {
		
		CNSSubscriptionAttributes subscriptionAttributes = null;
		ColumnSlice<String, String> slice = cassandraHandler.readColumnSlice(columnFamilySubscriptionAttributes, subscriptionArn, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		
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
