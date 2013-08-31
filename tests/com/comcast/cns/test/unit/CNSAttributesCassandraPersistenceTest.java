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

import java.util.Random;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cns.model.CNSRetryPolicy;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.cns.model.CNSThrottlePolicy;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.model.CNSTopicDeliveryPolicy;
import com.comcast.cns.model.CNSRetryPolicy.CnsBackoffFunction;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.persistence.CNSAttributesCassandraPersistence;
import com.comcast.cns.persistence.CNSSubscriptionCassandraPersistence;
import com.comcast.cns.persistence.CNSTopicCassandraPersistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class CNSAttributesCassandraPersistenceTest {

    private User user;
    private CNSTopicCassandraPersistence topicHandler;
    private CNSSubscriptionCassandraPersistence subscriptionHandler;
    private Random rand = new Random();
    private CNSTopic topic = null;
    private CNSSubscription subscription = null;
	private CNSAttributesCassandraPersistence attributeHandler = null;

    @Before
    public void setup() throws Exception {

    	Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();

        IUserPersistence userHandler = new UserCassandraPersistence();
        user = userHandler.getUserByName("cqs_unit_test");

        if (user == null) {
            user =  userHandler.createUser("cqs_unit_test", "cqs_unit_test");
        }

		topicHandler = new CNSTopicCassandraPersistence();
		subscriptionHandler = new CNSSubscriptionCassandraPersistence();
		attributeHandler = new CNSAttributesCassandraPersistence();

		String topicName = "T" + rand.nextLong();
		topic = topicHandler.createTopic(topicName, topicName, user.getUserId());
		subscription = subscriptionHandler.subscribe(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextLong(), CnsSubscriptionProtocol.http, topic.getArn(), user.getUserId());
    }
	
    @After    
    public void tearDown() {

    	try {
			topicHandler.deleteTopic(topic.getArn());
		} catch (Exception e) {
		}
    	
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }

	@Test
	public void testSetGetTopicAttributes() throws Exception {
		
		CNSTopicAttributes topicAttributes = new CNSTopicAttributes();
		topicAttributes.setTopicArn(topic.getArn());
		topicAttributes.setUserId(user.getUserId());

		CNSTopicDeliveryPolicy deliveryPolicy = new CNSTopicDeliveryPolicy();
		CNSRetryPolicy defaultHealthyRetryPolicy = new CNSRetryPolicy();
		defaultHealthyRetryPolicy.setNumRetries(6);
		defaultHealthyRetryPolicy.setMaxDelayTarget(20);
		defaultHealthyRetryPolicy.setMinDelayTarget(20);
		defaultHealthyRetryPolicy.setNumMaxDelayRetries(3);
		defaultHealthyRetryPolicy.setNumMinDelayRetries(1);
		defaultHealthyRetryPolicy.setNumNoDelayRetries(2);
		defaultHealthyRetryPolicy.setBackOffFunction(CnsBackoffFunction.linear);
		deliveryPolicy.setDefaultHealthyRetryPolicy(defaultHealthyRetryPolicy );
		topicAttributes.setDeliveryPolicy(deliveryPolicy);

		attributeHandler.setTopicAttributes(topicAttributes , topic.getArn());

		CNSTopicAttributes topicAttributes2 = attributeHandler.getTopicAttributes(topic.getArn());

		assertEquals("Topic attributes do not match", topicAttributes2.getDeliveryPolicy().toString(), deliveryPolicy.toString());
		assertEquals("Topic attributes do not match", topicAttributes2.getEffectiveDeliveryPolicy().toString(), deliveryPolicy.toString());
		assertEquals("Topic attributes do not match", topicAttributes2.getSubscriptionsConfirmed(), 0);
		assertEquals("Topic attributes do not match", topicAttributes2.getSubscriptionsDeleted(), 0);
		assertEquals("Topic attributes do not match", topicAttributes2.getTopicArn(), topic.getArn());
		assertEquals("Topic attributes do not match", topicAttributes2.getUserId(), user.getUserId());
	}
	
	@Test
	public void testSetGetSubscriptionAttributes() throws Exception {
		
		CNSSubscriptionAttributes subscriptionAttributes = new CNSSubscriptionAttributes();

		subscriptionAttributes.setSubscriptionArn(subscription.getArn());
		subscriptionAttributes.setTopicArn(subscription.getTopicArn());
		subscriptionAttributes.setUserId(user.getUserId());
		
		CNSSubscriptionDeliveryPolicy deliveryPolicy = new CNSSubscriptionDeliveryPolicy();
		CNSRetryPolicy healthyRetryPolicy = new CNSRetryPolicy();
		healthyRetryPolicy.setBackOffFunction(CnsBackoffFunction.arithmetic);
		healthyRetryPolicy.setMaxDelayTarget(21);
		healthyRetryPolicy.setMinDelayTarget(19);
		healthyRetryPolicy.setNumMaxDelayRetries(1);
		healthyRetryPolicy.setNumMinDelayRetries(0);
		healthyRetryPolicy.setNumNoDelayRetries(2);
		healthyRetryPolicy.setNumRetries(97);
		deliveryPolicy.setHealthyRetryPolicy(healthyRetryPolicy);				
		CNSThrottlePolicy throttlePolicy = new CNSThrottlePolicy();
		throttlePolicy.setMaxReceivesPerSecond(2);
		deliveryPolicy.setThrottlePolicy(throttlePolicy);
		subscriptionAttributes.setDeliveryPolicy(deliveryPolicy);				
		
		attributeHandler.setSubscriptionAttributes(subscriptionAttributes , subscription.getArn());
		
		CNSSubscriptionAttributes subscriptionAttributes2 = attributeHandler.getSubscriptionAttributes(subscription.getArn());
		
		assertEquals("Subscription attributes do not match", subscriptionAttributes2.getDeliveryPolicy().toString(), deliveryPolicy.toString());
		assertEquals("Subscription attributes do not match", subscriptionAttributes2.getEffectiveDeliveryPolicy().toString(), deliveryPolicy.toString());
		assertEquals("Subscription attributes do not match", subscriptionAttributes2.getSubscriptionArn(), subscriptionAttributes.getSubscriptionArn());
		assertEquals("Subscription attributes do not match", subscriptionAttributes2.getTopicArn(), subscriptionAttributes.getTopicArn());
		assertEquals("Subscription attributes do not match", subscriptionAttributes2.getUserId(), subscriptionAttributes.getUserId());
	}
}
