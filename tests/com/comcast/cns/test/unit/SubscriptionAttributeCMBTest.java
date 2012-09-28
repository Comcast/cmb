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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.cmb.test.tools.SubscriptionAttributeParser;
import com.comcast.cns.controller.CNSControllerServlet;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;

public class SubscriptionAttributeCMBTest {
	
	private static Logger logger = Logger.getLogger(SubscriptionAttributeCMBTest.class);

	private User user;
	private User user2;
	private User user3;

	private String userName1 = "cns_unit_test_1";
	private String userName2 = "cns_unit_test_2";
	private String userName3 = "cns_unit_test_3";
	
	private Random rand = new Random();

	@Before
	public void setup() {

		try {

			Util.initLog4jTest();
			CMBControllerServlet.valueAccumulator.initializeAllCounters();
			PersistenceFactory.reset();
			
			IUserPersistence userHandler = PersistenceFactory.getUserPersistence();

			user = userHandler.getUserByName(userName1);

			if (user == null) {
				user =  userHandler.createUser(userName1, userName1);
			}

			user2 = userHandler.getUserByName(userName2);

			if (user2 == null) {
				user2 =  userHandler.createUser(userName2, userName2);
			}

			user3 = userHandler.getUserByName(userName3);
			
			if (user3 == null) {
				user3 =  userHandler.createUser(userName3, userName3);
			}

		} catch (Exception ex) {
			logger.error("setup failed", ex);
			assertFalse(true);
		}
	}

	@After
	public void tearDown() {
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}

	@Test
	public void testGetAttributes() {

		try {
			
			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user, out, topicName);
			String res = out.toString();
			String topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = "http://nis.test3.plaxo.com:8080/CMB/Endpoint/recv/" + rand.nextInt();
			String protocol = "http";
			CNSTestingUtils.subscribe(cns, user, out, endpoint, protocol, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			if (subscriptionArn.equals("pending confirmation")) {

				List<CNSSubscription> confirmedSubscriptions = CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user.getUserId(), CnsSubscriptionProtocol.valueOf(protocol));

				if (confirmedSubscriptions.size() == 1) {
					subscriptionArn = confirmedSubscriptions.get(0).getArn();
				}
			}

			CNSTestingUtils.doGetSubscriptionAttributes(cns, user, out, subscriptionArn);
			res = out.toString();
			out.reset();
			
			logger.debug("getSubscription Attributes: " + res);
			
			SubscriptionAttributeParser attr = CNSTestingUtils.getSubscriptionAttributesFromString(res);
			
			assertTrue(attr.getOwner().equals(user.getUserId()));

			JSONObject effectiveDeliveryPolicy = new JSONObject(attr.getEffectiveDeliveryPolicy());
			
			assertTrue(effectiveDeliveryPolicy.has("healthyRetryPolicy"));
			
			JSONObject healthyRetryPolicy = effectiveDeliveryPolicy.getJSONObject("healthyRetryPolicy");				
			
			assertTrue(healthyRetryPolicy != null);
			assertTrue(healthyRetryPolicy.getInt("numRetries") == 3);
			assertTrue(healthyRetryPolicy.getInt("maxDelayTarget") == 20);
			assertTrue(healthyRetryPolicy.getInt("minDelayTarget") == 20);
			assertTrue(healthyRetryPolicy.getInt("numMaxDelayRetries") == 0);
			assertTrue(healthyRetryPolicy.getString("backoffFunction").equals("linear"));
			assertTrue(healthyRetryPolicy.getInt("numMinDelayRetries") == 0);
			assertTrue(healthyRetryPolicy.getInt("numNoDelayRetries") == 0);
			assertTrue(effectiveDeliveryPolicy.has("sicklyRetryPolicy"));
			assertTrue(effectiveDeliveryPolicy.get("sicklyRetryPolicy") == JSONObject.NULL);
			assertTrue(effectiveDeliveryPolicy.has("throttlePolicy"));

			JSONObject throttlePolicy = effectiveDeliveryPolicy.getJSONObject("throttlePolicy");								
			
			assertTrue(throttlePolicy.get("maxReceivesPerSecond") == JSONObject.NULL);
			assertTrue(attr.getConfirmationWasAuthenticated().equals("false"));
			assertTrue(attr.getTopicArn().equals(topicArn));
			assertTrue(attr.getSubscriptionArn().equals(subscriptionArn));
			
			CNSTestingUtils.deleteTopic(cns, user, out, topicArn);

		} catch (Exception e) {
			logger.error("Exception occured", e);
			fail("exception: "+e);
		}
	}

	@Test
	public void testSetGetAttributes() {

		try {

			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user, out, topicName);
			String res = out.toString();
			String topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = "http://nis.test3.plaxo.com:8080/CMB/Endpoint/recv/" + rand.nextInt();
			String protocol = "http";
			CNSTestingUtils.subscribe(cns, user, out, endpoint, protocol, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			if (subscriptionArn.equals("pending confirmation")) {

				List<CNSSubscription> confirmedSubscriptions = CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user.getUserId(), CnsSubscriptionProtocol.valueOf(protocol));

				if (confirmedSubscriptions.size() == 1) {
					subscriptionArn = confirmedSubscriptions.get(0).getArn();
				}
			}

			String attributeName = "DeliveryPolicy";
			String attributeValue = "{\"healthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":12,"+
			"\"maxDelayTarget\":13,"+
			"\"numRetries\":43,"+
			"\"numMaxDelayRetries\": 23,"+
			"\"numMinDelayRetries\": 20,"+
			"\"backoffFunction\": \"arithmetic\""+
			"}," +
			"\"throttlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":7" +
			"}," +
			"\"sicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":15,"+
			"\"numMinDelayRetries\": 4,"+
			"\"numMaxDelayRetries\": 5,"+
			"\"numNoDelayRetries\":6,"+
			"\"backoffFunction\": \"exponential\""+
			"}" +
			"}";

			CNSTestingUtils.doSetSubscriptionAttributes(cns, user, out, attributeName, attributeValue, subscriptionArn);
			out.reset();

			CNSTestingUtils.doGetSubscriptionAttributes(cns, user, out, subscriptionArn);
			res = out.toString();
			out.reset();
			
			logger.debug("getSubscription Attributes: " + res);
			
			SubscriptionAttributeParser attributeParser = CNSTestingUtils.getSubscriptionAttributesFromString(res);
			
			assertTrue(attributeParser.getOwner().equals(user.getUserId()));

			JSONObject effectiveDeliveryPolicy = new JSONObject(attributeParser.getEffectiveDeliveryPolicy());
			
			assertTrue(effectiveDeliveryPolicy.has("healthyRetryPolicy"));
			
			JSONObject healthyRetryPoliocy = effectiveDeliveryPolicy.getJSONObject("healthyRetryPolicy");				
			
			assertTrue(healthyRetryPoliocy != null);
			assertTrue(healthyRetryPoliocy.getInt("numRetries") == 43);
			assertTrue(healthyRetryPoliocy.getInt("minDelayTarget") == 12);
			assertTrue(healthyRetryPoliocy.getInt("maxDelayTarget") == 13);
			assertTrue(healthyRetryPoliocy.getInt("numMinDelayRetries") == 20);
			assertTrue(healthyRetryPoliocy.getInt("numMaxDelayRetries") == 23);
			assertTrue(healthyRetryPoliocy.getInt("numNoDelayRetries") == 0);
			assertTrue(healthyRetryPoliocy.getString("backoffFunction").equals("arithmetic"));

			assertTrue(effectiveDeliveryPolicy.has("sicklyRetryPolicy"));

			JSONObject sicklyRetryPolicy = effectiveDeliveryPolicy.getJSONObject("sicklyRetryPolicy");	
			
			assertTrue(sicklyRetryPolicy != null);
			assertTrue(sicklyRetryPolicy.getInt("numRetries") == 15);
			assertTrue(sicklyRetryPolicy.getInt("minDelayTarget") == 2);
			assertTrue(sicklyRetryPolicy.getInt("maxDelayTarget") == 3);
			assertTrue(sicklyRetryPolicy.getInt("numMinDelayRetries") == 4);
			assertTrue(sicklyRetryPolicy.getInt("numMaxDelayRetries") == 5);
			assertTrue(sicklyRetryPolicy.getInt("numNoDelayRetries") == 6);
			assertTrue(sicklyRetryPolicy.getString("backoffFunction").equals("exponential"));

			assertTrue(effectiveDeliveryPolicy.has("throttlePolicy"));

			JSONObject throttlePolicy = effectiveDeliveryPolicy.getJSONObject("throttlePolicy");								
			
			assertTrue(throttlePolicy.getInt("maxReceivesPerSecond") == 7);
			assertTrue(attributeParser.getConfirmationWasAuthenticated().equals("false"));

			JSONObject deliveryPolicy = new JSONObject(attributeParser.getDeliveryPolicy());
			
			assertTrue(deliveryPolicy.has("healthyRetryPolicy"));
			
			JSONObject healthyRetryPolicy = deliveryPolicy.getJSONObject("healthyRetryPolicy");				
			
			assertTrue(healthyRetryPolicy != null);
			assertTrue(healthyRetryPolicy.getInt("numRetries") == 43);
			assertTrue(healthyRetryPolicy.getInt("minDelayTarget") == 12);
			assertTrue(healthyRetryPolicy.getInt("maxDelayTarget") == 13);
			assertTrue(healthyRetryPolicy.getInt("numMinDelayRetries") == 20);
			assertTrue(healthyRetryPolicy.getInt("numMaxDelayRetries") == 23);
			assertTrue(healthyRetryPolicy.getInt("numNoDelayRetries") == 0);
			assertTrue(healthyRetryPolicy.getString("backoffFunction").equals("arithmetic"));

			assertTrue(deliveryPolicy.has("sicklyRetryPolicy"));
			assertTrue(deliveryPolicy.has("throttlePolicy"));

			throttlePolicy = deliveryPolicy.getJSONObject("throttlePolicy");		
			
			assertTrue(throttlePolicy.getInt("maxReceivesPerSecond") == 7);

			sicklyRetryPolicy = deliveryPolicy.getJSONObject("sicklyRetryPolicy");	
			
			assertTrue(sicklyRetryPolicy != null);
			assertTrue(sicklyRetryPolicy.getInt("numRetries") == 15);
			assertTrue(sicklyRetryPolicy.getInt("minDelayTarget") == 2);
			assertTrue(sicklyRetryPolicy.getInt("maxDelayTarget") == 3);
			assertTrue(sicklyRetryPolicy.getInt("numMinDelayRetries") == 4);
			assertTrue(sicklyRetryPolicy.getInt("numMaxDelayRetries") == 5);
			assertTrue(sicklyRetryPolicy.getInt("numNoDelayRetries") == 6);
			assertTrue(sicklyRetryPolicy.getString("backoffFunction").equals("exponential"));

			assertTrue(attributeParser.getTopicArn().equals(topicArn));
			assertTrue(attributeParser.getSubscriptionArn().equals(subscriptionArn));
			
			CNSTestingUtils.deleteTopic(cns, user, out, topicArn);

		} catch (Exception e) {
			logger.error("Exception occured", e);
			fail("exception: "+e);
		}
	}

	@Test
	public void testSetGetAttributes2() {
		
		try {
			
			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user, out, topicName);
			String res = out.toString();
			String topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();
			
			CNSTestingUtils.addPermission(cns, user, out, topicArn, "*", "*", rand.nextLong()+"");
			out.reset();
			
			String endpoint = "http://nis.test3.plaxo.com:8080/CMB/Endpoint/recv/" + rand.nextInt();
			String protocol = "http";
			CNSTestingUtils.subscribe(cns, user2, out, endpoint, protocol, topicArn);
			out.reset();
			String lastMessageUrl = endpoint.replace("recv", "info") + "?showLast=true";
			String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");
			out.reset();
			JSONObject js = new JSONObject(resp);
			String token = (String) js.get("Token");	
			String authOnSub = "false";
			CNSTestingUtils.confirmSubscription(cns, user2, out, topicArn, token, authOnSub);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String endpoint2 = "http://nis.test3.plaxo.com:8080/CMB/Endpoint/recv/" + rand.nextInt();
			String protocol2 = "http";
			CNSTestingUtils.subscribe(cns, user3, out, endpoint2, protocol2, topicArn);
			out.reset();
			String lastMessageUrl2 = endpoint2.replace("recv", "info") + "?showLast=true";
			resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl2, "");
			out.reset();
			
			js = new JSONObject(resp);
			token = (String) js.get("Token");	
			authOnSub = "false";
			CNSTestingUtils.confirmSubscription(cns, user3, out, topicArn, token, authOnSub);
			String subscriptionArn2  = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String attributeName = "DeliveryPolicy";
			String attributeValue = "{" +
			"\"http\": {" +
			"\"defaultHealthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":10,"+
			"\"maxDelayTarget\":10,"+
			"\"numRetries\":30,"+
			"\"numMaxDelayRetries\": 11,"+
			"\"numMinDelayRetries\": 11,"+
			"\"backoffFunction\": \"geometric\""+
			"}," +
			"\"disableSubscriptionOverrides\": false," +
			"\"defaultThrottlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":3" +
			"}," +
			"\"defaultSicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":1,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":16,"+
			"\"numMinDelayRetries\": 1,"+
			"\"numMaxDelayRetries\": 3,"+
			"\"numNoDelayRetries\":3,"+
			"\"backoffFunction\": \"exponential\""+
			"}" +
			"}" +
			"}";

			CNSTestingUtils.doSetTopicAttributes(cns, user, out, attributeName, attributeValue, topicArn);
			out.reset();

			String attributeName2 = "DeliveryPolicy";
			String attributeValue2 = "{\"healthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":12,"+
			"\"maxDelayTarget\":13,"+
			"\"numRetries\":43,"+
			"\"numMaxDelayRetries\": 23,"+
			"\"numMinDelayRetries\": 20,"+
			"\"backoffFunction\": \"arithmetic\""+
			"}," +
			"\"throttlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":7" +
			"}," +
			"\"sicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":15,"+
			"\"numMinDelayRetries\": 4,"+
			"\"numMaxDelayRetries\": 5,"+
			"\"numNoDelayRetries\":6,"+
			"\"backoffFunction\": \"exponential\""+
			"}" +
			"}";

			CNSTestingUtils.doSetSubscriptionAttributes(cns, user2, out, attributeName2, attributeValue2, subscriptionArn);
			out.reset();

			CNSTestingUtils.doGetSubscriptionAttributes(cns, user3, out, subscriptionArn2);
			res = out.toString();
			out.reset();

			{
				logger.debug("getSubscription Attributes: " + res);
				
				SubscriptionAttributeParser attributeParser = CNSTestingUtils.getSubscriptionAttributesFromString(res);
				assertTrue(attributeParser.getOwner().equals(user3.getUserId()));

				JSONObject effectiveDeliveryPolicy = new JSONObject(attributeParser.getEffectiveDeliveryPolicy());
				assertTrue(effectiveDeliveryPolicy.has("healthyRetryPolicy"));
				JSONObject healthyRetryPolicy = effectiveDeliveryPolicy.getJSONObject("healthyRetryPolicy");				
				assertTrue(healthyRetryPolicy != null);
				assertTrue("healthyRetryPolicy.numRetries returns incorrect value", healthyRetryPolicy.getInt("numRetries") == 30);
				assertTrue("healthyRetryPolicy.minDelayTarget returns incorrect value", healthyRetryPolicy.getInt("minDelayTarget") == 10);
				assertTrue("healthyRetryPolicy.maxDelayTarget returns incorrect value", healthyRetryPolicy.getInt("maxDelayTarget") == 10);
				assertTrue("healthyRetryPolicy.numMinDelayRetries returns incorrect value", healthyRetryPolicy.getInt("numMinDelayRetries") == 11);
				assertTrue("healthyRetryPolicy.numMaxDelayRetries returns incorrect value", healthyRetryPolicy.getInt("numMaxDelayRetries") == 11);
				assertTrue("healthyRetryPolicy.numNoDelayRetries returns incorrect value", healthyRetryPolicy.getInt("numNoDelayRetries") == 0);
				assertTrue("healthyRetryPolicy.backoffFunction returns incorrect value", healthyRetryPolicy.getString("backoffFunction").equals("geometric"));

				assertTrue(effectiveDeliveryPolicy.has("sicklyRetryPolicy"));
				JSONObject sicklyRetryPolicy = effectiveDeliveryPolicy.getJSONObject("sicklyRetryPolicy");	
				assertTrue(sicklyRetryPolicy != null);
				assertTrue("sicklyRetryPolicy.numRetries returns incorrect value", sicklyRetryPolicy.getInt("numRetries") == 16);
				assertTrue("sicklyRetryPolicy.minDelayTarget returns incorrect value", sicklyRetryPolicy.getInt("minDelayTarget") == 1);
				assertTrue("sicklyRetryPolicy.maxDelayTarget returns incorrect value", sicklyRetryPolicy.getInt("maxDelayTarget") == 3);
				assertTrue("sicklyRetryPolicy.numMinDelayRetries returns incorrect value", sicklyRetryPolicy.getInt("numMinDelayRetries") == 1);
				assertTrue("sicklyRetryPolicy.numMaxDelayRetries returns incorrect value", sicklyRetryPolicy.getInt("numMaxDelayRetries") == 3);
				assertTrue("sicklyRetryPolicy.numNoDelayRetries returns incorrect value", sicklyRetryPolicy.getInt("numNoDelayRetries") == 3);
				assertTrue("sicklyRetryPolicy.backoffFunction returns incorrect value", sicklyRetryPolicy.getString("backoffFunction").equals("exponential"));

				assertTrue(effectiveDeliveryPolicy.has("throttlePolicy"));
				JSONObject throttlePolicy = effectiveDeliveryPolicy.getJSONObject("throttlePolicy");								
				assertTrue(throttlePolicy.getInt("maxReceivesPerSecond") == 3);

				assertTrue(attributeParser.getConfirmationWasAuthenticated().equals("false"));

				JSONObject deliveryPolicy = new JSONObject(attributeParser.getDeliveryPolicy());
				assertTrue(deliveryPolicy.has("healthyRetryPolicy"));
				healthyRetryPolicy = deliveryPolicy.getJSONObject("healthyRetryPolicy");				
				assertTrue(healthyRetryPolicy != null);
				assertTrue("healthyRetryPolicy.numRetries returns incorrect value", healthyRetryPolicy.getInt("numRetries") == 30);
				assertTrue("healthyRetryPolicy.minDelayTarget returns incorrect value", healthyRetryPolicy.getInt("minDelayTarget") == 10);
				assertTrue("healthyRetryPolicy.maxDelayTarget returns incorrect value", healthyRetryPolicy.getInt("maxDelayTarget") == 10);
				assertTrue("healthyRetryPolicy.numMinDelayRetries returns incorrect value", healthyRetryPolicy.getInt("numMinDelayRetries") == 11);
				assertTrue("healthyRetryPolicy.numMaxDelayRetries returns incorrect value", healthyRetryPolicy.getInt("numMaxDelayRetries") == 11);
				assertTrue("healthyRetryPolicy.numNoDelayRetries returns incorrect value", healthyRetryPolicy.getInt("numNoDelayRetries") == 0);
				assertTrue("healthyRetryPolicy.backoffFunction returns incorrect value", healthyRetryPolicy.getString("backoffFunction").equals("geometric"));

				assertTrue(deliveryPolicy.has("sicklyRetryPolicy"));

				assertTrue(deliveryPolicy.has("throttlePolicy"));
				throttlePolicy = deliveryPolicy.getJSONObject("throttlePolicy");		
				assertTrue(throttlePolicy.getInt("maxReceivesPerSecond") == 3);

				sicklyRetryPolicy = deliveryPolicy.getJSONObject("sicklyRetryPolicy");	
				assertTrue(sicklyRetryPolicy != null);
				assertTrue("sicklyRetryPolicy.numRetries returns incorrect value", sicklyRetryPolicy.getInt("numRetries") == 16);
				assertTrue("sicklyRetryPolicy.minDelayTarget returns incorrect value", sicklyRetryPolicy.getInt("minDelayTarget") == 1);
				assertTrue("sicklyRetryPolicy.maxDelayTarget returns incorrect value", sicklyRetryPolicy.getInt("maxDelayTarget") == 3);
				assertTrue("sicklyRetryPolicy.numMinDelayRetries returns incorrect value", sicklyRetryPolicy.getInt("numMinDelayRetries") == 1);
				assertTrue("sicklyRetryPolicy.numMaxDelayRetries returns incorrect value", sicklyRetryPolicy.getInt("numMaxDelayRetries") == 3);
				assertTrue("sicklyRetryPolicy.numNoDelayRetries returns incorrect value", sicklyRetryPolicy.getInt("numNoDelayRetries") == 3);
				assertTrue("sicklyRetryPolicy.backoffFunction returns incorrect value", sicklyRetryPolicy.getString("backoffFunction").equals("exponential"));

				assertTrue(attributeParser.getTopicArn().equals(topicArn));
				assertTrue(attributeParser.getSubscriptionArn().equals(subscriptionArn2));
			}

			CNSTestingUtils.doGetSubscriptionAttributes(cns, user2, out, subscriptionArn);
			res = out.toString();
			out.reset();

			{
				logger.debug("getSubscription Attributes: " + res);
				SubscriptionAttributeParser attr = CNSTestingUtils.getSubscriptionAttributesFromString(res);
				assertTrue(attr.getOwner().equals(user2.getUserId()));

				JSONObject effectiveDeliveryPolicy = new JSONObject(attr.getEffectiveDeliveryPolicy());
				assertTrue(effectiveDeliveryPolicy.has("healthyRetryPolicy"));
				JSONObject healthyRetryPolicy = effectiveDeliveryPolicy.getJSONObject("healthyRetryPolicy");				
				assertTrue(healthyRetryPolicy != null);
				assertTrue(healthyRetryPolicy.getInt("numRetries") == 43);
				assertTrue(healthyRetryPolicy.getInt("minDelayTarget") == 12);
				assertTrue(healthyRetryPolicy.getInt("maxDelayTarget") == 13);
				assertTrue(healthyRetryPolicy.getInt("numMinDelayRetries") == 20);
				assertTrue(healthyRetryPolicy.getInt("numMaxDelayRetries") == 23);
				assertTrue(healthyRetryPolicy.getInt("numNoDelayRetries") == 0);
				assertTrue(healthyRetryPolicy.getString("backoffFunction").equals("arithmetic"));

				assertTrue(effectiveDeliveryPolicy.has("sicklyRetryPolicy"));
				JSONObject sicklyRetryPolicy = effectiveDeliveryPolicy.getJSONObject("sicklyRetryPolicy");	
				assertTrue(sicklyRetryPolicy != null);
				assertTrue(sicklyRetryPolicy.getInt("numRetries") == 15);
				assertTrue(sicklyRetryPolicy.getInt("minDelayTarget") == 2);
				assertTrue(sicklyRetryPolicy.getInt("maxDelayTarget") == 3);
				assertTrue(sicklyRetryPolicy.getInt("numMinDelayRetries") == 4);
				assertTrue(sicklyRetryPolicy.getInt("numMaxDelayRetries") == 5);
				assertTrue(sicklyRetryPolicy.getInt("numNoDelayRetries") == 6);
				assertTrue(sicklyRetryPolicy.getString("backoffFunction").equals("exponential"));

				assertTrue(effectiveDeliveryPolicy.has("throttlePolicy"));
				JSONObject throttlePolicy = effectiveDeliveryPolicy.getJSONObject("throttlePolicy");								
				assertTrue(throttlePolicy.getInt("maxReceivesPerSecond") == 7);

				assertTrue(attr.getConfirmationWasAuthenticated().equals("false"));

				JSONObject deliveryPolicy = new JSONObject(attr.getDeliveryPolicy());
				assertTrue(deliveryPolicy.has("healthyRetryPolicy"));
				healthyRetryPolicy = deliveryPolicy.getJSONObject("healthyRetryPolicy");				
				assertTrue(healthyRetryPolicy != null);
				assertTrue(healthyRetryPolicy.getInt("numRetries") == 43);
				assertTrue(healthyRetryPolicy.getInt("minDelayTarget") == 12);
				assertTrue(healthyRetryPolicy.getInt("maxDelayTarget") == 13);
				assertTrue(healthyRetryPolicy.getInt("numMinDelayRetries") == 20);
				assertTrue(healthyRetryPolicy.getInt("numMaxDelayRetries") == 23);
				assertTrue(healthyRetryPolicy.getInt("numNoDelayRetries") == 0);
				assertTrue(healthyRetryPolicy.getString("backoffFunction").equals("arithmetic"));

				assertTrue(deliveryPolicy.has("sicklyRetryPolicy"));

				assertTrue(deliveryPolicy.has("throttlePolicy"));
				throttlePolicy = deliveryPolicy.getJSONObject("throttlePolicy");		
				assertTrue(throttlePolicy.getInt("maxReceivesPerSecond") == 7);

				sicklyRetryPolicy = deliveryPolicy.getJSONObject("sicklyRetryPolicy");	
				assertTrue(sicklyRetryPolicy != null);
				assertTrue(sicklyRetryPolicy.getInt("numRetries") == 15);
				assertTrue(sicklyRetryPolicy.getInt("minDelayTarget") == 2);
				assertTrue(sicklyRetryPolicy.getInt("maxDelayTarget") == 3);
				assertTrue(sicklyRetryPolicy.getInt("numMinDelayRetries") == 4);
				assertTrue(sicklyRetryPolicy.getInt("numMaxDelayRetries") == 5);
				assertTrue(sicklyRetryPolicy.getInt("numNoDelayRetries") == 6);
				assertTrue(sicklyRetryPolicy.getString("backoffFunction").equals("exponential"));

				assertTrue(attr.getTopicArn().equals(topicArn));

				assertTrue(attr.getSubscriptionArn().equals(subscriptionArn));
			}
			
			CNSTestingUtils.deleteTopic(cns, user, out, topicArn);

		} catch (Exception e) {
			logger.error("Exception occured", e);
			fail("exception: "+e);
		}
	}

	@Test
	public void testSetGetAttributes3() {

		try {

			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user, out, topicName);
			String res = out.toString();
			String topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = "http://nis.test3.plaxo.com:8080/CMB/Endpoint/recv/" + rand.nextInt();
			String protocol = "http";
			CNSTestingUtils.subscribe(cns, user, out, endpoint, protocol, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			if (subscriptionArn.equals("pending confirmation")) {

				List<CNSSubscription> confirmedSubscriptions = CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user.getUserId(), CnsSubscriptionProtocol.valueOf(protocol));

				if (confirmedSubscriptions.size() == 1) {
					subscriptionArn = confirmedSubscriptions.get(0).getArn();
				}
			}

			String attributeName = "DeliveryPolicy";
			String attributeValue = "{" +
			"\"http\": {" +
			"\"defaultHealthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":10,"+
			"\"maxDelayTarget\":10,"+
			"\"numRetries\":30,"+
			"\"numMaxDelayRetries\": 11,"+
			"\"numMinDelayRetries\": 11,"+
			"\"backoffFunction\": \"geometric\""+
			"}," +
			"\"disableSubscriptionOverrides\": true," +
			"\"defaultThrottlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":3" +
			"}," +
			"\"defaultSicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":1,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":16,"+
			"\"numMinDelayRetries\": 1,"+
			"\"numMaxDelayRetries\": 3,"+
			"\"numNoDelayRetries\":3,"+
			"\"backoffFunction\": \"exponential\""+
			"}" +
			"}" +
			"}";

			CNSTestingUtils.doSetTopicAttributes(cns, user, out, attributeName, attributeValue, topicArn);
			out.reset();

			String attributeName2 = "DeliveryPolicy";
			String attributeValue2 = "{\"healthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":12,"+
			"\"maxDelayTarget\":13,"+
			"\"numRetries\":43,"+
			"\"numMaxDelayRetries\": 23,"+
			"\"numMinDelayRetries\": 20,"+
			"\"backoffFunction\": \"arithmetic\""+
			"}," +
			"\"throttlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":7" +
			"}," +
			"\"sicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":15,"+
			"\"numMinDelayRetries\": 4,"+
			"\"numMaxDelayRetries\": 5,"+
			"\"numNoDelayRetries\":6,"+
			"\"backoffFunction\": \"exponential\""+
			"}" +
			"}";

			CNSTestingUtils.doSetSubscriptionAttributes(cns, user, out, attributeName2, attributeValue2, subscriptionArn);
			out.reset();

			CNSTestingUtils.doGetSubscriptionAttributes(cns, user, out, subscriptionArn);
			res = out.toString();
			out.reset();
			
			logger.debug("getSubscription Attributes: " + res);
			
			SubscriptionAttributeParser attributeParser = CNSTestingUtils.getSubscriptionAttributesFromString(res);
			assertTrue(attributeParser.getOwner().equals(user.getUserId()));

			JSONObject effectiveDeliveryPolicy = new JSONObject(attributeParser.getEffectiveDeliveryPolicy());
			assertTrue(effectiveDeliveryPolicy.has("healthyRetryPolicy"));
			JSONObject healthyRetryPolicy = effectiveDeliveryPolicy.getJSONObject("healthyRetryPolicy");				
			assertTrue(healthyRetryPolicy != null);
			assertTrue(healthyRetryPolicy.getInt("numRetries") == 30);
			assertTrue(healthyRetryPolicy.getInt("minDelayTarget") == 10);
			assertTrue(healthyRetryPolicy.getInt("maxDelayTarget") == 10);
			assertTrue(healthyRetryPolicy.getInt("numMinDelayRetries") == 11);
			assertTrue(healthyRetryPolicy.getInt("numMaxDelayRetries") == 11);
			assertTrue(healthyRetryPolicy.getInt("numNoDelayRetries") == 0);
			assertTrue(healthyRetryPolicy.getString("backoffFunction").equals("geometric"));

			assertTrue(effectiveDeliveryPolicy.has("sicklyRetryPolicy"));
			JSONObject sicklyRetryPolicy = effectiveDeliveryPolicy.getJSONObject("sicklyRetryPolicy");	
			assertTrue(sicklyRetryPolicy != null);
			assertTrue(sicklyRetryPolicy.getInt("numRetries") == 16);
			assertTrue(sicklyRetryPolicy.getInt("minDelayTarget") == 1);
			assertTrue(sicklyRetryPolicy.getInt("maxDelayTarget") == 3);
			assertTrue(sicklyRetryPolicy.getInt("numMinDelayRetries") == 1);
			assertTrue(sicklyRetryPolicy.getInt("numMaxDelayRetries") == 3);
			assertTrue(sicklyRetryPolicy.getInt("numNoDelayRetries") == 3);
			assertTrue(sicklyRetryPolicy.getString("backoffFunction").equals("exponential"));

			assertTrue(effectiveDeliveryPolicy.has("throttlePolicy"));
			JSONObject throttlePolicy = effectiveDeliveryPolicy.getJSONObject("throttlePolicy");								
			assertTrue(throttlePolicy.getInt("maxReceivesPerSecond") == 3);

			assertTrue(attributeParser.getConfirmationWasAuthenticated().equals("false"));

			JSONObject deliveryPolicy = new JSONObject(attributeParser.getDeliveryPolicy());
			assertTrue(deliveryPolicy.has("healthyRetryPolicy"));
			healthyRetryPolicy = deliveryPolicy.getJSONObject("healthyRetryPolicy");				
			assertTrue(healthyRetryPolicy != null);
			assertTrue(healthyRetryPolicy.getInt("numRetries") == 43);
			assertTrue(healthyRetryPolicy.getInt("minDelayTarget") == 12);
			assertTrue(healthyRetryPolicy.getInt("maxDelayTarget") == 13);
			assertTrue(healthyRetryPolicy.getInt("numMinDelayRetries") == 20);
			assertTrue(healthyRetryPolicy.getInt("numMaxDelayRetries") == 23);
			assertTrue(healthyRetryPolicy.getInt("numNoDelayRetries") == 0);
			assertTrue(healthyRetryPolicy.getString("backoffFunction").equals("arithmetic"));

			assertTrue(deliveryPolicy.has("sicklyRetryPolicy"));
			assertTrue(deliveryPolicy.has("throttlePolicy"));

			throttlePolicy = deliveryPolicy.getJSONObject("throttlePolicy");		
			
			assertTrue(throttlePolicy.getInt("maxReceivesPerSecond") == 7);

			sicklyRetryPolicy = deliveryPolicy.getJSONObject("sicklyRetryPolicy");	
			assertTrue(sicklyRetryPolicy != null);
			assertTrue(sicklyRetryPolicy.getInt("numRetries") == 15);
			assertTrue(sicklyRetryPolicy.getInt("minDelayTarget") == 2);
			assertTrue(sicklyRetryPolicy.getInt("maxDelayTarget") == 3);
			assertTrue(sicklyRetryPolicy.getInt("numMinDelayRetries") == 4);
			assertTrue(sicklyRetryPolicy.getInt("numMaxDelayRetries") == 5);
			assertTrue(sicklyRetryPolicy.getInt("numNoDelayRetries") == 6);
			assertTrue(sicklyRetryPolicy.getString("backoffFunction").equals("exponential"));

			assertTrue(attributeParser.getTopicArn().equals(topicArn));
			assertTrue(attributeParser.getSubscriptionArn().equals(subscriptionArn));
			
			CNSTestingUtils.deleteTopic(cns, user, out, topicArn);

		} catch (Exception e) {
			logger.error("Exception occured", e);
			fail("exception: "+e);
		}
	}

	@Test
	public void testBadSetAttributes() {

		try {

			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user, out, topicName);
			String res = out.toString();
			String topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = "http://nis.test3.plaxo.com:8080/CMB/Endpoint/recv/" + rand.nextInt();
			String protocol = "http";
			CNSTestingUtils.subscribe(cns, user, out, endpoint, protocol, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			if (subscriptionArn.equals("pending confirmation")) {

				List<CNSSubscription> confirmedSubscriptions = CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user.getUserId(), CnsSubscriptionProtocol.valueOf(protocol));

				if (confirmedSubscriptions.size() == 1) {
					subscriptionArn = confirmedSubscriptions.get(0).getArn();
				}
			}

			String attributeName = "DeliveryPolicy";
			String attributeValue = null;

			CNSTestingUtils.doSetSubscriptionAttributes(cns, user, out, attributeName, attributeValue, subscriptionArn);
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(),"InvalidParameter", "missing parameters"));
			out.reset();

			attributeValue = "{\"healthyRetryPolicy\":" +				 
			"{"+
			"\"numRetries\":43,"+
			"\"numMaxDelayRetries\": 23,"+
			"\"numMinDelayRetries\": 20,"+
			"\"backoffFunction\": \"arithmetic\""+
			"}," +
			"\"throttlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":7" +
			"}," +
			"\"sicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numMinDelayRetries\": 4,"+
			"\"numMaxDelayRetries\": 5,"+
			"\"numNoDelayRetries\":6,"+
			"\"backoffFunction\": \"exponential\""+
			"}" +
			"}";

			CNSTestingUtils.doSetSubscriptionAttributes(cns, user, out, attributeName, attributeValue, subscriptionArn);
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(),"InvalidParameter", "DeliveryPolicy: healthyRetryPolicy.maxDelayTarget must be specified"));
			out.reset();

			attributeValue = "{\"healthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":5,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":43,"+
			"\"numMaxDelayRetries\": 23,"+
			"\"numMinDelayRetries\": 20,"+
			"\"backoffFunction\": \"arithmetic\""+
			"}," +
			"\"throttlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":7" +
			"}," +
			"\"sicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numMinDelayRetries\": 4,"+
			"\"numMaxDelayRetries\": 5,"+
			"\"numNoDelayRetries\":6,"+
			"\"backoffFunction\": \"exponential\""+
			"}" +
			"}";

			CNSTestingUtils.doSetSubscriptionAttributes(cns, user, out, attributeName, attributeValue, subscriptionArn);
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(),"InvalidParameter", "DeliveryPolicy: healthyRetryPolicy.maxDelayTarget must be greater than or equal to minDelayTarget"));
			out.reset();

			attributeValue = "{\"healthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":20,"+
			"\"numMaxDelayRetries\": 7,"+
			"\"numMinDelayRetries\": 7,"+
			"\"numNoDelayRetries\":7,"+
			"\"backoffFunction\": \"arithmetic\""+
			"}," +
			"\"throttlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":7" +
			"}," +
			"\"sicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numMinDelayRetries\": 4,"+
			"\"numMaxDelayRetries\": 5,"+
			"\"numNoDelayRetries\":6,"+
			"\"backoffFunction\": \"exponential\""+
			"}" +
			"}";

			CNSTestingUtils.doSetSubscriptionAttributes(cns, user, out, attributeName, attributeValue, subscriptionArn);
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(),"InvalidParameter", "DeliveryPolicy: healthyRetryPolicy.numRetries must be greater than or equal to total of numMinDelayRetries, numNoDelayRetries and numMaxDelayRetries"));
			out.reset();

			attributeValue = "{\"healthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":21,"+
			"\"numMaxDelayRetries\": 7,"+
			"\"numMinDelayRetries\": 7,"+
			"\"numNoDelayRetries\":7,"+
			"\"backoffFunction\": \"arithmetic\""+
			"}," +
			"\"throttlePolicy\":" +
			"{" +
			"\"maxReceivesPerSecond\":7" +
			"}," +
			"\"sicklyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":2,"+
			"\"maxDelayTarget\":3,"+
			"\"numRetries\":20,"+
			"\"numMaxDelayRetries\": 7,"+
			"\"numMinDelayRetries\": 7,"+
			"\"numNoDelayRetries\":7,"+
			"\"backoffFunction\": \"arithmetic\""+
			"}" +
			"}";

			CNSTestingUtils.doSetSubscriptionAttributes(cns, user, out, attributeName, attributeValue, subscriptionArn);
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(),"InvalidParameter", "DeliveryPolicy: sicklyRetryPolicy.numRetries must be greater than or equal to total of numMinDelayRetries, numNoDelayRetries and numMaxDelayRetries"));
			out.reset();
			
			CNSTestingUtils.deleteTopic(cns, user, out, topicArn);

		} catch (Exception e) {
			logger.error("Exception occured", e);
			fail("exception: "+e);
		}
	}

	@Test
	public void testBadGetAttributes() {

		try {
		
			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user, out, topicName);
			String res = out.toString();
			String topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = "http://nis.test3.plaxo.com:8080/CMB/Endpoint/recv/" + rand.nextInt();
			String protocol = "http";
			CNSTestingUtils.subscribe(cns, user, out, endpoint, protocol, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			if (subscriptionArn.equals("pending confirmation")) {

				List<CNSSubscription> confirmedSubscriptions = CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user.getUserId(), CnsSubscriptionProtocol.valueOf(protocol));

				if (confirmedSubscriptions.size() == 1) {
					subscriptionArn = confirmedSubscriptions.get(0).getArn();
				}
			}

			CNSTestingUtils.doGetSubscriptionAttributes(cns, user, out, null);
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res,"InvalidParameter", "missing parameters"));
			out.reset();
			
			CNSTestingUtils.deleteTopic(cns, user, out, topicArn);

		} catch (Exception e) {
			logger.error("Exception occured", e);
			fail("exception: "+e);
		}
	}
}
