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
import java.util.Random;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.test.tools.TopicAttributeParser;
import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.Util;
import com.comcast.plaxo.cns.controller.CNSControllerServlet;

public class TopicAttributeCMBTest {

	private static Logger logger = Logger.getLogger(TopicAttributeCMBTest.class);

	private User user1;
	private User user2;
	Random rand = new Random();

	@Before
	public void setup() {

		try {

			Util.initLog4jTest();
			CMBControllerServlet.valueAccumulator.initializeAllCounters();
			PersistenceFactory.reset();
			
			IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
			
			String userName1 = "cns_unit_test_1";
			String userName2 = "cns_unit_test_2";

			user1 = userHandler.getUserByName(userName1);

			if (user1 == null) {
				user1 =  userHandler.createUser(userName1, userName1);
			}

			user2 = userHandler.getUserByName(userName2);

			if (user2 == null) {
				user2 =  userHandler.createUser(userName2, userName2);
			}

		} catch (Exception ex) {
			logger.error("setup failed", ex);
			assertFalse(true);
		}
	}

	@After
	public void tearDown() {
	}

	@Test
	public void testGetAttributes() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			CNSTestingUtils.doGetTopicAttributes(cns, user1, out, topicArn);
			res = out.toString();
			out.reset();
			logger.debug("getSubscription Attributes: " + res);
			TopicAttributeParser tap = CNSTestingUtils.getTopicAttributesFromString(res);
			assertTrue("Default Effective Delivery Policy missing", tap.getEffectiveDeliveryPolicy() != null);
			String deliveryPolicy = tap.getEffectiveDeliveryPolicy();
			JSONObject dpJson = new JSONObject(deliveryPolicy);
			logger.debug("Delivery Policy:" + deliveryPolicy);

			assertTrue("Default HTTP Policy Missing", dpJson.has("http"));
			dpJson = dpJson.getJSONObject("http");

			assertTrue("Default Effective Default Healty Delivery Policy missing", dpJson.has("defaultHealthyRetryPolicy"));
			JSONObject dhrp = dpJson.getJSONObject("defaultHealthyRetryPolicy");
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("backoffFunction"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getString("backoffFunction").equals("linear"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("numMinDelayRetries"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("numMinDelayRetries") == 0);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("numMaxDelayRetries"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("numMaxDelayRetries") == 0);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("numRetries"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("numRetries") == 3);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("minDelayTarget"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("minDelayTarget") == 20);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("numNoDelayRetries"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("numNoDelayRetries") == 0);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("maxDelayTarget"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("maxDelayTarget") == 20);

			assertTrue("Default Owner missing", tap.getOwner() != null);

		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("test failed: " + ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
		}
	}

	@Test
	public void testSetGetAttributes() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String jsonStr2 = "{" +
			"\"http\":" +
			"{\"disableSubscriptionOverrides\":true," +
			"\"defaultSicklyRetryPolicy\":null," +
			"\"defaultHealthyRetryPolicy\":" +				 
			"{"+
			"\"minDelayTarget\":1,"+
			"\"maxDelayTarget\":2,"+
			"\"numRetries\":17,"+
			"\"numMaxDelayRetries\": 4,"+
			"\"numMinDelayRetries\": 6,"+
			"\"numNoDelayRetries\": 7,"+
			"\"backoffFunction\": \"geometric\""+
			"}," +
			"\"defaultThrottlePolicy\":null" +
			"}}";

			CNSTestingUtils.doSetTopicAttributes(cns, user1, out, "DeliveryPolicy", jsonStr2, topicArn);
			out.reset();
			CNSTestingUtils.doGetTopicAttributes(cns, user1, out, topicArn);
			res = out.toString();
			out.reset();
			logger.debug("get Topic Attributes: " + res);
			TopicAttributeParser tap = CNSTestingUtils.getTopicAttributesFromString(res);
			assertTrue("Default Effective Delivery Policy missing", tap.getEffectiveDeliveryPolicy() != null);
			String deliveryPolicy = tap.getEffectiveDeliveryPolicy();
			JSONObject dpJson = new JSONObject(deliveryPolicy);
			logger.debug("Delivery Policy:" + deliveryPolicy);

			assertTrue("Default HTTP Policy Missing", dpJson.has("http"));
			dpJson = dpJson.getJSONObject("http");

			assertTrue("Default Effective Default Healty Delivery Policy missing", dpJson.has("defaultHealthyRetryPolicy"));
			JSONObject dhrp = dpJson.getJSONObject("defaultHealthyRetryPolicy");
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("backoffFunction"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getString("backoffFunction").equals("geometric"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("numMinDelayRetries"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("numMinDelayRetries") == 6);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("numMaxDelayRetries"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("numMaxDelayRetries") == 4);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("numRetries"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("numRetries") == 17);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("minDelayTarget"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("minDelayTarget") == 1);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("numNoDelayRetries"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("numNoDelayRetries") == 7);
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.has("maxDelayTarget"));
			assertTrue("Default Effective Default Healty Delivery Policy setup wrong", dhrp.getInt("maxDelayTarget") == 2);

			assertTrue("Default Owner missing", tap.getOwner() != null);

		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("test failed: " + ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
		}
	}

	@Test
	public void testDisplayName() {
		
		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			CNSTestingUtils.doSetTopicAttributes(cns, user1, out, "DisplayName", "Disp Name", topicArn);
			out.reset();
			CNSTestingUtils.doGetTopicAttributes(cns, user1, out, topicArn);
			res = out.toString();
			out.reset();
			logger.debug("get Topic Attributes: " + res);
			TopicAttributeParser tap = CNSTestingUtils.getTopicAttributesFromString(res);
			assertTrue("Display Name setup wrong", tap.getDisplayName() != null);
			assertTrue("Display Name setup wrong", tap.getDisplayName().equals("Disp Name"));

			CNSTestingUtils.doSetTopicAttributes(cns, user1, out, "DisplayName", "Disp\nName", topicArn);
			String resp = out.toString();
			String code = "InvalidParameter";
			String errorMessage = "Bad Display Name";
			logger.debug("resp is: " + resp);
			assertTrue(CNSTestingUtils.verifyErrorResponse(resp, code, errorMessage));

			out.reset();

			CNSTestingUtils.doSetTopicAttributes(cns, user1, out, "DisplayName", "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567891", topicArn);
			resp = out.toString();
			code = "InvalidParameter";
			errorMessage = "Bad Display Name";
			logger.debug("resp is: " + resp);
			assertTrue(CNSTestingUtils.verifyErrorResponse(resp, code, errorMessage));

			out.reset();

			CNSTestingUtils.doSetTopicAttributes(cns, user1, out, "DisplayName", "�e�ou", topicArn);
			resp = out.toString();
			code = "InvalidParameter";
			errorMessage = "Bad Display Name";
			logger.debug("resp is: " + resp);
			assertTrue(CNSTestingUtils.verifyErrorResponse(resp, code, errorMessage));

			out.reset();

		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("test failed: " + ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
		}
	}
}
