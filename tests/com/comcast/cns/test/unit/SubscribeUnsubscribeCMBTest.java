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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

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
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.cmb.test.tools.ListSubscriptionParser;
import com.comcast.cns.controller.CNSControllerServlet;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cqs.controller.CQSControllerServlet;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.ICQSQueuePersistence;


public class SubscribeUnsubscribeCMBTest {
	
	private static Logger logger = Logger.getLogger(SubscribeUnsubscribeCMBTest.class);

	private User user1;
	private User user2;

	private Random rand = new Random();
	
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
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}

	@Test
	public void testSubscribeUnsubscribe() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			String protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();
			
			CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user1.getUserId(), CnsSubscriptionProtocol.email);
			
			CNSTestingUtils.listSubscriptions(cns, user1, out, null);
			logger.info(out.toString());
			Vector<CNSSubscriptionTest> subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			logger.info("subscriptions size is:" + subscriptions.size());
			assertTrue(subscriptions.size() == 1);
			subscriptionArn = subscriptions.get(0).getSubscriptionArn();
			CNSSubscriptionTest sub = subscriptions.get(0);
			assertTrue(sub.getEndpoint().equals(CMBTestingConstants.EMAIL_ENDPOINT));
			assertTrue(sub.getProtocol().equals("email"));
			assertTrue(sub.getTopicArn().equals(topicArn));
			assertTrue(sub.getOwner().equals(user1.getUserId()));
			out.reset();

			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, topicArn, null);			
			subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			assertTrue(subscriptions.size() == 1);
			sub = subscriptions.get(0);
			assertTrue(sub.getEndpoint().equals(CMBTestingConstants.EMAIL_ENDPOINT));
			assertTrue(sub.getProtocol().equals("email"));
			assertTrue(sub.getTopicArn().equals(topicArn));
			assertTrue(sub.getOwner().equals(user1.getUserId()));
			out.reset();

			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			out.reset();

			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, topicArn, null);
			subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			assertTrue(subscriptions.size() == 0);
			out.reset();

			CNSTestingUtils.listSubscriptions(cns, user1, out, null);
			subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			assertTrue(subscriptions.size() == 0);

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
	public void testBadListSubscriptions() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			String protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			CNSTestingUtils.listSubscriptions(cns, user1, out, "Faketoken");
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameterValue", "Invalid parameter nextToken"));				
			out.reset();

			String fakeArn = topicArn.substring(0,topicArn.length()-1) + "b";
			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, fakeArn, null);			
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "NotFound", "Resource not found."));				
			out.reset();

			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, null, null);			
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));				
			out.reset();

			fakeArn = "monkey";
			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, fakeArn, null);			
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));				
			out.reset();

			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, topicArn, "Faketoken");			
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameterValue", "Invalid parameter nextToken"));				
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

	@Test
	public void testBadUnsubscribe() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			String protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String fakeArn = "monkey";
			CNSTestingUtils.unSubscribe(cns, user1, out, fakeArn);
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			CNSTestingUtils.unSubscribe(cns, user1, out, null);
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
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

	public static void addParam(Map<String, String[]> params, String name, String val) {        
		CNSTestingUtils.addParam(params, name, val);
	}

	@Test
	public void testBadSubscriptions() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			String protocol = "call";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			protocol = "sms";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/1234";
			protocol = "https";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			endpoint = CMBTestingConstants.HTTPS_ENDPOINT_BASE_URL + "recv/1234";
			protocol = "http";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			endpoint = "650-988-8888";
			protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			endpoint = "western";
			protocol = "email-json";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			String fakeTopicArn = topicArn.substring(0,topicArn.length()-1) + "b";
			endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, fakeTopicArn);
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "NotFound", "Resource not found."));
			out.reset();

			fakeTopicArn = "monkey";
			endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, fakeTopicArn);
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, null);
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, null, topicArn);
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			protocol = "email";
			CNSTestingUtils.subscribe(cns, user1, out, null, protocol, topicArn);
			logger.debug(out.toString());
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "InvalidParameter", "request parameter does not comply with the associated constraints."));
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

	@Test
	public void testManySubscriptions() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();
			
			CNSTestingUtils.addPermission(cns, user1, out, topicArn, "*", "*", rand.nextLong()+"");
			out.reset();

			String endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/2526";
			String protocol = "http";
			CNSTestingUtils.subscribe(cns, user2, out, endpoint, protocol, topicArn);
			CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();
			
			CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user2.getUserId(), CnsSubscriptionProtocol.http);
			
			CNSTestingUtils.listSubscriptions(cns, user1, out, null);
			logger.info(out.toString());
			Vector<CNSSubscriptionTest> subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			assertTrue(subscriptions.size() == 0);
			out.reset();

			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, topicArn, null);
			subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			assertTrue(subscriptions.size() == 1);
			out.reset();

			subscriptions.get(0).getSubscriptionArn();

			endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/2527";
			protocol = "http";
			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
			CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user1.getUserId(), CnsSubscriptionProtocol.http);

			CNSTestingUtils.listSubscriptions(cns, user1, out, null);
			subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			assertTrue(subscriptions.size() == 1);
			CNSSubscriptionTest sub = subscriptions.get(0);
			subscriptions.get(0).getSubscriptionArn();

			assertTrue(sub.getEndpoint().equals(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/2527"));
			assertTrue(sub.getProtocol().equals("http"));
			assertTrue(sub.getTopicArn().equals(topicArn));
			assertTrue(sub.getOwner().equals(user1.getUserId()));
			out.reset();

			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, topicArn, null);
			logger.info(out.toString());
			subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			assertTrue(subscriptions.size() == 2);

			if (subscriptions.get(0).getOwner().equals(user1.getUserId())) {
				sub = subscriptions.get(0); 
			} else {
				sub = subscriptions.get(1); 
			}

			assertTrue(sub.getEndpoint().equals(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/2527"));
			assertTrue(sub.getProtocol().equals("http"));
			assertTrue(sub.getTopicArn().equals(topicArn));
			assertTrue(sub.getOwner().equals(user1.getUserId()));
			out.reset();

			// add 100 other subscriptions

			for (int i=0; i<100; i++) {
				endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/2500" + i;
				protocol = "http";
				CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocol, topicArn);
				CNSTestingUtils.getSubscriptionArnFromString(out.toString());
				out.reset();
			}
			
			CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user1.getUserId(), CnsSubscriptionProtocol.http);
			out.reset();
			
			ListSubscriptionParser pl;
			CNSTestingUtils.listSubscriptions(cns, user1, out, null);
			pl = CNSTestingUtils.getSubscriptionDataFromString(out.toString());
			subscriptions = pl.getSubscriptions();
			assertTrue(subscriptions.size() == 100);
			assertTrue(pl.getNextToken() != null);
			out.reset();

			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, topicArn, null);
			logger.info(out.toString());
			pl = CNSTestingUtils.getSubscriptionDataFromString(out.toString());
			subscriptions = pl.getSubscriptions();
			logger.info("subscriptions size is:" + subscriptions.size());
			assertTrue(subscriptions.size() == 100); 
			assertTrue(pl.getNextToken() != null);
			out.reset();

			CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);

			out.reset();

			CNSTestingUtils.listSubscriptions(cns, user1, out, null);
			subscriptions = CNSTestingUtils.getSubscriptionsFromString(out.toString());
			assertTrue(subscriptions.size() == 0); 
			out.reset();

			CNSTestingUtils.listSubscriptionsByTopic(cns, user1, out, topicArn, null);
			res = out.toString();
			logger.debug("response: " + res);
			assertTrue(CNSTestingUtils.verifyErrorResponse(res, "NotFound", "Resource not found."));
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

	@Test
	public void testSubscribeCQS() {

		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueArn = null;
		String queueURL = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();
			
			CNSTestingUtils.addPermission(cns, user1, out, topicArn, "*", "*", rand.nextLong()+"");
			out.reset();
			
			String cqsResponse = CNSTestingUtils.addQueue(cqs, user2, "Q" + rand.nextLong());
			ICQSQueuePersistence queueHandler = PersistenceFactory.getQueuePersistence();	
			logger.info("cqsQueue: " + cqsResponse);
			queueURL = CNSTestingUtils.getQueueUrl(cqsResponse);					
			CQSQueue queue = queueHandler.getQueue(com.comcast.cqs.util.Util.getRelativeForAbsoluteQueueUrl(queueURL));
			queueArn = queue.getArn();
			
			Thread.sleep(500);

			String protocol = "cqs";

			CNSTestingUtils.setPermissionForUser(cqs, user2, user1, queueURL);
			Thread.sleep(200);
			
			CNSTestingUtils.subscribe(cns, user1, out, queueArn, protocol, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			logger.debug("subscriptionArn: " + subscriptionArn);
			out.reset();
			Thread.sleep(500);

			String msg = CNSTestingUtils.receiveMessage(cqs, user2, queueURL, "1");
			logger.debug("msg: " + msg);
			String receiptHandle = CNSTestingUtils.getReceiptHandle(msg);
			String message = CNSTestingUtils.getMessage(msg);

			if (message == null) {
				fail("blank message response=" + msg);
			}
			
			logger.debug("message: " + message);
			JSONObject js = new JSONObject(message);

			String token = js.getString("Token");
			assertTrue(token != null);
			assertTrue(receiptHandle != null);
			logger.debug("receiptHandle: " + receiptHandle);
			logger.debug("token: " + token);
			out.reset();

			CNSTestingUtils.confirmSubscription(cns, user1, out, topicArn, token, null);
			logger.debug("confirmSubscription response: " + out.toString());
			subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			assertTrue(subscriptionArn != null);
			out.reset();

			CNSTestingUtils.deleteMessage(cqs, user2, queueURL, receiptHandle);
			out.reset();
	
			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			out.reset();
			
			CNSTestingUtils.deleteallSubscriptions(cns, user2, out);
			out.reset(); 
			
			Thread.sleep(500);

			CNSTestingUtils.subscribe(cns, user2, out, queueArn, protocol, topicArn);
			Thread.sleep(500);
			subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			logger.debug("subscriptionArn: " + subscriptionArn);
			out.reset();
			
			assertTrue(subscriptionArn != null && !subscriptionArn.toLowerCase().contains("pending"));

			CNSTestingUtils.deleteMessage(cqs, user2, queueURL, receiptHandle);
			out.reset();
			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
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
			
			if (queueURL != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user2, queueURL);
				} catch (Exception e) { }
			}
		}
	}
	
	@Test
	public void testSubscribeEmail() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();

			String endpoint = CMBTestingConstants.EMAIL_ENDPOINT;
			String protocol = "email";
			CNSTestingUtils.subscribe(cns, user2, out, endpoint, protocol, topicArn);
			CNSTestingUtils.getSubscriptionArnFromString(out.toString());

			protocol = "email-json";
			CNSTestingUtils.subscribe(cns, user2, out, endpoint, protocol, topicArn);
			CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			
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
	public void testBadConfirmRequest() {

		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueURL = null;

		try {
			
			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			out.reset();
			
			CNSTestingUtils.addPermission(cns, user1, out, topicArn, "*", "*", rand.nextLong()+"");
			out.reset();

			String token = "faketoken";

			CNSTestingUtils.confirmSubscription(cns, user1, out, topicArn, token, null);
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(), "NotFound", "Resource not found."));				
			out.reset();

			CNSTestingUtils.confirmSubscription(cns, user1, out, topicArn, null, null);
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidParameter", "request parameter does not comply with the associated constraints."));				
			out.reset();
			
			String cqsResponse = CNSTestingUtils.addQueue(cqs, user2, "Q" + rand.nextLong());
			ICQSQueuePersistence queueHandler = PersistenceFactory.getQueuePersistence();	
			logger.info("cqsQueue: " + cqsResponse);
			queueURL = CNSTestingUtils.getQueueUrl(cqsResponse);					
			queueHandler.getQueue(queueURL);

			String endpoint = queueURL;
			String protocol = "cqs";
			CNSTestingUtils.setPermissionForUser(cqs, user2, user1, queueURL);

			CNSTestingUtils.subscribe(cns, user2, out, endpoint, protocol, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			logger.debug("subscriptionArn: " + subscriptionArn);
			out.reset();
			Thread.sleep(1000);

			String msg = CNSTestingUtils.receiveMessage(cqs, user2, queueURL, "1");
			logger.debug("msg: " + msg);
			String receiptHandle = CNSTestingUtils.getReceiptHandle(msg);
			String message = CNSTestingUtils.getMessage(msg);
			assertTrue("Failed to receive message", message != null);
			JSONObject js = new JSONObject(message);

			token = js.getString("Token");
			assertTrue(token != null);
			assertTrue(receiptHandle != null);
			logger.debug("receiptHandle: " + receiptHandle);
			logger.debug("token: " + token);
			out.reset();

			CNSTestingUtils.confirmSubscription(cns, user1, out, "fakeARN", token, null);
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidParameter", "request parameter does not comply with the associated constraints."));				
			out.reset();

			CNSTestingUtils.confirmSubscription(cns, user1, out, topicArn, token, "blech");
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidParameter", "request parameter does not comply with the associated constraints."));				
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

			if (queueURL != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user2, queueURL);
				} catch (Exception e) { }
			}
		}
	}
}
