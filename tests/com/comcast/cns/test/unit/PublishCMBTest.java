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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
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
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.cns.controller.CNSControllerServlet;
import com.comcast.cns.io.CommunicationUtils;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cqs.controller.CQSControllerServlet;

public class PublishCMBTest {
	
	private static Logger logger = Logger.getLogger(PublishCMBTest.class);
	
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

		} catch (Throwable ex) {
			fail(ex.toString());
		}
	}

	@After
	public void tearDown() {
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}

	@Test
	public void testSendReceiveCQS() throws Exception {
		
		CQSControllerServlet cqs = new CQSControllerServlet();
		String queueUrl = null;
		String queueArn = null;

		try {	
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);

			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			logger.info("Created queue " + queueUrl);

			Thread.sleep(500);
			
			CNSSubscription.CnsSubscriptionProtocol protocol = CNSSubscription.CnsSubscriptionProtocol.cqs;
			String endPoint = queueArn;
			String message = "test_abc";
			
			logger.info("Sending message");

			CommunicationUtils.sendMessage(user1, protocol, endPoint, message, "", "", "");

			Thread.sleep(500);
			
			String msg = CNSTestingUtils.receiveMessage(cqs, user1, queueUrl, "1");

			String messageResp = CNSTestingUtils.getMessage(msg);
			
			assertTrue("Expected message test_abc, instead found " + messageResp, messageResp.equals("test_abc"));

			String receiptHandle = CNSTestingUtils.getReceiptHandle(msg);
			
			CNSTestingUtils.deleteMessage(cqs, user1, queueUrl, receiptHandle);

		} catch (Exception ex) {
			fail(ex.toString());
		} finally {
			if (queueUrl != null) {
				CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
			}
		}
	}
	
	@Test
	public void testHttpEndpoint() throws Exception {

		CNSSubscription.CnsSubscriptionProtocol protocol = CNSSubscription.CnsSubscriptionProtocol.http;
		String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/252910";
		String message = "test_abc";
		CommunicationUtils.sendMessage(user1, protocol, endPoint, message, "", "", "");
		String lastMessageUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "info/252910?showLast=true";
		String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

		assertTrue("Endpoint " + CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + " is down", resp.equals(message));
	}

	@Test
	public void testEmailPublisher() throws Exception {
		
		CNSSubscription.CnsSubscriptionProtocol protocol = CNSSubscription.CnsSubscriptionProtocol.email;
		String endPoint = CMBTestingConstants.EMAIL_ENDPOINT;
		String message = "test email";
		CommunicationUtils.sendMessage(user1, protocol, endPoint, message, "", "", "");
		
		protocol = CNSSubscription.CnsSubscriptionProtocol.email_json;
		endPoint = CMBTestingConstants.EMAIL_ENDPOINT;
		message = "test email";
		CommunicationUtils.sendMessage(user1, protocol, endPoint, message, "", "", "");
	}

	@Test
	public void testPublishToNonExitentTopicArn() {

		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	

			String message = "test_efg";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);
			logger.info("Created topic " + topicArn);
			out.reset();
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);
			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			logger.info("Created queue " + queueUrl);
			
			logger.info("Publishing to non-existent topic arn");

			Thread.sleep(500);

			String endpoint = queueArn;
			String protocolStr = "cqs";

			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = null;
			String subject = null;
			String fakeTopicArn = topicArn.substring(0,topicArn.length()-1);

			String resp = CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, fakeTopicArn);
			out.reset();
			String code = "NotFound";
			String errorMessage = "Resource not found.";

			assertTrue("Did no get resource not found exception", CNSTestingUtils.verifyErrorResponse(resp, code, errorMessage));

			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			out.reset();
			
		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
				} catch (Exception ex) { }
			}
		}
	}	   

	@Test
	public void testPublishToInvalidTopicArn() {
		
		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String message = "test_efg";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);
			
			out.reset();
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);

			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			logger.info("Created queue " + queueUrl);
			
			logger.info("Publishing to invalid topic arn");

			Thread.sleep(500);

			String endpoint = queueArn;
			String protocolStr = "cqs";

			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = null;
			String subject = null;
			String fakeTopicArn = "squirrel";

			String resp = CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, fakeTopicArn);
			String code = "InvalidParameter";
			String errorMessage = "TopicArn";

			assertTrue("Expected invalid parameter exception", CNSTestingUtils.verifyErrorResponse(resp, code, errorMessage));
			out.reset();

			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			out.reset();

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
				} catch (Exception ex) { }
			}
		}
	}

	@Test
	public void testPublishToMissingArn() {

		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String message = "test_efg";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);
			
			out.reset();
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);

			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			logger.info("Created queue " + queueUrl);
			
			logger.info("Publishing to invalid topic arn");

			Thread.sleep(500);

			String endpoint = queueArn;
			String protocolStr = "cqs";

			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = null;
			String subject = null;

			String resp = CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, null);
			String code = "InvalidParameter";
			String errorMessage = "TopicArn";

			assertTrue("Expected invalid parameter exception", CNSTestingUtils.verifyErrorResponse(resp, code, errorMessage));

			out.reset();

			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			out.reset();

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
				} catch (Exception ex) { }
			}
		}
	}	   

	@Test
	public void testPublishMissingMessage() {
		
		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);
			
			out.reset();
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);

			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			logger.info("Created queue " + queueUrl);
			
			logger.info("Publishing missing message");

			Thread.sleep(500);

			String endpoint = queueArn;
			String protocolStr = "cqs";

			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = null;
			String subject = null;

			String resp = CNSTestingUtils.doPublish(cns, user1, out, null, messageStructure, subject, topicArn);
			String code = "ValidationError";
			String errorMessage = "1 validation error detected: Value null at 'message' failed to satisfy constraint: Member must not be null";
			
			assertTrue("Expected validation error", CNSTestingUtils.verifyErrorResponse(resp, code, errorMessage));

			out.reset();
			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			out.reset();

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
				} catch (Exception ex) { }
			}
		}
	}	   

	@Test
	public void testPublishBadMessage() {
		
		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String message = "test_efg";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);
			
			out.reset();
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);

			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			logger.info("Created queue " + queueUrl);
			
			logger.info("Publishing bad message");

			Thread.sleep(500);

			String endpoint = queueArn;
			String protocolStr = "cqs";

			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = "boo";
			String subject = null;

			String resp = CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);
			String code = "InvalidParameter";
			String errorMessage = "Invalid parameter: Invalid Message Structure parameter: boo";

			assertTrue("Expected invalid message structure exception", CNSTestingUtils.verifyErrorResponse(resp, code, errorMessage));

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
			
			if (queueUrl != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
				} catch (Exception ex) { }
			}
		}
	}	   

	@Test
	public void testPublishToTopicWithCQSSubscriber() {

		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String message = "test_efgh";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);
			
			out.reset();
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);

			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			logger.info("Created queue " + queueUrl);
			
			logger.info("Publishing message");

			Thread.sleep(1000);

			String endpoint = queueArn;
			String protocolStr = "cqs";

			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = null;
			String subject = null;

			CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);

			Thread.sleep(1000);
			
			String msg = CNSTestingUtils.receiveMessage(cqs, user1, queueUrl, "1");
			assertTrue("msg received is null", msg != null);
			logger.debug("Message is: " + msg);
			String messageResp = CNSTestingUtils.getMessage(msg);


			JSONObject json = new JSONObject(messageResp);
			String resp_message = json.getString("Message");

			assertTrue(resp_message.equals("test_efgh"));
			String receiptHandle = CNSTestingUtils.getReceiptHandle(msg);
			assertTrue(receiptHandle != null);
			
			CNSTestingUtils.deleteMessage(cqs, user1, queueUrl, receiptHandle);

			message = "test_etc_1111";
			messageStructure = null;
			subject = null;

			CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);

			Thread.sleep(1000);
			
			msg = CNSTestingUtils.receiveMessage(cqs, user1, queueUrl, "1");
			
			assertTrue("Message is null", msg != null);

			messageResp = CNSTestingUtils.getMessage(msg);

			assertTrue("Message response is null", messageResp != null);

			json = new JSONObject(messageResp);
			resp_message = json.getString("Message");
			logger.debug("resp_message is:" + resp_message);

			assertTrue("Expected message test_etc_1111, instead found " + resp_message, resp_message.equals("test_etc_1111"));
			
			receiptHandle = CNSTestingUtils.getReceiptHandle(msg);
			
			assertTrue(receiptHandle != null);
			
			CNSTestingUtils.deleteMessage(cqs, user1, queueUrl, receiptHandle);

			out.reset();
			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
				} catch (Exception ex) { }
			}
		}
	}	   

	@Test
	public void testPublishToTopicWithEmailSubscriber() {
		
		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.EMAIL_ENDPOINT;
			String message = "test the email servlet";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);

			out.reset();

			String protocolStr = "email";

			CNSTestingUtils.subscribe(cns, user1, out, endPoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = null;
			String subject = "Email Subject";
			CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);

			Thread.sleep(500);
			
			out.reset();
			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			out.reset();

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
		}
	}	   

	@Test
	public void testPublishToTopicWithHttpSubscriber() {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		
		try {	
			
			int endpointid = rand.nextInt();
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/60" + endpointid;
			String message = "test Http servlet";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);

			out.reset();

			String protocolStr = "http";

			CNSTestingUtils.subscribe(cns, user1, out, endPoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();
			
			Thread.sleep(500);
			
			String lastMessageUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "info/60" + endpointid + "?showLast=true";

			if (subscriptionArn.equals("pending confirmation")) {

				String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

    			JSONObject o = new JSONObject(resp);
    			
    			if (!o.has("SubscribeURL")) {
    				fail("Message is not a confirmation messsage");
    			}
    			
    			String subscriptionUrl = o.getString("SubscribeURL");
    			
				resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");

			} else {
				fail("No confirmation requested");
			}
			
			String messageStructure = null;
			String subject = null;
			CNSTestingUtils.doPublish(cns, user1, out, null, messageStructure, subject, topicArn);
			
			assertTrue("Expected validation exception", CNSTestingUtils.verifyErrorResponse(out.toString(), "ValidationError", "1 validation error detected: Value null at 'message' failed to satisfy constraint: Member must not be null"));

			CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);
			message = "";
			
			Thread.sleep(1000);

			String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			JSONObject json = new JSONObject(resp);

			String resp_message = json.getString("Message");

			assertTrue("Expected message 'test Http servlet', intead found " + resp_message, resp_message.equals("test Http servlet"));

			out.reset();

			message = "Http servlet 12584";
			messageStructure = null;
			subject = null;
			CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);
			
			Thread.sleep(1000);

			resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			json = new JSONObject(resp);

			resp_message = json.getString("Message");

			assertTrue("Expected message " + message + ", instead got " + resp_message, resp_message.equals(message));

			out.reset();

			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
		}
	}	   

	@Test
	public void testMultipleEndpoints() {
		
		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();
			String message = "test Http servlet 2";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);

			out.reset();

			String protocolStr = "http";			
			CNSTestingUtils.subscribe(cns, user1, out, endPoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();
			
			String lastMessageUrl = endPoint.replace("recv", "info") + "?showLast=true";
			
			if (subscriptionArn.equals("pending confirmation")) {

				String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

    			JSONObject o = new JSONObject(resp);
    			
    			if (!o.has("SubscribeURL")) {
    				fail("Message is not a confirmation messsage");
    			}
    			
    			String subscriptionUrl = o.getString("SubscribeURL");
    			
				resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
    		    
		        logger.info(resp);
			}
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);

			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			Thread.sleep(1000);

			String endpoint = queueArn;
			protocolStr = "cqs";

			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocolStr, topicArn);
			String subscriptionArn2 = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = null;
			String subject = null;
			CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);
			
			Thread.sleep(1000);

			String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");
			logger.debug("resp is: " + resp);

			JSONObject json = new JSONObject(resp);

			String resp_message = json.getString("Message");

			assertTrue("Expected message " + message + ", instead got " + resp_message, resp_message.equals(message));

			String msg = CNSTestingUtils.receiveMessage(cqs, user1, queueUrl, "1");
			String messageResp = CNSTestingUtils.getMessage(msg);
			assertTrue("Message response is null", messageResp != null);

			json = new JSONObject(messageResp);
			resp_message = json.getString("Message");

			assertTrue("Expected message " + message + ", instead found " + resp_message, resp_message.trim().equals(message));

			String receiptHandle = CNSTestingUtils.getReceiptHandle(msg);
			assertTrue(receiptHandle != null);
			CNSTestingUtils.deleteMessage(cqs, user1, queueUrl, receiptHandle);
			out.reset();

			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn2);
			out.reset();

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
				} catch (Exception ex) { }
			}
		}
	}	   

	@Test
	public void testMessageStructure() {
		
		CNSControllerServlet cns = new CNSControllerServlet();
		CQSControllerServlet cqs = new CQSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();
			String httpMessage = "test Http servlet 2";

			String cqsMessage = "test CQS servlet 2";

			String topicName = "T" + rand.nextLong();
			CNSTestingUtils.addTopic(cns, user1, out, topicName);
			String res = out.toString();
			topicArn = CNSTestingUtils.getArnFromString(res);

			logger.info("Created topic " + topicArn);

			out.reset();

			String protocolStr = "http";			
			CNSTestingUtils.subscribe(cns, user1, out, endPoint, protocolStr, topicArn);
			String subscriptionArn = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();
			
			String lastMessageUrl = endPoint.replace("recv", "info") + "?showLast=true";

			if (subscriptionArn.equals("pending confirmation")) {

				String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

    			JSONObject o = new JSONObject(resp);
    			
    			if (!o.has("SubscribeURL")) {
    				fail("Message is not a confirmation messsage");
    			}
    			
    			String subscriptionUrl = o.getString("SubscribeURL");
    			
				resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
			}
			
			String queueName = "Q" + rand.nextLong();
			String cqsQueue = CNSTestingUtils.addQueue(cqs, user1, queueName);

			queueUrl = CNSTestingUtils.getQueueUrl(cqsQueue);					
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			Thread.sleep(1000);

			String endpoint = queueArn;
			protocolStr = "cqs";

			CNSTestingUtils.subscribe(cns, user1, out, endpoint, protocolStr, topicArn);
			String subscriptionArn2 = CNSTestingUtils.getSubscriptionArnFromString(out.toString());
			out.reset();

			String messageStructure = "json";
			String subject = null;
			String message = CNSTestingUtils.generateMultiendpointMessageJson(null, null, "test message", httpMessage, null, cqsMessage);
			CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);
			
			Thread.sleep(1000);

			String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			JSONObject json = new JSONObject(resp);

			String resp_message = json.getString("Message");

			assertTrue("Expected message '" + httpMessage + "', instead found " + resp_message, resp_message.equals(httpMessage));

			String msg = CNSTestingUtils.receiveMessage(cqs, user1, queueUrl, "1");
			assertTrue(msg != null);
			String messageResp = CNSTestingUtils.getMessage(msg);
			assertTrue(messageResp != null);

			json = new JSONObject(messageResp);
			resp_message = json.getString("Message");

			assertTrue("Expected message '" + cqsMessage + "', instead found " + resp_message, resp_message.equals(cqsMessage));

			String receiptHandle = CNSTestingUtils.getReceiptHandle(msg);
			assertTrue(receiptHandle != null);
			CNSTestingUtils.deleteMessage(cqs, user1, queueUrl, receiptHandle);

			out.reset();

			httpMessage = "test Http servlet 45554";
			String httpMessage2 = httpMessage;
			cqsMessage = "test CQS servlet 2758";
			
			message = CNSTestingUtils.generateMultiendpointMessageJson(null, null, "test message", httpMessage2, null, cqsMessage);
			CNSTestingUtils.doPublish(cns, user1, out, message, messageStructure, subject, topicArn);

			Thread.sleep(1000);

			resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			json = new JSONObject(resp);
			resp_message = json.getString("Message");

			assertTrue("Expected message '" + httpMessage + "', instead found " + resp_message, resp_message.equals(httpMessage));

			msg = CNSTestingUtils.receiveMessage(cqs, user1, queueUrl, "1");
			messageResp = CNSTestingUtils.getMessage(msg);
			assertTrue(messageResp != null);

			json = new JSONObject(messageResp);
			resp_message = json.getString("Message");
			assertTrue("Message response is null", resp_message != null);

			assertTrue("Expected message '" + cqsMessage + "', instead found " + resp_message, resp_message.trim().equals(cqsMessage));

			receiptHandle = CNSTestingUtils.getReceiptHandle(msg);

			CNSTestingUtils.deleteMessage(cqs, user1, queueUrl, receiptHandle);

			out.reset();
			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn);
			CNSTestingUtils.unSubscribe(cns, user1, out, subscriptionArn2);
			out.reset();

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					CNSTestingUtils.deleteTopic(cns, user1, out, topicArn);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					CNSTestingUtils.deleteQueue(cqs, user1, queueUrl);
				} catch (Exception ex) { }
			}
		}
	}	   
}
