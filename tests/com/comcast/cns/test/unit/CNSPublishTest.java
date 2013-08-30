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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.cns.controller.CNSControllerServlet;
import com.comcast.cns.io.CommunicationUtils;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cqs.controller.CQSControllerServlet;

public class CNSPublishTest {
	
	private static Logger logger = Logger.getLogger(CNSPublishTest.class);
	
	private User user1;
	private User user2;
	
	private AmazonSNS sns1 = null;
	private AmazonSQS sqs1 = null;

	private AmazonSNS sns2 = null;
	private AmazonSQS sqs2 = null;
	
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
			
			AWSCredentials awsCredentials1 = new BasicAWSCredentials(user1.getAccessKey(), user1.getAccessSecret());
			AWSCredentials awsCredentials2 = new BasicAWSCredentials(user2.getAccessKey(), user2.getAccessSecret());
			
			sns2 = new AmazonSNSClient(awsCredentials2);
		    sqs2 = new AmazonSQSClient(awsCredentials2);

			sns1 = new AmazonSNSClient(awsCredentials1);
		    sqs1 = new AmazonSQSClient(awsCredentials1);

	    	sns1.setEndpoint(CMBProperties.getInstance().getCNSServiceUrl());
			sqs1.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());
			
			sns2.setEndpoint(CMBProperties.getInstance().getCNSServiceUrl());
			sqs2.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());

		} catch (Throwable ex) {
			fail(ex.toString());
		}
	}

	@After
	public void tearDown() {
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	@Test
	public void testHttpEndpoint() throws Exception {

		CNSSubscription.CnsSubscriptionProtocol protocol = CNSSubscription.CnsSubscriptionProtocol.http;
		String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/252910";
		String message = "test_abc";
		CommunicationUtils.sendMessage(user1, protocol, endPoint, message);
		String lastMessageUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "info/252910?showLast=true";
		String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

		assertTrue("Endpoint " + CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + " is down", resp.equals(message));
	}

	@Test
	public void testEmailPublisher() throws Exception {
		
		CNSSubscription.CnsSubscriptionProtocol protocol = CNSSubscription.CnsSubscriptionProtocol.email;
		String endPoint = CMBTestingConstants.EMAIL_ENDPOINT;
		String message = "test email";
		CommunicationUtils.sendMessage(user1, protocol, endPoint, message);
		
		protocol = CNSSubscription.CnsSubscriptionProtocol.email_json;
		endPoint = CMBTestingConstants.EMAIL_ENDPOINT;
		message = "test email";
		CommunicationUtils.sendMessage(user1, protocol, endPoint, message);
	}

	@Test
	public void testPublishToInvalidArn() {

		try {	
			
			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessage("test");
			publishRequest.setTopicArn("abc");
			
			try {
				sns1.publish(publishRequest);
			} catch (Exception ex) {
				return;
			}
			
			fail("call did not fail");

		} catch (Exception ex) {
			
			fail(ex.getMessage());
		}
	}  

	@Test
	public void testPublishToMissingArn() {

		try {	
			
			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessage("test");
			
			try {
				sns1.publish(publishRequest);
			} catch (Exception ex) {
				return;
			}
			
			fail("call did not fail");

		} catch (Exception ex) {
			
			fail(ex.getMessage());
		}
	}  

	@Test
	public void testPublishMissingMessage() {
		
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();
			String topicName = "T" + rand.nextLong();
			
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName); 
			topicArn =  sns1.createTopic(createTopicRequest).getTopicArn();

			logger.info("Created topic " + topicArn);

			String queueName = "Q" + rand.nextLong();
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			queueUrl = sqs1.createQueue(createQueueRequest).getQueueUrl();
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			Thread.sleep(1000);

			endPoint = queueArn;

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = sns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setTopicArn(topicArn);
			
			try {
				sns1.publish(publishRequest);
			} catch (Exception ex) {
				return;
			}
			
			fail("call did not fail");

		} catch (Exception ex) {
			
			fail(ex.getMessage());

		} finally {
			
			if (topicArn != null) {
				try {
					DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest();
					deleteTopicRequest.setTopicArn(topicArn);
					sns1.deleteTopic(deleteTopicRequest);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(); 
					deleteQueueRequest.setQueueUrl(queueUrl);
					sqs1.deleteQueue(deleteQueueRequest);
				} catch (Exception ex) { }
			}
		}
	}  

	@Test
	public void testPublishBadMessage() {
		
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();
			String topicName = "T" + rand.nextLong();
			
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName); 
			topicArn =  sns1.createTopic(createTopicRequest).getTopicArn();

			logger.info("Created topic " + topicArn);

			String queueName = "Q" + rand.nextLong();
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			queueUrl = sqs1.createQueue(createQueueRequest).getQueueUrl();
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			Thread.sleep(1000);

			endPoint = queueArn;

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = sns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String messageStructure = "json";
			String message = "boo";

			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessageStructure(messageStructure);
			publishRequest.setMessage(message);
			publishRequest.setTopicArn(topicArn);
			
			try {
				sns1.publish(publishRequest);
			} catch (Exception ex) {
				return;
			}
			
			fail("call did not fail");

		} catch (Exception ex) {
			
			fail(ex.getMessage());

		} finally {
		
			if (topicArn != null) {
				try {
					DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest();
					deleteTopicRequest.setTopicArn(topicArn);
					sns1.deleteTopic(deleteTopicRequest);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(); 
					deleteQueueRequest.setQueueUrl(queueUrl);
					sqs1.deleteQueue(deleteQueueRequest);
				} catch (Exception ex) { }
			}
		}
	}	   

	@Test
	public void testMultipleEndpoints() {
		
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();
			String topicName = "T" + rand.nextLong();
			
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName); 
			topicArn =  sns1.createTopic(createTopicRequest).getTopicArn();

			logger.info("Created topic " + topicArn);

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = sns1.subscribe(subscribeRequest).getSubscriptionArn();
			
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
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			queueUrl = sqs1.createQueue(createQueueRequest).getQueueUrl();
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			Thread.sleep(1000);

			endPoint = queueArn;

			subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn2 = sns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String message = "test message";

			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessage(message);
			publishRequest.setTopicArn(topicArn);
			
			sns1.publish(publishRequest);
			
			Thread.sleep(1000);

			String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			assertTrue("Expected message '" + message + "', instead found " + resp, resp.contains(message));

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(); 
			receiveMessageRequest.setQueueUrl(queueUrl);
			ReceiveMessageResult result = sqs1.receiveMessage(receiveMessageRequest);
			
			assertTrue("No message found", result.getMessages().size() > 0);
			String msg = result.getMessages().get(0).getBody();
			assertTrue("Expected message '" + message + "', instead found " + msg, msg.contains(message));

			String receiptHandle = result.getMessages().get(0).getReceiptHandle();
			assertTrue("Receipt handle is null", receiptHandle != null);
			
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiptHandle);
			sqs1.deleteMessage(deleteMessageRequest);

			UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest(); 
			unsubscribeRequest.setSubscriptionArn(subscriptionArn2);
			sns1.unsubscribe(unsubscribeRequest);

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
			
			if (topicArn != null) {
				try {
					DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest();
					deleteTopicRequest.setTopicArn(topicArn);
					sns1.deleteTopic(deleteTopicRequest);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(); 
					deleteQueueRequest.setQueueUrl(queueUrl);
					sqs1.deleteQueue(deleteQueueRequest);
				} catch (Exception ex) { }
			}
		}
	}	
	
	@Test
	public void testMessageStructure() {
		
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();
			String httpMessage = "test Http servlet 2";
			String cqsMessage = "test CQS servlet 2";
			String topicName = "T" + rand.nextLong();
			
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName); 
			topicArn =  sns1.createTopic(createTopicRequest).getTopicArn();

			logger.info("Created topic " + topicArn);

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = sns1.subscribe(subscribeRequest).getSubscriptionArn();
			
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
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			queueUrl = sqs1.createQueue(createQueueRequest).getQueueUrl();
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			Thread.sleep(1000);

			endPoint = queueArn;

			subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn2 = sns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String messageStructure = "json";
			String message = CNSTestingUtils.generateMultiendpointMessageJson(null, null, "test message", httpMessage, null, cqsMessage);

			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessageStructure(messageStructure);
			publishRequest.setMessage(message);
			publishRequest.setTopicArn(topicArn);
			
			sns1.publish(publishRequest);
			
			Thread.sleep(1000);

			String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			JSONObject json = new JSONObject(resp);

			String resp_message = json.getString("Message");

			assertTrue("Expected message '" + httpMessage + "', instead found " + resp_message, resp_message.equals(httpMessage));

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(); 
			receiveMessageRequest.setQueueUrl(queueUrl);
			ReceiveMessageResult result = sqs1.receiveMessage(receiveMessageRequest);
			
			assertTrue("No message found", result.getMessages().size() > 0);
			String msg = result.getMessages().get(0).getBody();
			assertTrue("Expected message '" + cqsMessage + "', instead found " + resp_message, msg.contains(cqsMessage));

			String receiptHandle = result.getMessages().get(0).getReceiptHandle();
			assertTrue("Receipt handle is null", receiptHandle != null);
			
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiptHandle);
			sqs1.deleteMessage(deleteMessageRequest);

			httpMessage = "test Http servlet 45554";
			cqsMessage = "test CQS servlet 2758";
			
			message = CNSTestingUtils.generateMultiendpointMessageJson(null, null, "test message", httpMessage, null, cqsMessage);

			publishRequest = new PublishRequest();
			publishRequest.setMessageStructure(messageStructure);
			publishRequest.setMessage(message);
			publishRequest.setTopicArn(topicArn);
			
			sns1.publish(publishRequest);

			Thread.sleep(1000);

			resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			json = new JSONObject(resp);
			resp_message = json.getString("Message");

			assertTrue("Expected message '" + httpMessage + "', instead found " + resp_message, resp_message.contains(httpMessage));

			receiveMessageRequest = new ReceiveMessageRequest(); 
			receiveMessageRequest.setQueueUrl(queueUrl);
			result = sqs1.receiveMessage(receiveMessageRequest);

			assertTrue("No message found", result.getMessages().size() > 0);
			msg = result.getMessages().get(0).getBody();
			assertTrue("Expected message '" + cqsMessage + "', instead found " + resp_message, msg.contains(cqsMessage));

			receiptHandle = result.getMessages().get(0).getReceiptHandle();
			assertTrue("Receipt handle is null", receiptHandle != null);
			
			deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiptHandle);
			sqs1.deleteMessage(deleteMessageRequest);
			
			UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest(); 
			unsubscribeRequest.setSubscriptionArn(subscriptionArn2);
			sns1.unsubscribe(unsubscribeRequest);
			
		} catch (Exception ex) {
			
			fail(ex.getMessage());

		} finally {
		
			if (topicArn != null) {
				try {
					DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest();
					deleteTopicRequest.setTopicArn(topicArn);
					sns1.deleteTopic(deleteTopicRequest);
				} catch (Exception e) { }
			}
			
			if (queueUrl != null) {
				try {
					DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(); 
					deleteQueueRequest.setQueueUrl(queueUrl);
					sqs1.deleteQueue(deleteQueueRequest);
				} catch (Exception ex) { }
			}
		}
	}	   
}
