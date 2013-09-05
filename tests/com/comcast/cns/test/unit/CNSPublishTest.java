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

import java.util.Random;

import org.json.JSONObject;
import org.junit.Test;

import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cmb.test.tools.CNSTestingUtils;

public class CNSPublishTest extends CMBAWSBaseTest {
	
	private Random rand = new Random();

	@Test
	public void testPublishToInvalidArn() {

		try {	
			
			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessage("test");
			publishRequest.setTopicArn("abc");
			
			try {
				cns1.publish(publishRequest);
			} catch (Exception ex) {
				return;
			}
			
			fail("call did not fail");

		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}  

	@Test
	public void testPublishToMissingArn() {

		try {	
			
			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessage("test");
			
			try {
				cns1.publish(publishRequest);
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
			
			topicArn =  getTopic(1, USR.USER1);
			queueUrl = getQueueUrl(1, USR.USER1);
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			Thread.sleep(1000);

			String endPoint = queueArn;

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setTopicArn(topicArn);
			
			try {
				cns1.publish(publishRequest);
			} catch (Exception ex) {
				return;
			}
			
			fail("call did not fail");

		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}  

	@Test
	public void testPublishBadMessage() {
		
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();

			topicArn =  getTopic(1, USR.USER1);
			queueUrl = getQueueUrl(1, USR.USER1);
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
			
			Thread.sleep(1000);

			endPoint = queueArn;

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String messageStructure = "json";
			String message = "boo";

			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessageStructure(messageStructure);
			publishRequest.setMessage(message);
			publishRequest.setTopicArn(topicArn);
			
			try {
				cns1.publish(publishRequest);
			} catch (Exception ex) {
				return;
			}
			
			fail("call did not fail");

		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}	   

	@Test
	public void testMultipleEndpoints() {
		
		String topicArn = null;
		String queueUrl = null;
		String queueArn = null;
		
		try {	
			
			String endPoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();

			topicArn =  getTopic(1, USR.USER1);
			queueUrl = getQueueUrl(1, USR.USER1);
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);

			Thread.sleep(1000);

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
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
			
			endPoint = queueArn;

			subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn2 = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String message = "test message";

			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessage(message);
			publishRequest.setTopicArn(topicArn);
			
			cns1.publish(publishRequest);
			
			Thread.sleep(1000);

			String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			assertTrue("Expected message '" + message + "', instead found " + resp, resp.contains(message));

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(); 
			receiveMessageRequest.setQueueUrl(queueUrl);
			ReceiveMessageResult result = cqs1.receiveMessage(receiveMessageRequest);
			
			assertTrue("No message found", result.getMessages().size() > 0);
			String msg = result.getMessages().get(0).getBody();
			assertTrue("Expected message '" + message + "', instead found " + msg, msg.contains(message));

			String receiptHandle = result.getMessages().get(0).getReceiptHandle();
			assertTrue("Receipt handle is null", receiptHandle != null);
			
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiptHandle);
			cqs1.deleteMessage(deleteMessageRequest);

			UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest(); 
			unsubscribeRequest.setSubscriptionArn(subscriptionArn2);
			cns1.unsubscribe(unsubscribeRequest);

		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
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

			topicArn =  getTopic(1, USR.USER1);
			queueUrl = getQueueUrl(1, USR.USER1);
			queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);

			Thread.sleep(1000);

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
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
			
			endPoint = queueArn;

			subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPoint);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn2 = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String messageStructure = "json";
			String message = CNSTestingUtils.generateMultiendpointMessageJson(null, null, "test message", httpMessage, null, cqsMessage);

			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessageStructure(messageStructure);
			publishRequest.setMessage(message);
			publishRequest.setTopicArn(topicArn);
			
			cns1.publish(publishRequest);
			
			Thread.sleep(1000);

			String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			JSONObject json = new JSONObject(resp);

			String resp_message = json.getString("Message");

			assertTrue("Expected message '" + httpMessage + "', instead found " + resp_message, resp_message.equals(httpMessage));

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(); 
			receiveMessageRequest.setQueueUrl(queueUrl);
			ReceiveMessageResult result = cqs1.receiveMessage(receiveMessageRequest);
			
			assertTrue("No message found", result.getMessages().size() > 0);
			String msg = result.getMessages().get(0).getBody();
			assertTrue("Expected message '" + cqsMessage + "', instead found " + resp_message, msg.contains(cqsMessage));

			String receiptHandle = result.getMessages().get(0).getReceiptHandle();
			assertTrue("Receipt handle is null", receiptHandle != null);
			
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiptHandle);
			cqs1.deleteMessage(deleteMessageRequest);

			httpMessage = "test Http servlet 45554";
			cqsMessage = "test CQS servlet 2758";
			
			message = CNSTestingUtils.generateMultiendpointMessageJson(null, null, "test message", httpMessage, null, cqsMessage);

			publishRequest = new PublishRequest();
			publishRequest.setMessageStructure(messageStructure);
			publishRequest.setMessage(message);
			publishRequest.setTopicArn(topicArn);
			
			cns1.publish(publishRequest);

			Thread.sleep(1000);

			resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");

			json = new JSONObject(resp);
			resp_message = json.getString("Message");

			assertTrue("Expected message '" + httpMessage + "', instead found " + resp_message, resp_message.contains(httpMessage));

			receiveMessageRequest = new ReceiveMessageRequest(); 
			receiveMessageRequest.setQueueUrl(queueUrl);
			result = cqs1.receiveMessage(receiveMessageRequest);

			assertTrue("No message found", result.getMessages().size() > 0);
			msg = result.getMessages().get(0).getBody();
			assertTrue("Expected message '" + cqsMessage + "', instead found " + resp_message, msg.contains(cqsMessage));

			receiptHandle = result.getMessages().get(0).getReceiptHandle();
			assertTrue("Receipt handle is null", receiptHandle != null);
			
			deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiptHandle);
			cqs1.deleteMessage(deleteMessageRequest);
			
			UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest(); 
			unsubscribeRequest.setSubscriptionArn(subscriptionArn2);
			cns1.unsubscribe(unsubscribeRequest);
			
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}	   
}
