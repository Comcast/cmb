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

import java.util.Map;
import java.util.Random;
import org.json.JSONObject;
import org.junit.Test;

import com.amazonaws.services.sns.model.GetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cmb.test.tools.CNSTestingUtils;

import static org.junit.Assert.*;

public class CNSRawMessageDeliveryTest extends CMBAWSBaseTest {
	
	private static Random rand = new Random();
	
	@Test
	public void testRawMessageDelivery() throws Exception {
		
        // create topic
		String topicArn = getTopic(1, USR.USER1);
		
		// subscribe and confirm http endpoint to receive raw message
		String id = rand.nextLong() + "";
		
		String rawEndPointUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + id;
		String rawEndPointLastMessageUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "info/" + id + "?showLast=true";
		
		SubscribeRequest rawEndPointSubscribeRequest = new SubscribeRequest();
		rawEndPointSubscribeRequest.setEndpoint(rawEndPointUrl);
		rawEndPointSubscribeRequest.setProtocol("http");
		rawEndPointSubscribeRequest.setTopicArn(topicArn);
		SubscribeResult subscribeResult = cns1.subscribe(rawEndPointSubscribeRequest);
		String rawEndPointsubscriptionArn = subscribeResult.getSubscriptionArn();
		
		if (rawEndPointsubscriptionArn.equals("pending confirmation")) {
			
			Thread.sleep(500);				
			String response = CNSTestingUtils.sendHttpMessage(rawEndPointLastMessageUrl, "");
			logger.info(response);
			JSONObject o = new JSONObject(response);
			if (!o.has("SubscribeURL")) {
				throw new Exception("message is not a confirmation messsage");
			}
			String subscriptionUrl = o.getString("SubscribeURL");
			response = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
			
			String startTag = "<SubscriptionArn>";
			String endTag = "</SubscriptionArn>";
			int startIndex = response.indexOf(startTag);
			int endIndex = response.indexOf(endTag);
			String subArn = response.substring(startIndex + startTag.length(), endIndex);
			if (subArn != null && !subArn.isEmpty()) {
				rawEndPointsubscriptionArn = subArn;
			}
			logger.info("Raw-message EndPoint subscription Arn after confirmation: " + rawEndPointsubscriptionArn);
		}
		
		// set subscription attribute for raw message delivery
		
		Boolean rawMessageDelivery = true;
		try {
			SetSubscriptionAttributesRequest setSubscriptionAttributesRequest = new SetSubscriptionAttributesRequest(rawEndPointsubscriptionArn, "RawMessageDelivery", rawMessageDelivery.toString());
			cns1.setSubscriptionAttributes(setSubscriptionAttributesRequest);
			
			Map<String, String> attributes = null;
			GetSubscriptionAttributesRequest getSubscriptionAttributesRequest = new GetSubscriptionAttributesRequest(rawEndPointsubscriptionArn);
			GetSubscriptionAttributesResult getSubscriptionAttributesResult = cns1.getSubscriptionAttributes(getSubscriptionAttributesRequest);
			attributes = getSubscriptionAttributesResult.getAttributes();
			String rawMessageDeliveryStr = attributes.get("RawMessageDelivery");
			if (rawMessageDeliveryStr != null && !rawMessageDeliveryStr.isEmpty()) {
				rawMessageDelivery = Boolean.parseBoolean(rawMessageDeliveryStr);
				assertTrue("Set raw message delivery successful", rawMessageDelivery);
			} else {
				fail("no raw message delivery flag found");
			}
			logger.info("Raw Message Delivery attribute:" + rawMessageDeliveryStr);
		} catch (Exception ex) {
			throw new Exception("Can't set raw message delivery attribute to subscription arn " + rawEndPointsubscriptionArn);
		}
		
		// subscribe and confirm http endpoint to receive JSON message
		id = rand.nextLong() + "";
		String jsonEndPointUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + id;
		String jsonEndPointLastMessageUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "info/" + id + "?showLast=true";
		
		SubscribeRequest jsonEndPointSubscribeRequest = new SubscribeRequest();
		jsonEndPointSubscribeRequest.setEndpoint(jsonEndPointUrl);
		jsonEndPointSubscribeRequest.setProtocol("http");
		jsonEndPointSubscribeRequest.setTopicArn(topicArn);
		SubscribeResult jsonSubscribeResult = cns1.subscribe(jsonEndPointSubscribeRequest);
		String jsonEndPointsubscriptionArn = jsonSubscribeResult.getSubscriptionArn();
		logger.info("JSON EndPoint subscription arn:" + jsonEndPointsubscriptionArn);
		
		if (jsonEndPointsubscriptionArn.equals("pending confirmation")) {
			
			Thread.sleep(500);				
			String response = CNSTestingUtils.sendHttpMessage(jsonEndPointLastMessageUrl, "");
			JSONObject o = new JSONObject(response);
			if (!o.has("SubscribeURL")) {
				throw new Exception("message is not a confirmation messsage");
			}
			String subscriptionUrl = o.getString("SubscribeURL");
			response = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
			
			String startTag = "<SubscriptionArn>";
			String endTag = "</SubscriptionArn>";
			int startIndex = response.indexOf(startTag);
			int endIndex = response.indexOf(endTag);
			String subArn = response.substring(startIndex + startTag.length(), endIndex);
			if (subArn != null && !subArn.isEmpty()) {
				jsonEndPointsubscriptionArn = subArn;
			}
			logger.info("JSON EndPoint subscription arn after confirmation:" + jsonEndPointsubscriptionArn);
		}
		
		// publish and receive message
		
		String messageText = "Pulish a raw message";
		PublishRequest publishRequest = new PublishRequest();
		publishRequest.setMessage(messageText);
		publishRequest.setSubject("unit test raw message");
		publishRequest.setTopicArn(topicArn);
		cns1.publish(publishRequest);
		
		Thread.sleep(500);

		// check raw message is received			
		String response = CNSTestingUtils.sendHttpMessage(rawEndPointLastMessageUrl, "");
		logger.info("Reponse of raw-message endpoint:" + response);
		if (response != null && response.length() > 0) {
			assertEquals("Receive raw message", response, messageText);				
		} else {
			fail("no messages found");
		}
		
		// check json message is received
		response = CNSTestingUtils.sendHttpMessage(jsonEndPointLastMessageUrl, "");
		logger.info("Reponse of JSON-message endpoint: " + response);
		if (response != null && response.length() > 0) {
			try {
				JSONObject obj = new JSONObject(response);
			} catch (Exception ex) {
				fail("message not surrounded by json envelope");
			}
		} else {
			fail("no messages found");
		}
	}
}
