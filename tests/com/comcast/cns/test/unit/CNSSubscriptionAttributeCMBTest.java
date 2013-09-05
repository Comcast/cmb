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

import java.util.Arrays;

import org.json.JSONObject;
import org.junit.Test;

import com.amazonaws.services.sns.model.AddPermissionRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.comcast.cmb.common.util.XmlUtil;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cmb.test.tools.CNSTestingUtils;

public class CNSSubscriptionAttributeCMBTest extends CMBAWSBaseTest {
	
	@Test
	public void testGetAttributes() {

		try {
			
			String topicArn = getTopic(1, USR.USER1);

			String endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endpoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String lastMessageUrl = endpoint.replace("recv", "info") + "?showLast=true";

			if (subscriptionArn.equals("pending confirmation")) {
				String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");
    			JSONObject o = new JSONObject(resp);
    			if (!o.has("SubscribeURL")) {
    				fail("Message is not a confirmation messsage");
    			}
    			String subscriptionUrl = o.getString("SubscribeURL");
				resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
				logger.info(resp);
				subscriptionArn = XmlUtil.getCurrentLevelTextValue(XmlUtil.getChildNodes(XmlUtil.buildDoc(resp)).get(0), "SubscriptionArn");
			}

			GetSubscriptionAttributesResult result = cns1.getSubscriptionAttributes(new GetSubscriptionAttributesRequest(subscriptionArn));

			JSONObject effectiveDeliveryPolicy = new JSONObject(result.getAttributes().get("EffectiveDeliveryPolicy"));
			
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
			assertTrue(result.getAttributes().get("ConfirmationWasAuthenticated").equals("false"));
			assertTrue(result.getAttributes().get("TopicArn").equals(topicArn));
			assertTrue(result.getAttributes().get("SubscriptionArn").equals(subscriptionArn));
			
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}

	@Test
	public void testSetGetAttributes() {

		try {

			String topicArn = getTopic(1, USR.USER1);

			String endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endpoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String lastMessageUrl = endpoint.replace("recv", "info") + "?showLast=true";

			if (subscriptionArn.equals("pending confirmation")) {
				String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");
    			JSONObject o = new JSONObject(resp);
    			if (!o.has("SubscribeURL")) {
    				fail("Message is not a confirmation messsage");
    			}
    			String subscriptionUrl = o.getString("SubscribeURL");
				resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
				logger.info(resp);
				subscriptionArn = XmlUtil.getCurrentLevelTextValue(XmlUtil.getChildNodes(XmlUtil.buildDoc(resp)).get(0), "SubscriptionArn");
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
			
			cns1.setSubscriptionAttributes(new SetSubscriptionAttributesRequest(subscriptionArn, attributeName, attributeValue));

			GetSubscriptionAttributesResult result = cns1.getSubscriptionAttributes(new GetSubscriptionAttributesRequest(subscriptionArn));

			JSONObject effectiveDeliveryPolicy = new JSONObject(result.getAttributes().get("EffectiveDeliveryPolicy"));
			
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
			assertTrue(result.getAttributes().get("ConfirmationWasAuthenticated").equals("false"));

			JSONObject deliveryPolicy = new JSONObject(result.getAttributes().get("DeliveryPolicy"));
			
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

			assertTrue(result.getAttributes().get("TopicArn").equals(topicArn));
			assertTrue(result.getAttributes().get("SubscriptionArn").equals(subscriptionArn));
			
		} catch (Exception e) {
			logger.error("test failed", e);
			fail(e.getMessage());
		}
	}

	@Test
	public void testSetGetAttributesWithPermissions() {
		
		try {
			
			String topicArn = getTopic(1, USR.USER1);
			
			cns1.addPermission(new AddPermissionRequest(topicArn, "p1", Arrays.asList("*") , Arrays.asList("*")));

			String endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endpoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn1 = cns2.subscribe(subscribeRequest).getSubscriptionArn();
			
			String lastMessageUrl = endpoint.replace("recv", "info") + "?showLast=true";

			if (subscriptionArn1.equals("pending confirmation")) {
				String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");
    			JSONObject o = new JSONObject(resp);
    			if (!o.has("SubscribeURL")) {
    				fail("Message is not a confirmation messsage");
    			}
    			String subscriptionUrl = o.getString("SubscribeURL");
				resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
				logger.info(resp);
				subscriptionArn1 = XmlUtil.getCurrentLevelTextValue(XmlUtil.getChildNodes(XmlUtil.buildDoc(resp)).get(0), "SubscriptionArn");
			}

			endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();

			subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endpoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn2 = cns2.subscribe(subscribeRequest).getSubscriptionArn();
			
			lastMessageUrl = endpoint.replace("recv", "info") + "?showLast=true";

			if (subscriptionArn2.equals("pending confirmation")) {
				String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");
    			JSONObject o = new JSONObject(resp);
    			if (!o.has("SubscribeURL")) {
    				fail("Message is not a confirmation messsage");
    			}
    			String subscriptionUrl = o.getString("SubscribeURL");
				resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
				logger.info(resp);
				subscriptionArn2 = XmlUtil.getCurrentLevelTextValue(XmlUtil.getChildNodes(XmlUtil.buildDoc(resp)).get(0), "SubscriptionArn");
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

			cns1.setTopicAttributes(new SetTopicAttributesRequest(topicArn, attributeName, attributeValue));

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

			cns2.setSubscriptionAttributes(new SetSubscriptionAttributesRequest(subscriptionArn1, attributeName2, attributeValue2));

			GetSubscriptionAttributesResult result = cns3.getSubscriptionAttributes(new GetSubscriptionAttributesRequest(subscriptionArn2));

			{
				JSONObject effectiveDeliveryPolicy = new JSONObject(result.getAttributes().get("EffectiveDeliveryPolicy"));
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

				assertTrue(result.getAttributes().get("ConfirmationWasAuthenticated").equals("false"));

				JSONObject deliveryPolicy = new JSONObject(result.getAttributes().get("DeliveryPolicy"));
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

				assertTrue(result.getAttributes().get("TopicArn").equals(topicArn));
				assertTrue(result.getAttributes().get("SubscriptionArn").equals(subscriptionArn2));
			}

			result = cns2.getSubscriptionAttributes(new GetSubscriptionAttributesRequest(subscriptionArn1));

			{
				JSONObject effectiveDeliveryPolicy = new JSONObject(result.getAttributes().get("EffectiveDeliveryPolicy"));

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

				assertTrue(result.getAttributes().get("ConfirmationWasAuthenticated").equals("false"));

				JSONObject deliveryPolicy = new JSONObject(result.getAttributes().get("DeliveryPolicy"));
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

				assertTrue(result.getAttributes().get("TopicArn").equals(topicArn));

				assertTrue(result.getAttributes().get("SubscriptionArn").equals(subscriptionArn1));
			}

		} catch (Exception e) {
			logger.error("test failed", e);
			fail(e.getMessage());
		}
	}

	@Test
	public void testBadSetAttributes() {

		try {

			String topicArn = getTopic(1, USR.USER1);

			String endpoint = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + rand.nextInt();

			SubscribeRequest subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endpoint);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			String subscriptionArn = cns1.subscribe(subscribeRequest).getSubscriptionArn();
			
			String lastMessageUrl = endpoint.replace("recv", "info") + "?showLast=true";

			if (subscriptionArn.equals("pending confirmation")) {
				String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");
    			JSONObject o = new JSONObject(resp);
    			if (!o.has("SubscribeURL")) {
    				fail("Message is not a confirmation messsage");
    			}
    			String subscriptionUrl = o.getString("SubscribeURL");
				resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
				logger.info(resp);
				subscriptionArn = XmlUtil.getCurrentLevelTextValue(XmlUtil.getChildNodes(XmlUtil.buildDoc(resp)).get(0), "SubscriptionArn");
			}

			String attributeName = "DeliveryPolicy";
			String attributeValue = null;
			
			try {
				cns1.setSubscriptionAttributes(new SetSubscriptionAttributesRequest(subscriptionArn, attributeName, attributeValue));
				fail("exception expected");
			} catch (Exception ex) {
				logger.info("expected exception", ex);
			}

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

			try {
				cns1.setSubscriptionAttributes(new SetSubscriptionAttributesRequest(subscriptionArn, attributeName, attributeValue));
				fail("exception expected");
			} catch (Exception ex) {
				// DeliveryPolicy: healthyRetryPolicy.maxDelayTarget must be specified
				logger.info("expected exception", ex);
			}

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
			
			try {
				cns1.setSubscriptionAttributes(new SetSubscriptionAttributesRequest(subscriptionArn, attributeName, attributeValue));
				fail("exception expected");
			} catch (Exception ex) {
				// DeliveryPolicy: healthyRetryPolicy.maxDelayTarget must be greater than or equal to minDelayTarget
				logger.info("expected exception", ex);
			}

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

			try {
				cns1.setSubscriptionAttributes(new SetSubscriptionAttributesRequest(subscriptionArn, attributeName, attributeValue));
				fail("exception expected");
			} catch (Exception ex) {
				// DeliveryPolicy: healthyRetryPolicy.numRetries must be greater than or equal to total of numMinDelayRetries, numNoDelayRetries and numMaxDelayRetries
				logger.info("expected exception", ex);
			}

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

			try {
				cns1.setSubscriptionAttributes(new SetSubscriptionAttributesRequest(subscriptionArn, attributeName, attributeValue));
				fail("exception expected");
			} catch (Exception ex) {
				// DeliveryPolicy: sicklyRetryPolicy.numRetries must be greater than or equal to total of numMinDelayRetries, numNoDelayRetries and numMaxDelayRetries
				logger.info("expected exception", ex);
			}

		} catch (Exception e) {
			logger.error("test failed", e);
			fail(e.getMessage());
		}
	}

	@Test
	public void testBadGetAttributes() {

		try {

			try {
				GetSubscriptionAttributesResult result = cns1.getSubscriptionAttributes(new GetSubscriptionAttributesRequest(null));
				fail("exception expected");
			} catch (Exception ex) {
				logger.info("expected exception", ex);
			}
			
		} catch (Exception e) {
			logger.error("Exception occured", e);
			fail("exception: "+e);
		}
	}
}
