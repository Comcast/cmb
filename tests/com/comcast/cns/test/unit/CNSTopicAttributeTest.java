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

import org.json.JSONObject;
import org.junit.Test;

import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CNSTopicAttributeTest extends CMBAWSBaseTest {

	@Test
	public void testGetAttributes() {

		try {
			
			String topicArn = getTopic(1, USR.USER1);
			
			GetTopicAttributesResult results = cns1.getTopicAttributes(new GetTopicAttributesRequest(topicArn));
			
			assertTrue("Default Effective Delivery Policy missing", results.getAttributes().get("EffectiveDeliveryPolicy") != null);

			String deliveryPolicy = results.getAttributes().get("EffectiveDeliveryPolicy");
			JSONObject dpJson = new JSONObject(deliveryPolicy);
			
			logger.info("Delivery Policy:" + deliveryPolicy);

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

		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail(ex.getMessage());

		}	
	}

	@Test
	public void testSetGetAttributes() {

		try {
			
			String topicArn = this.getTopic(1, USR.USER1);

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
			
			cns1.setTopicAttributes(new SetTopicAttributesRequest(topicArn, "DeliveryPolicy", jsonStr2));
			GetTopicAttributesResult results = cns1.getTopicAttributes(new GetTopicAttributesRequest(topicArn));
			assertTrue("Default Effective Delivery Policy missing", results.getAttributes().get("EffectiveDeliveryPolicy") != null);

			String deliveryPolicy = results.getAttributes().get("EffectiveDeliveryPolicy");
			JSONObject dpJson = new JSONObject(deliveryPolicy);
			
			logger.info("Delivery Policy:" + deliveryPolicy);

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

		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}

	@Test
	public void testDisplayName() {
		
		try {
			
			String topicArn = this.getTopic(1, USR.USER1);
			String displayName = "My Display Name !@#$%^&*()-+";
			cns1.setTopicAttributes(new SetTopicAttributesRequest(topicArn, "DisplayName", displayName));
			GetTopicAttributesResult results = cns1.getTopicAttributes(new GetTopicAttributesRequest(topicArn));
			assertTrue("Display name is " + results.getAttributes().get("DisplayName") + ", expected: " + displayName, results.getAttributes().get("DisplayName").equals(displayName));

		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}
}
