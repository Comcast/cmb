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

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.model.CNSRetryPolicy;
import com.comcast.cns.model.CNSThrottlePolicy;
import com.comcast.cns.model.CNSTopicDeliveryPolicy;
import com.comcast.cns.model.CNSRetryPolicy.CnsBackoffFunction;

public class TopicDeliveryPolicyTest {

	private static Logger logger = Logger.getLogger(TopicDeliveryPolicyTest.class);

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
    }

    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
	   
	 @Test
	 public void testConstructor() {
		 //Test JSON
		 String jsonStr = "{" +
				 "\"http\":" +
				 "{\"disableSubscriptionOverrides\":false," +
				 "\"defaultSicklyRetryPolicy\":" +
				 "{"+
				        "\"minDelayTarget\":10,"+
				        "\"maxDelayTarget\":11,"+
				        "\"numRetries\":42,"+
				        "\"numMaxDelayRetries\": 14,"+
				        "\"numMinDelayRetries\": 13,"+
				        "\"numNoDelayRetries\": 15,"+
				        "\"backoffFunction\": \"geometric\""+
				        "}," +
		 		"\"defaultHealthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":21,"+
		        "\"maxDelayTarget\":22,"+
		        "\"numRetries\":77,"+
		        "\"numMaxDelayRetries\": 24,"+
		        "\"numMinDelayRetries\": 26,"+
		        "\"numNoDelayRetries\": 27,"+
		        "\"backoffFunction\": \"linear\""+
		        "}," +
		        "\"defaultThrottlePolicy\":" +
		        "{ " +
		        "\"maxReceivesPerSecond\":5" +
		        "}" +
		        "}}";
		 
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
		        "\"backoffFunction\": \"linear\""+
		        "}," +
		        "\"defaultThrottlePolicy\":null" +
		        "}}";
		 
		 String jsonStr3 = "{" +
				 "\"http\":" +
				 "{\"disableSubscriptionOverrides\":true," +
				 "\"defaultSicklyRetryPolicy\":null," +
		 		"\"defaultHealthyRetryPolicy\":null," +
		        
		        "\"defaultThrottlePolicy\":null" +
		        "}}";
		 
		 String jsonStr4 = "{" +
				 "\"hdttp\":" +
				 "{\"disableSubscriptionOverrides\":true," +
				 "\"defaultSicklyRetryPolicy\":null," +
		 		"\"defaultHealthyRetryPolicy\":null," +
		        
		        "\"defaultThrottlePolicy\":null" +
		        "}}";
		 
		 String jsonStr5 = "{" +
				 "\"http\":" +
				 "{\"abcd\": 8" +
		        "}}";
		 
		 String jsonStr6 = "{" +
				 "\"http\":" +
				 "{\"disableSubscriptionOverrides\":true," +
				 "\"defaultSicklyRetryPolicy\":null," +
		 		"\"defaultHealthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":3601,"+
		        "\"numRetries\":17,"+
		        "\"numMaxDelayRetries\": 4,"+
		        "\"numMinDelayRetries\": 6,"+
		        "\"numNoDelayRetries\": 7,"+
		        "\"backoffFunction\": \"linear\""+
		        "}," +
		        "\"defaultThrottlePolicy\":null" +
		        "}}";
		 
		 try {
			 JSONObject json = new JSONObject(jsonStr);
			 CNSTopicDeliveryPolicy subpolicy = new CNSTopicDeliveryPolicy(json);
			 CNSRetryPolicy hrp = subpolicy.getDefaultHealthyRetryPolicy();
			 assertTrue(hrp.getMinDelayTarget() == 21);
			 assertTrue(hrp.getMaxDelayTarget() == 22);
			 assertTrue(hrp.getNumRetries() == 77);
			 assertTrue(hrp.getNumMaxDelayRetries() == 24);
			 assertTrue(hrp.getNumMinDelayRetries() == 26);
			 assertTrue(hrp.getNumNoDelayRetries() == 27);
			 assertTrue(hrp.getBackOffFunction() == CnsBackoffFunction.linear);
			 
			 CNSRetryPolicy srp = subpolicy.getDefaultSicklyRetryPolicy();
			 assertTrue(srp.getMinDelayTarget() == 10);
			 assertTrue(srp.getMaxDelayTarget() == 11);
			 assertTrue(srp.getNumRetries() == 42);
			 assertTrue(srp.getNumMaxDelayRetries() == 14);
			 assertTrue(srp.getNumMinDelayRetries() == 13);
			 assertTrue(srp.getNumNoDelayRetries() == 15);
			 assertTrue(srp.getBackOffFunction() == CnsBackoffFunction.geometric);
			 
			 CNSThrottlePolicy dtp = subpolicy.getDefaultThrottlePolicy();
			 assertTrue(dtp != null);
			 assertTrue(dtp.getMaxReceivesPerSecond() == 5);
			 assertTrue(subpolicy.isDisableSubscriptionOverrides() == false);

			 
			 JSONObject json2 = new JSONObject(jsonStr2);
			 CNSTopicDeliveryPolicy subpolicy2 = new CNSTopicDeliveryPolicy(json2);
			 CNSRetryPolicy hrp2 = subpolicy2.getDefaultHealthyRetryPolicy();
			 logger.debug("hrp2: " + hrp2.toString());
			 assertTrue("minDelayTarget != 1", hrp2.getMinDelayTarget() == 1);
			 assertTrue(hrp2.getMaxDelayTarget() == 2);
			 assertTrue(hrp2.getNumRetries() == 17);
			 assertTrue(hrp2.getNumMaxDelayRetries() == 4);
			 assertTrue(hrp2.getNumMinDelayRetries() == 6);
			 assertTrue(hrp2.getNumNoDelayRetries() == 7);
			 assertTrue(hrp2.getBackOffFunction() == CnsBackoffFunction.linear);
			 assertTrue(subpolicy2.getDefaultSicklyRetryPolicy() == null);
			 CNSThrottlePolicy dtp2 = subpolicy2.getDefaultThrottlePolicy();
			 assertTrue(dtp2 != null);
			 assertTrue(dtp2.getMaxReceivesPerSecond() == null);
			 assertTrue(subpolicy2.isDisableSubscriptionOverrides() == true);
			 
			 //Default constructor
			 CNSTopicDeliveryPolicy subpolicy3 = new CNSTopicDeliveryPolicy();
			 assertTrue(subpolicy3 != null);
			 logger.debug("subpolicy3: " + subpolicy3.toString());
			 CNSRetryPolicy hrp3 = subpolicy3.getDefaultHealthyRetryPolicy();
			 assertTrue(hrp3.getMinDelayTarget() == 20);
			 assertTrue(hrp3.getMaxDelayTarget() == 20);
			 assertTrue(hrp3.getNumRetries() == 3);
			 assertTrue(hrp3.getNumMaxDelayRetries() == 0);
			 assertTrue(hrp3.getNumMinDelayRetries() == 0);
			 assertTrue(hrp3.getNumNoDelayRetries() == 0);
			 assertTrue(hrp3.getBackOffFunction() == CnsBackoffFunction.linear);
			 assertTrue(subpolicy3.getDefaultSicklyRetryPolicy() == null);
			 CNSThrottlePolicy dtp3 = subpolicy3.getDefaultThrottlePolicy();
			 assertTrue(dtp3 != null);
			 assertTrue(dtp3.getMaxReceivesPerSecond() == null);
			 assertTrue(subpolicy3.isDisableSubscriptionOverrides() == false);
			 
			 //Default constructor
			 JSONObject json3 = new JSONObject(jsonStr3);
			 CNSTopicDeliveryPolicy subpolicy4 = new CNSTopicDeliveryPolicy(json3);
			 assertTrue(subpolicy4 != null);
			 logger.debug("subpolicy4: " + subpolicy4.toString());
			 CNSRetryPolicy hrp4 = subpolicy4.getDefaultHealthyRetryPolicy();
			 assertTrue(hrp4.getMinDelayTarget() == 20);
			 assertTrue(hrp4.getMaxDelayTarget() == 20);
			 assertTrue(hrp4.getNumRetries() == 3);
			 assertTrue(hrp4.getNumMaxDelayRetries() == 0);
			 assertTrue(hrp4.getNumMinDelayRetries() == 0);
			 assertTrue(hrp4.getNumNoDelayRetries() == 0);
			 assertTrue(hrp4.getBackOffFunction() == CnsBackoffFunction.linear);
			 assertTrue(subpolicy4.getDefaultSicklyRetryPolicy() == null);
			 CNSThrottlePolicy dtp4 = subpolicy4.getDefaultThrottlePolicy();
			 assertTrue(dtp4 != null);
			 assertTrue(dtp4.getMaxReceivesPerSecond() == null);
			 assertTrue(subpolicy4.isDisableSubscriptionOverrides() == true);
			 
			 //Default constructor
			 boolean exceptionOccured = false;
			 try {
				 JSONObject json4 = new JSONObject(jsonStr4);
				 new CNSTopicDeliveryPolicy(json4);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 1:");
					logger.debug(e.getMessage());
				} else {
					assertFalse(true);
				}
			 }
			 assertTrue(exceptionOccured);
			 exceptionOccured = false;
			 
			 //Default constructor
			
			 try {
				 JSONObject json5 = new JSONObject(jsonStr5);
				 new CNSTopicDeliveryPolicy(json5);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 2:");
					logger.debug(e.getMessage());
				} else {
					assertFalse(true);
				}
			 }
			 assertTrue(exceptionOccured);
			 exceptionOccured = false;
			 
			 
			 try {
				 JSONObject json6 = new JSONObject(jsonStr6);
				 new CNSTopicDeliveryPolicy(json6);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 3:");
					logger.debug(e.getMessage());
				} else {
					assertFalse(true);
				}
			 }
			 assertTrue(exceptionOccured);
			 exceptionOccured = false;
			 
			 
		 } catch (Exception e) {
             logger.error("Exception occured", e);
             fail("exception: "+e);
		 }
		 
	 }
	 
	 @Test
	 public void testUpdate() {
		 //Test JSON
		 String jsonStr = "{" +
				 "\"http\":" +
				 "{\"disableSubscriptionOverrides\":false," +
				 "\"defaultSicklyRetryPolicy\":" +
				 "{"+
				        "\"minDelayTarget\":10,"+
				        "\"maxDelayTarget\":11,"+
				        "\"numRetries\":42,"+
				        "\"numMaxDelayRetries\": 14,"+
				        "\"numMinDelayRetries\": 13,"+
				        "\"numNoDelayRetries\": 15,"+
				        "\"backoffFunction\": \"geometric\""+
				        "}," +
		 		"\"defaultHealthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":21,"+
		        "\"maxDelayTarget\":22,"+
		        "\"numRetries\":77,"+
		        "\"numMaxDelayRetries\": 24,"+
		        "\"numMinDelayRetries\": 26,"+
		        "\"numNoDelayRetries\": 27,"+
		        "\"backoffFunction\": \"linear\""+
		        "}," +
		        "\"defaultThrottlePolicy\":" +
		        "{ " +
		        "\"maxReceivesPerSecond\":5" +
		        "}" +
		        "}}";
		 
		 String fakejsonStr = "{" +
				 "\"http\":" +
				 "{\"disableSubscriptionOverrides\":false," +
				 "\"defaultSicklyRetryPolicy\":" +
				 "{"+
				        "\"minDelayTarget\":10,"+
				        "\"maxDelayTarget\":11,"+
				        "\"numRetries\":42,"+
				        "\"numMaxDelayRetries\": 14,"+
				        "\"numMinDelayRetries\": 13,"+
				        "\"numNoDelayRetries\": 15,"+
				        "\"backoffFunction\": \"geometric\""+
				        "}," +
		 		"\"defaultHealthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":21,"+
		        "\"maxDelayTarget\":3601,"+
		        "\"numRetries\":77,"+
		        "\"numMaxDelayRetries\": 24,"+
		        "\"numMinDelayRetries\": 26,"+
		        "\"numNoDelayRetries\": 27,"+
		        "\"backoffFunction\": \"linear\""+
		        "}," +
		        "\"defaultThrottlePolicy\":" +
		        "{ " +
		        "\"maxReceivesPerSecond\":5" +
		        "}" +
		        "}}";
		 
		 String fakejsonStr2 = "{" +
				 "\"http\":" +
				 "{\"disableSubscriptionOverrides\":false," +
				 "\"defaultSicklyRetryPolicy\":" +
				 "{"+
				        "\"minDelayTarget\":10,"+
				        "\"maxDelayTarget\":3601,"+
				        "\"numRetries\":42,"+
				        "\"numMaxDelayRetries\": 14,"+
				        "\"numMinDelayRetries\": 13,"+
				        "\"numNoDelayRetries\": 15,"+
				        "\"backoffFunction\": \"geometric\""+
				        "}," +
		 		"\"defaultHealthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":21,"+
		        "\"maxDelayTarget\":22,"+
		        "\"numRetries\":77,"+
		        "\"numMaxDelayRetries\": 24,"+
		        "\"numMinDelayRetries\": 26,"+
		        "\"numNoDelayRetries\": 27,"+
		        "\"backoffFunction\": \"linear\""+
		        "}," +
		        "\"defaultThrottlePolicy\":" +
		        "{ " +
		        "\"maxReceivesPerSecond\":5" +
		        "}" +
		        "}}";
		 try {
			 boolean exceptionOccured = false;
			 
			 CNSTopicDeliveryPolicy topicPolicy = new CNSTopicDeliveryPolicy();
			 JSONObject json = new JSONObject(jsonStr);
			 topicPolicy.update(json);
			 
			 assertTrue(topicPolicy != null);
			 logger.debug("topicPolicy: " + topicPolicy.toString());
			 CNSRetryPolicy hrp = topicPolicy.getDefaultHealthyRetryPolicy();
			 assertTrue(hrp.getMinDelayTarget() == 21);
			 assertTrue(hrp.getMaxDelayTarget() == 22);
			 assertTrue(hrp.getNumRetries() == 77);
			 assertTrue(hrp.getNumMaxDelayRetries() == 24);
			 assertTrue(hrp.getNumMinDelayRetries() == 26);
			 assertTrue(hrp.getNumNoDelayRetries() == 27);
			 assertTrue(hrp.getBackOffFunction() == CnsBackoffFunction.linear);
			 CNSRetryPolicy srp = topicPolicy.getDefaultSicklyRetryPolicy();
			 assertTrue(srp.getMinDelayTarget() == 10);
			 assertTrue(srp.getMaxDelayTarget() == 11);
			 assertTrue(srp.getNumRetries() == 42);
			 assertTrue(srp.getNumMaxDelayRetries() == 14);
			 assertTrue(srp.getNumMinDelayRetries() == 13);
			 assertTrue(srp.getNumNoDelayRetries() == 15);
			 assertTrue(srp.getBackOffFunction() == CnsBackoffFunction.geometric);
			 CNSThrottlePolicy dtp3 = topicPolicy.getDefaultThrottlePolicy();
			 assertTrue(dtp3 != null);
			 assertTrue(dtp3.getMaxReceivesPerSecond() == 5);
			 assertTrue(topicPolicy.isDisableSubscriptionOverrides() == false);
			 
			 try {
				 JSONObject json2 = new JSONObject(fakejsonStr);
				 new CNSTopicDeliveryPolicy(json2);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 3:");
					logger.debug(e.getMessage());
				} else {
					assertFalse(true);
				}
			 }
			 assertTrue(exceptionOccured);
			 exceptionOccured = false;
			 
			 try {
				 JSONObject json2 = new JSONObject(fakejsonStr2);
				 new CNSTopicDeliveryPolicy(json2);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 3:");
					logger.debug(e.getMessage());
				} else {
					assertFalse(true);
				}
			 }
			 assertTrue(exceptionOccured);
			 exceptionOccured = false;
			 
		 } catch (Exception e) {
             logger.error("Exception occured", e);
             fail("exception: "+e);
		 }
		 
	 }
	 
	 @Test
	 public void testGetterSetter() {
		 CNSTopicDeliveryPolicy topicPolicy = new CNSTopicDeliveryPolicy();
		 CNSRetryPolicy hrp = new CNSRetryPolicy();
		 hrp.setNumRetries(19);
		 CNSRetryPolicy srp = new CNSRetryPolicy();
		 CNSThrottlePolicy dtp = new CNSThrottlePolicy();
		 
		 dtp.setMaxReceivesPerSecond(19);
		 
		 srp.setBackOffFunction(CnsBackoffFunction.geometric);
		 topicPolicy.setDefaultHealthyRetryPolicy(hrp);
		 topicPolicy.setDefaultSicklyRetryPolicy(srp);
		 topicPolicy.setDefaultThrottlePolicy(dtp);
		 topicPolicy.setDisableSubscriptionOverrides(true);
		 
		 CNSRetryPolicy nhrp = topicPolicy.getDefaultHealthyRetryPolicy();
		 CNSRetryPolicy nsrp = topicPolicy.getDefaultSicklyRetryPolicy();
		 CNSThrottlePolicy ndtp = topicPolicy.getDefaultThrottlePolicy();
		 assertTrue("subscription Overrides not set", topicPolicy.isDisableSubscriptionOverrides());
		 assertTrue("default healthy retry policy not set correctly", nhrp.getMinDelayTarget() == 20);
		 assertTrue("default healthy retry policy not set correctly",nhrp.getMaxDelayTarget() == 20);
		 assertTrue("default healthy retry policy not set correctly",nhrp.getNumRetries() == 19);
		 assertTrue("default healthy retry policy not set correctly",nhrp.getNumMaxDelayRetries() == 0);
		 assertTrue("default healthy retry policy not set correctly",nhrp.getNumMinDelayRetries() == 0);
		 assertTrue("default healthy retry policy not set correctly",nhrp.getNumNoDelayRetries() == 0);
		 assertTrue("default healthy retry policy not set correctly",nhrp.getBackOffFunction() == CnsBackoffFunction.linear);
		 
		 assertTrue("default sickly retry policy not set correctly", nsrp.getMinDelayTarget() == 20);
		 assertTrue("default sickly retry policy not set correctly",nsrp.getMaxDelayTarget() == 20);
		 assertTrue("default sickly retry policy not set correctly",nsrp.getNumRetries() == 3);
		 assertTrue("default sickly retry policy not set correctly",nsrp.getNumMaxDelayRetries() == 0);
		 assertTrue("default sickly retry policy not set correctly",nsrp.getNumMinDelayRetries() == 0);
		 assertTrue("default sickly retry policy not set correctly",nsrp.getNumNoDelayRetries() == 0);
		 assertTrue("default sickly retry policy not set correctly",nsrp.getBackOffFunction() == CnsBackoffFunction.geometric);
		 
		 assertTrue("defaultThrottlePolicy not set correctly", ndtp.getMaxReceivesPerSecond() == 19);
		 try {
			 JSONObject json = topicPolicy.toJSON();
			 logger.debug("Json is:" + json.toString());
			 assertTrue("toJSON fails to return proper JSON", json.has("http"));
			 JSONObject httpJson = json.getJSONObject("http");
			 assertTrue("toJSON fails to return proper JSON", httpJson.has("defaultHealthyRetryPolicy"));
			 assertTrue("toJSON fails to return proper JSON", httpJson.has("defaultSicklyRetryPolicy"));
			 assertTrue("toJSON fails to return proper JSON", httpJson.has("defaultThrottlePolicy"));
			 assertTrue("toJSON fails to return proper JSON", httpJson.has("disableSubscriptionOverrides"));
			 JSONObject hrpJson = httpJson.getJSONObject("defaultHealthyRetryPolicy");
			 JSONObject srpJson = httpJson.getJSONObject("defaultSicklyRetryPolicy");
			 JSONObject tpJson = httpJson.getJSONObject("defaultThrottlePolicy");
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.has("backoffFunction"));
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.get("backoffFunction").equals("linear"));
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.has("numMinDelayRetries"));
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.getInt("numMinDelayRetries") == 0);
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.has("numMaxDelayRetries"));
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.getInt("numMaxDelayRetries") == 0);
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.has("numRetries"));
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.getInt("numRetries") == 19);
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.has("minDelayTarget"));
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.getInt("minDelayTarget") == 20);
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.has("numNoDelayRetries"));
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.getInt("numNoDelayRetries") == 0);
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.has("maxDelayTarget"));
			 assertTrue("toJSON fails to return proper defaultHealthyRetryPolicy", hrpJson.getInt("maxDelayTarget") == 20);
			 
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.has("backoffFunction"));
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.get("backoffFunction").equals("geometric"));
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.has("numMinDelayRetries"));
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.getInt("numMinDelayRetries") == 0);
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.has("numMaxDelayRetries"));
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.getInt("numMaxDelayRetries") == 0);
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.has("numRetries"));
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.getInt("numRetries") == 3);
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.has("minDelayTarget"));
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.getInt("minDelayTarget") == 20);
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.has("numNoDelayRetries"));
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.getInt("numNoDelayRetries") == 0);
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.has("maxDelayTarget"));
			 assertTrue("toJSON fails to return proper defaultSicklyRetryPolicy", srpJson.getInt("maxDelayTarget") == 20);
			 
			 assertTrue("toJSON fails to return proper defaultThrottlePolicy", tpJson.has("maxReceivesPerSecond"));
			 assertTrue("toJSON fails to return proper defaultThrottlePolicy", tpJson.getInt("maxReceivesPerSecond") == 19);
			 assertTrue("toJSON fails to return proper JSON", httpJson.getBoolean("disableSubscriptionOverrides") == true);
		 } catch (Exception e) {
             logger.error("Exception occured", e);
             fail("exception: "+e);
		 }
	 }
}
