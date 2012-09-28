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
import com.comcast.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.cns.model.CNSThrottlePolicy;
import com.comcast.cns.model.CNSRetryPolicy.CnsBackoffFunction;

public class SubscriptionDeliveryPolicyTest {

	private static Logger logger = Logger.getLogger(SubscriptionDeliveryPolicyTest.class);

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
		 String jsonStr = "{\"healthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":2,"+
		        "\"numRetries\":10,"+
		        "\"numMaxDelayRetries\": 4,"+
		        "\"numMinDelayRetries\": 6,"+
		        "\"backoffFunction\": \"linear\""+
		        "}," +
		        "\"throttlePolicy\":" +
		        "{" +
		        "\"maxReceivesPerSecond\":5" +
	 			"}" +
		        "}";
		 
		 String jsonStr2 = "{\"sicklyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":2,"+
		        "\"numRetries\":10,"+
		        "\"numMaxDelayRetries\": 4,"+
		        "\"numMinDelayRetries\": 6,"+
		        "\"backoffFunction\": \"exponential\""+
		        "}" +
		        "" +
		        "}";
		 String jsonStr3 = "{}";
		 
		 String jsonStr4 = "{\"healthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":2,"+
		        "\"numRetries\":\"cookie\"," +
		        "\"numMaxDelayRetries\": 4,"+
		        "\"numMinDelayRetries\": 6,"+
		        "\"backoffFunction\": \"linear\""+
		        "}" +
		        "}";
		 
		 String jsonStr5 = "{\"sicklyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":2,"+
		        "\"numRetries\":\"cookie\"," +
		        "\"numMaxDelayRetries\": 4,"+
		        "\"numMinDelayRetries\": 6,"+
		        "\"backoffFunction\": \"linear\""+
		        "}" +
		        "}";
		 
		 String jsonStr6 = "{" +		 
				 "\"throttlePolicy\":" +
			        "{" +
			        "\"maxReceivesPerSecond\":\"stand\"" +
		 			"}" +
		        "}";
		 
		 String jsonStr7 = "{\"healthyRetryPolicy\":" +				 
				 "{"+
			        "\"minDelayTarget\":1,"+
			        "\"maxDelayTarget\":2,"+
			        "\"numRetries\":10,"+
			        "\"numMaxDelayRetries\": 4,"+
			        "\"numMinDelayRetries\": 6,"+
			        "\"backoffFunction\": \"linear\""+
			        "}," +
			        "\"throttlePolicy\":" +
			        "{" +
			        "\"maxReceivesPerSecond\":5" +
		 			"}," +
		 			"\"maximalPolicy\":" +
			        "{" +
			        "\"mx\":7" +
		 			"}" +
			        "}";
		 
		 String jsonStr8 = "{\"healthyRetryPolicy\":" +				 
				 "{"+
			        "\"minDelayTarget\":1,"+
			        "\"maxDelayTarget\":3601,"+
			        "\"numRetries\":10,"+
			        "\"numMaxDelayRetries\": 4,"+
			        "\"numMinDelayRetries\": 6,"+
			        "\"backoffFunction\": \"linear\""+
			        "}," +
			        "\"throttlePolicy\":" +
			        "{" +
			        "\"maxReceivesPerSecond\":5" +
		 			"}" +
		 			
			        "}";
		 
		 try {
			 JSONObject json = new JSONObject(jsonStr);
			 CNSSubscriptionDeliveryPolicy subpolicy = new CNSSubscriptionDeliveryPolicy(json);
			 CNSRetryPolicy hRetryPolicy = subpolicy.getHealthyRetryPolicy();
			 assertTrue(hRetryPolicy.getMinDelayTarget() == 1);
			 assertTrue(hRetryPolicy.getMaxDelayTarget() == 2);
			 assertTrue(hRetryPolicy.getNumRetries() == 10);
			 assertTrue(hRetryPolicy.getNumMaxDelayRetries() == 4);
			 assertTrue(hRetryPolicy.getNumMinDelayRetries() == 6);
			 assertTrue(hRetryPolicy.getBackOffFunction() == CnsBackoffFunction.linear);
			 CNSThrottlePolicy tPolicy = subpolicy.getThrottlePolicy();
			 assertTrue(tPolicy.getMaxReceivesPerSecond() == 5);
			 assertTrue(subpolicy.getSicklyRetryPolicy() == null);
			 
			 //Test 2nd Json constructor
			 JSONObject json2 = new JSONObject(jsonStr2);
			 CNSSubscriptionDeliveryPolicy subpolicy3 = new CNSSubscriptionDeliveryPolicy(json2);
			 hRetryPolicy = subpolicy3.getHealthyRetryPolicy();
			 assertTrue(hRetryPolicy.getMinDelayTarget() == 20);
			 assertTrue(hRetryPolicy.getMaxDelayTarget() == 20);
			 assertTrue(hRetryPolicy.getNumRetries() == 3);
			 assertTrue(hRetryPolicy.getNumMaxDelayRetries() == 0);
			 assertTrue(hRetryPolicy.getNumMinDelayRetries() == 0);
			 assertTrue(hRetryPolicy.getBackOffFunction() == CnsBackoffFunction.linear);
			 CNSThrottlePolicy tPolicy2 = subpolicy3.getThrottlePolicy();
			 assertTrue(tPolicy2.getMaxReceivesPerSecond() == null);
			 CNSRetryPolicy sRetryPolicy = subpolicy3.getSicklyRetryPolicy();
			 assertTrue(sRetryPolicy.getMinDelayTarget() == 1);
			 assertTrue(sRetryPolicy.getMaxDelayTarget() == 2);
			 assertTrue(sRetryPolicy.getNumRetries() == 10);
			 assertTrue(sRetryPolicy.getNumMaxDelayRetries() == 4);
			 assertTrue(sRetryPolicy.getNumMinDelayRetries() == 6);
			 assertTrue(sRetryPolicy.getBackOffFunction() == CnsBackoffFunction.exponential);
			 
			 //Test default constructor
			 CNSSubscriptionDeliveryPolicy subpolicy2 = new CNSSubscriptionDeliveryPolicy();
			 hRetryPolicy = subpolicy2.getHealthyRetryPolicy();
			 assertTrue(hRetryPolicy.getMinDelayTarget() == 20);
			 assertTrue(hRetryPolicy.getMaxDelayTarget() == 20);
			 assertTrue(hRetryPolicy.getNumRetries() == 3);
			 assertTrue(hRetryPolicy.getNumMaxDelayRetries() == 0);
			 assertTrue(hRetryPolicy.getNumMinDelayRetries() == 0);
			 assertTrue(hRetryPolicy.getBackOffFunction() == CnsBackoffFunction.linear);
			 tPolicy = subpolicy2.getThrottlePolicy();
			 logger.debug("tPolicy: " + tPolicy.toString());
			 assertTrue(tPolicy.getMaxReceivesPerSecond() == null);
			 assertTrue(subpolicy.getSicklyRetryPolicy() == null);
			 
			 
			 JSONObject json3 = new JSONObject(jsonStr3);
			 CNSSubscriptionDeliveryPolicy subpolicy4 = new CNSSubscriptionDeliveryPolicy(json3);
			 hRetryPolicy = subpolicy4.getHealthyRetryPolicy();
			 assertTrue(hRetryPolicy.getMinDelayTarget() == 20);
			 assertTrue(hRetryPolicy.getMaxDelayTarget() == 20);
			 assertTrue(hRetryPolicy.getNumRetries() == 3);
			 assertTrue(hRetryPolicy.getNumMaxDelayRetries() == 0);
			 assertTrue(hRetryPolicy.getNumMinDelayRetries() == 0);
			 assertTrue(hRetryPolicy.getBackOffFunction() == CnsBackoffFunction.linear);
			 tPolicy = subpolicy2.getThrottlePolicy();
			 logger.debug("tPolicy: " + tPolicy.toString());
			 assertTrue(tPolicy.getMaxReceivesPerSecond() == null);
			 assertTrue(subpolicy.getSicklyRetryPolicy() == null);
			 
			 boolean exceptionOccured = false;
			 json = new JSONObject(jsonStr4);
			
			 try {
				new CNSSubscriptionDeliveryPolicy(json);
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
				
			 json = new JSONObject(jsonStr5);
				
			 try {
				new CNSSubscriptionDeliveryPolicy(json);
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
			 
			 json = new JSONObject(jsonStr6);
				
			 try {
				new CNSSubscriptionDeliveryPolicy(json);
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
			 
			 json = new JSONObject(jsonStr7);
				
			 try {
				new CNSSubscriptionDeliveryPolicy(json);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 4:");
					logger.debug(e.getMessage());
				} else {
					assertFalse(true);
				}
			 }
			 assertTrue(exceptionOccured);
			 exceptionOccured = false;
			 
			 
			 json = new JSONObject(jsonStr8);
			 try {
				new CNSSubscriptionDeliveryPolicy(json);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 4:");
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
		 String jsonStr = "{\"healthyRetryPolicy\":" +				 
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
	 			"}"+
		        "}";
		 
		 String jsonStr2 = "{" +
				"\"healthyRetryPolicy\":null," +		
	 			"\"sicklyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":32,"+
		        "\"maxDelayTarget\":33,"+
		        "\"numRetries\":99,"+
		        "\"numMaxDelayRetries\": 32,"+
		        "\"numMinDelayRetries\": 31,"+
		        "\"numNoDelayRetries\":33," +
		        "\"backoffFunction\": \"exponential\""+
		        "}" +
		        "}";
		 
		 String jsonStr4 = "{\"healthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":2,"+
		        "\"numRetries\":1," +
		        "\"numMaxDelayRetries\": 4,"+
		        "\"numMinDelayRetries\": 6,"+
		        "\"backoffFunction\": \"linear\""+
		        "}" +
		        "}";
		 
		 String jsonStr5 = "{\"sicklyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":2,"+
		        "\"numRetries\":\"cookie\"," +
		        "\"numMaxDelayRetries\": 4,"+
		        "\"numMinDelayRetries\": 6,"+
		        "\"backoffFunction\": \"linear\""+
		        "}" +
		        "}";
		 
		 String jsonStr6 = "{" +		 
				 "\"throttlePolicy\":" +
			        "{" +
			        "\"maxReceivesPerSecond\":\"stand\"" +
		 			"}" +
		        "}";
		 
		 String jsonStr7 = "{" +		 
				 "\"throttlePolicy\":{\"cookie monster\":\"cookie monster\"}" +
		        "}";
		 
		 String jsonStr8 = "{\"healthyRetryPolicy\":" +				 
				 "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":3601,"+
		        "\"numRetries\":13," +
		        "\"numMaxDelayRetries\": 4,"+
		        "\"numMinDelayRetries\": 6,"+
		        "\"backoffFunction\": \"linear\""+
		        "}" +
		        "}";
		 
		 try {
			 CNSSubscriptionDeliveryPolicy subpolicy = new CNSSubscriptionDeliveryPolicy();
			 JSONObject json = new JSONObject(jsonStr);
			 subpolicy.update(json);
			 CNSRetryPolicy hRetryPolicy = subpolicy.getHealthyRetryPolicy();
			 logger.debug("hRetryPolicy is: " + hRetryPolicy.toString());
			 assertTrue(hRetryPolicy.getMinDelayTarget() == 12);
			 assertTrue(hRetryPolicy.getMaxDelayTarget() == 13);
			 assertTrue(hRetryPolicy.getNumRetries() == 43);
			 assertTrue(hRetryPolicy.getNumMaxDelayRetries() == 23);
			 assertTrue(hRetryPolicy.getNumMinDelayRetries() == 20);
			 assertTrue(hRetryPolicy.getBackOffFunction() == CnsBackoffFunction.arithmetic);
			 CNSThrottlePolicy tPolicy = subpolicy.getThrottlePolicy();
			 assertTrue(tPolicy.getMaxReceivesPerSecond() == 7);
			 
			 CNSRetryPolicy sRetryPolicy = subpolicy.getSicklyRetryPolicy();
			 assertTrue(sRetryPolicy == null);
			 
			 json = new JSONObject(jsonStr2);
			 subpolicy.update(json);
			 hRetryPolicy = subpolicy.getHealthyRetryPolicy();
			 assertTrue(hRetryPolicy != null);
			 logger.debug("hRetryPolicy is: " + hRetryPolicy.toString());
			 assertTrue(hRetryPolicy.getMinDelayTarget() == 20);
			 assertTrue(hRetryPolicy.getMaxDelayTarget() == 20);
			 assertTrue(hRetryPolicy.getNumRetries() == 3);
			 assertTrue(hRetryPolicy.getNumMaxDelayRetries() == 0);
			 assertTrue(hRetryPolicy.getNumMinDelayRetries() == 0);
			 assertTrue(hRetryPolicy.getBackOffFunction() == CnsBackoffFunction.linear);
			 tPolicy = subpolicy.getThrottlePolicy();
			 assertTrue(tPolicy.getMaxReceivesPerSecond() == null);
			 sRetryPolicy = subpolicy.getSicklyRetryPolicy();
			 logger.debug("sRetryPolicy is: " + sRetryPolicy.toString());
			 assertTrue(sRetryPolicy.getMinDelayTarget() == 32);
			 assertTrue(sRetryPolicy.getMaxDelayTarget() == 33);
			 assertTrue(sRetryPolicy.getNumRetries() == 99);
			 assertTrue(sRetryPolicy.getNumMaxDelayRetries() == 32);
			 assertTrue(sRetryPolicy.getNumMinDelayRetries() == 31);
			 assertTrue(sRetryPolicy.getNumNoDelayRetries() == 33);
			 assertTrue(sRetryPolicy.getBackOffFunction() == CnsBackoffFunction.exponential);
			 
			 boolean exceptionOccured = false;
			 json = new JSONObject(jsonStr4);
			
			 try {
				 subpolicy.update(json);
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
				
			 json = new JSONObject(jsonStr5);
				
			 try {
				 subpolicy.update(json);
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
			 
			 json = new JSONObject(jsonStr6);
				
			 try {
				 subpolicy.update(json);
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
			 
			 json = new JSONObject(jsonStr7);
				
			 try {
				 subpolicy.update(json);
				 logger.debug("subpolicy: " + subpolicy);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 4:");
					logger.debug(e.getMessage());
				} else {
					assertFalse(true);
				}
			 }
			 assertTrue(exceptionOccured);
			 exceptionOccured = false;
			 
			 json = new JSONObject(jsonStr8);
				
			 try {
				 subpolicy.update(json);
				 logger.debug("subpolicy: " + subpolicy);
			 } catch (Exception e) {
				if(e instanceof CMBException) {
					assertTrue(true);
					exceptionOccured = true;
					logger.debug("Exception 5:");
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
	 public void testToStringToJSON() {
		 try {
			 CNSSubscriptionDeliveryPolicy subpolicy = new CNSSubscriptionDeliveryPolicy();
		 	JSONObject subpolicyJSON = subpolicy.toJSON();
		 	logger.info(subpolicy.toString());
		 	assertTrue(subpolicyJSON.has("healthyRetryPolicy"));
		 	JSONObject respJSON = subpolicyJSON.getJSONObject("healthyRetryPolicy");
			assertTrue(respJSON.has("backoffFunction"));
			assertTrue(respJSON.getString("backoffFunction").equals("linear"));
			assertTrue(respJSON.has("numMaxDelayRetries"));
			assertTrue(respJSON.getInt("numMaxDelayRetries") == 0);
			assertTrue(respJSON.has("numMinDelayRetries"));
			assertTrue(respJSON.getInt("numMinDelayRetries") == 0);
			assertTrue(respJSON.has("numRetries"));
			assertTrue(respJSON.getInt("numRetries") == 3);
			assertTrue(respJSON.has("minDelayTarget"));
			assertTrue(respJSON.getInt("minDelayTarget") == 20);
			assertTrue(respJSON.has("maxDelayTarget"));
			assertTrue(respJSON.getInt("maxDelayTarget") == 20);
			
			assertTrue(subpolicyJSON.get("sicklyRetryPolicy") == JSONObject.NULL);
			String jsonStr = "{\"sicklyRetryPolicy\":null,\"healthyRetryPolicy\":{\"backoffFunction\":\"linear\",\"numMinDelayRetries\":0,\"numMaxDelayRetries\":0,\"numRetries\":3,\"minDelayTarget\":20,\"numNoDelayRetries\":0,\"maxDelayTarget\":20},\"throttlePolicy\":{\"maxReceivesPerSecond\":null}}";
			assertTrue(subpolicy.toString().equals(jsonStr));
			
		 } catch (Exception e) {
             logger.error("Exception occured", e);
             fail("exception: "+e);
		 }
	 }
	 
	 @Test
	 public void testGettersSetters() {
		 try {
			 CNSSubscriptionDeliveryPolicy subpolicy = new CNSSubscriptionDeliveryPolicy();
			 CNSRetryPolicy hrp = new CNSRetryPolicy();
			 hrp.setBackOffFunction(CnsBackoffFunction.geometric);
			 
			 CNSRetryPolicy srp = new CNSRetryPolicy();
			 srp.setBackOffFunction(CnsBackoffFunction.exponential);
			 
			 CNSThrottlePolicy tp = new CNSThrottlePolicy();
			 tp.setMaxReceivesPerSecond(78);
			 
			 subpolicy.setHealthyRetryPolicy(hrp);
			 subpolicy.setSicklyRetryPolicy(srp);
			 subpolicy.setThrottlePolicy(tp);
			 
			 CNSRetryPolicy hrp2 = subpolicy.getHealthyRetryPolicy();
			 CNSRetryPolicy srp2 = subpolicy.getSicklyRetryPolicy();
			 CNSThrottlePolicy tp2 = subpolicy.getThrottlePolicy();
			 
			 assertTrue(hrp2 != null);
			 assertTrue(srp2 != null);
			 assertTrue(tp2 != null);
			 
			 assertTrue(hrp2.getBackOffFunction() == CnsBackoffFunction.geometric);
			 assertTrue(srp2.getBackOffFunction() == CnsBackoffFunction.exponential);
			 assertTrue(tp2.getMaxReceivesPerSecond() == 78);
			 
		 } catch (Exception e) {
             logger.error("Exception occured", e);
             fail("exception: "+e);
		 }
	 }
}
