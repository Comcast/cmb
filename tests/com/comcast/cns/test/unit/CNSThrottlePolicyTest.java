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
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.model.CNSModelConstructionException;
import com.comcast.cns.model.CNSThrottlePolicy;

public class CNSThrottlePolicyTest {
	
	private static Logger logger = Logger.getLogger(CNSThrottlePolicyTest.class);
	
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();

    }
	 
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
	 
	 @Test
	 public void testConstructor() {
		 String jsonStr = "{"+		        
		        "\"maxReceivesPerSecond\":5" +
	 			"}";
		 String jsonStr2 = "{"+		        
			        "\"maxReceivesPerSecond\":null" +
		 			"}";
		 
		 String jsonStr3 = "{"+		        
		 			"}";
		 
		 String jsonStr4 = "{"+		        
			        "\"maxReceivesPerSecond\":\"cookie\"" +
		 			"}";
		 
		 String jsonStr5 = "{"+		        
			        "\"cookie\":\"cookie\"" +
		 			"}";
		 
		 try {
			 JSONObject json = new JSONObject(jsonStr);
			 CNSThrottlePolicy tp = new CNSThrottlePolicy();
			 assertTrue(tp.getMaxReceivesPerSecond() == null);
			 //TODO test;
			 tp = new CNSThrottlePolicy(json);
			 assertTrue(tp.getMaxReceivesPerSecond() == 5);
			 
			 json = new JSONObject(jsonStr2);
			 tp = new CNSThrottlePolicy(json);
			 assertTrue(tp.getMaxReceivesPerSecond() == null);
			 
			 json = new JSONObject(jsonStr3);
			 tp = new CNSThrottlePolicy(json);
			 assertTrue(tp.getMaxReceivesPerSecond() == null);
			 
			 json = new JSONObject(jsonStr4);
				boolean exceptionOccured = false;
				try {
					 tp = new CNSThrottlePolicy(json);
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
					} else {
						assertFalse(true);
					}
				}
				assertTrue(exceptionOccured);
				exceptionOccured = false;
				
				json = new JSONObject(jsonStr5);
				try {
					 tp = new CNSThrottlePolicy(json);
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
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
		 String jsonStr = "{"+		        
		        "\"maxReceivesPerSecond\":5" +
	 			"}";
		 String jsonStr2 = "{"+		        
			        "\"maxReceivesPerSecond\":null" +
		 			"}";
		 
		 String jsonStr3 = "{"+		        
		 			"}";
		 
		 String jsonStr4 = "{"+		        
			        "\"maxReceivesPerSecond\":\"cookie\"" +
		 			"}";
		 
		 String jsonStr5 = "{"+		        
			        "\"cookie\":\"cookie\"" +
		 			"}";
		 
		 try {
			 JSONObject json = new JSONObject(jsonStr);
			 CNSThrottlePolicy tp = new CNSThrottlePolicy();
			 assertTrue(tp.getMaxReceivesPerSecond() == null);
			 //TODO test;
			 tp.update(json);
			 assertTrue(tp.getMaxReceivesPerSecond() == 5);
			 
			 json = new JSONObject(jsonStr2);
			 tp.update(json);
			 assertTrue(tp.getMaxReceivesPerSecond() == null);
			 
			 json = new JSONObject(jsonStr3);
			 tp.update(json);
			 assertTrue(tp.getMaxReceivesPerSecond() == null);
			 
			 
			 json = new JSONObject(jsonStr4);
				boolean exceptionOccured = false;
				try {
					tp.update(json);
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
					} else {
						assertFalse(true);
					}
				}
				assertTrue(exceptionOccured);
				exceptionOccured = false;
				
			json = new JSONObject(jsonStr5);
				try {
					 tp.update(json);
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
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
}
