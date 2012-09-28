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
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.model.CNSModelConstructionException;
import com.comcast.cns.model.CNSRetryPolicy;
import com.comcast.cns.model.CNSRetryPolicy.CnsBackoffFunction;

public class CNSRetryPolicyTest {
	private static Logger logger = Logger.getLogger(CNSRetryPolicyTest.class);
	
	@Before
	public void setup() throws Exception{
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
		  try {
			  String retryPolicy = "{"+
		        "\"minDelayTarget\":1,"+
		        "\"maxDelayTarget\":2,"+
		        "\"numRetries\":4,"+
		        "\"numMaxDelayRetries\": 4,"+
		        "\"backoffFunction\": \"linear\""+
		        "}";
			  
			  String retryPolicy2 = "{"+
					  "\"backoffFunction\":\"linear\","+
					  "\"numMinDelayRetries\":0," + 
					  "\"numMaxDelayRetries\":4,"+
					  "\"numRetries\":4,"+
				      "\"minDelayTarget\":1,"+
				      "\"numNoDelayRetries\":0," +
				      "\"maxDelayTarget\":2"+				        
				        "}";
			  
			  String retryPolicy3 = "{"+
					    "\"minDelayTarget\":20,"+
				        "\"maxDelayTarget\":20,"+
				        "\"numRetries\":3" +			        
				        "}";
			  
				JSONObject json = new JSONObject(retryPolicy);
				CNSRetryPolicy rpolicy = new CNSRetryPolicy(json);
				assertTrue(rpolicy.getMinDelayTarget() == 1);
				assertTrue(rpolicy.getMaxDelayTarget() == 2);
				assertTrue(rpolicy.getNumRetries() == 4);
				assertTrue(rpolicy.getNumMaxDelayRetries() == 4);
				assertTrue(rpolicy.getBackOffFunction() == CnsBackoffFunction.linear);
				logger.debug("retryPolicy2: " + retryPolicy2);
				logger.debug("rpolicy is: " + rpolicy.toString());
				assertTrue(rpolicy.toString().equals(retryPolicy2));
				
				JSONObject respJSON = rpolicy.toJSON();
				assertTrue(respJSON.has("backoffFunction"));
				assertTrue(respJSON.getString("backoffFunction").equals("linear"));
				assertTrue(respJSON.has("numMaxDelayRetries"));
				assertTrue(respJSON.getInt("numMaxDelayRetries") == 4);
				assertTrue(respJSON.has("numMinDelayRetries"));
				assertTrue(respJSON.getInt("numMinDelayRetries") == 0);
				assertTrue(respJSON.has("numRetries"));
				assertTrue(respJSON.getInt("numRetries") == 4);
				assertTrue(respJSON.has("minDelayTarget"));
				assertTrue(respJSON.getInt("minDelayTarget") == 1);
				assertTrue(respJSON.has("maxDelayTarget"));
				assertTrue(respJSON.getInt("maxDelayTarget") == 2);
				
				String fakeRetryPolicy = "{"+
						  "\"backoffFunction\":\"superlinear\","+
						  "\"numMaxDelayRetries\":4,"+
						  "\"numRetries\":4,"+
					      "\"minDelayTarget\":1,"+
					      "\"maxDelayTarget\":2"+				        
					        "}";
				
				String fakeRetryPolicy2 = "{"+						 
						  "\"numMinDelayRetries\":0," + 
						  "\"numMaxDelayRetries\":4,"+						 
					      "\"minDelayTarget\":1,"+
					      "\"maxDelayTarget\":2,"+				
					      "\"numNoDelayRetries\":0," +
					      "\"backoffFunction\":\"linear\""+       
					        "}";
				
				String fakeRetryPolicy3 = "{"+
						  "\"numMinDelayRetries\":0," + 
						  "\"numMaxDelayRetries\":4,"+
						  "\"numRetries\":\"boo\","+
					      "\"minDelayTarget\":1,"+
					      "\"maxDelayTarget\":2,"+				
					      "\"numNoDelayRetries\":0," +
					      "\"backoffFunction\":\"linear\""+       
					        "}";
				
				String fakeRetryPolicy4 = "{"+
						  "\"numMinDelayRetries\":0," + 
						  "\"numMaxDelayRetries\":4,"+
						  "\"numRetries\":\"101\","+
					      "\"minDelayTarget\":33,"+
					      "\"maxDelayTarget\":34,"+				
					      "\"numNoDelayRetries\":33," +
					      "\"backoffFunction\":\"linear\""+       
					        "}";
				
				String fakeRetryPolicy5 = "{"+
						  "\"numMinDelayRetries\":0," + 
						  "\"numMaxDelayRetries\":4,"+
						  "\"numRetries\":\"100\","+
					      "\"minDelayTarget\":33,"+
					      "\"maxDelayTarget\":34,"+				
					      "\"numNoDelayRetries\":-1," +
					      "\"backoffFunction\":\"linear\""+       
					        "}";
				
				String fakeRetryPolicy6 = "{"+
						  "\"numMinDelayRetries\":0," + 
						  "\"numMaxDelayRetries\":4,"+
						  "\"numRetries\":\"100\","+
					      "\"minDelayTarget\":33,"+
					      "\"maxDelayTarget\":3601,"+				
					      "\"numNoDelayRetries\":1," +
					      "\"backoffFunction\":\"linear\""+       
					        "}";
				
				json = new JSONObject(fakeRetryPolicy);
				boolean exceptionOccured = false;
				try {
					new CNSRetryPolicy(json);
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug("Exception 1:");
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
					} else {
						assertFalse(true);
					}
				}
				assertTrue(exceptionOccured);
				exceptionOccured = false;
				
				json = new JSONObject(fakeRetryPolicy2);
				try {
					new CNSRetryPolicy(json);
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug("Exception 2:");
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
					} else {
						assertFalse(true);
					}
				}
				assertTrue(exceptionOccured);
				exceptionOccured = false;
				
				json = new JSONObject(fakeRetryPolicy3);
				try {
					CNSRetryPolicy rpolicy2 = new CNSRetryPolicy(json);
					logger.debug("rpolicy2 :" + rpolicy2.toString());
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug("Exception 3:");
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
					} else {
						assertFalse(true);
					}
				}
				assertTrue(exceptionOccured);
				exceptionOccured = false;
				
				json = new JSONObject(fakeRetryPolicy4);
				try {
					CNSRetryPolicy rpolicy2 = new CNSRetryPolicy(json);
					logger.debug("rpolicy2 :" + rpolicy2.toString());
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug("Exception 4:");
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
					} else {
						assertFalse(true);
					}
				}
				assertTrue(exceptionOccured);
				exceptionOccured = false;
				
				
				json = new JSONObject(fakeRetryPolicy5);
				try {
					CNSRetryPolicy rpolicy2 = new CNSRetryPolicy(json);
					logger.debug("rpolicy2 :" + rpolicy2.toString());
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug("Exception 5:");
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
					} else {
						assertFalse(true);
					}
				}
				assertTrue(exceptionOccured);
				exceptionOccured = false;
				
				
				json = new JSONObject(fakeRetryPolicy6);
				try {
					CNSRetryPolicy rpolicy2 = new CNSRetryPolicy(json);
					logger.debug("rpolicy2 :" + rpolicy2.toString());
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						assertTrue(true);
						exceptionOccured = true;
						logger.debug("Exception 6:");
						logger.debug(((CNSModelConstructionException) e).getErrormessage());
					} else {
						assertFalse(true);
					}
				}
				assertTrue(exceptionOccured);
				exceptionOccured = false;
				
				//Test default policy on empty json
				JSONObject json2 = new JSONObject(retryPolicy3);
				CNSRetryPolicy rpolicy2 = new CNSRetryPolicy(json2);
				assertTrue(rpolicy2.getMinDelayTarget() == 20);
				assertTrue(rpolicy2.getMaxDelayTarget() == 20);
				assertTrue(rpolicy2.getNumRetries() == 3);
				assertTrue(rpolicy2.getNumMaxDelayRetries() == 0);
				assertTrue(rpolicy2.getNumMinDelayRetries() == 0);
				assertTrue(rpolicy2.getBackOffFunction() == CnsBackoffFunction.linear);
				
				
				//Test default constructor
				CNSRetryPolicy rpolicy3 = new CNSRetryPolicy();
				assertTrue(rpolicy3.getMinDelayTarget() == 20);
				assertTrue(rpolicy3.getMaxDelayTarget() == 20);
				assertTrue(rpolicy3.getNumRetries() == 3);
				assertTrue(rpolicy3.getNumMaxDelayRetries() == 0);
				assertTrue(rpolicy3.getNumMinDelayRetries() == 0);
				assertTrue(rpolicy3.getBackOffFunction() == CnsBackoffFunction.linear);
				
				//Test multi variable constructor
				CNSRetryPolicy rpolicy4 = new CNSRetryPolicy(2, 4, 6, 8, 10, 12, CnsBackoffFunction.exponential);
				assertTrue(rpolicy4.getMinDelayTarget() == 2);
				assertTrue(rpolicy4.getMaxDelayTarget() == 4);
				assertTrue(rpolicy4.getNumRetries() == 6);
				assertTrue(rpolicy4.getNumMaxDelayRetries() == 8);
				assertTrue(rpolicy4.getNumMinDelayRetries() == 10);
				assertTrue(rpolicy4.getNumNoDelayRetries() == 12);
				assertTrue(rpolicy4.getBackOffFunction() == CnsBackoffFunction.exponential);
		  } catch (Exception e) {
              logger.error("Exception occured", e);
              fail("exception: "+e);
		  }
	  }
	  
	  @Test
	public void testSetGetUpdate() {
		  try {
				//Test setters
			    String retryPolicy = "{"+
				        "\"minDelayTarget\":1,"+
				        "\"maxDelayTarget\":2,"+
				        "\"numRetries\":4,"+
				        "\"numMaxDelayRetries\": 4,"+
				        "\"backoffFunction\": \"linear\""+
				        "}";
			    
			    String retryPolicy3 = "{"+
					    "\"minDelayTarget\":20,"+
				        "\"maxDelayTarget\":20,"+
				        "\"numRetries\":3" +			        
				        "}";
			  
			    JSONObject json = new JSONObject(retryPolicy);
				CNSRetryPolicy rpolicy = new CNSRetryPolicy(json);
				rpolicy.setBackOffFunction(CnsBackoffFunction.arithmetic);
				rpolicy.setMaxDelayTarget(6);
				rpolicy.setMinDelayTarget(7);
				rpolicy.setNumMaxDelayRetries(8);
				rpolicy.setNumMinDelayRetries(12);
				rpolicy.setNumNoDelayRetries(14);
				rpolicy.setNumRetries(9);
				assertTrue(rpolicy.getMinDelayTarget() == 7);
				assertTrue(rpolicy.getMaxDelayTarget() == 6);
				assertTrue(rpolicy.getNumRetries() == 9);
				assertTrue(rpolicy.getNumMaxDelayRetries() == 8);
				assertTrue(rpolicy.getNumMinDelayRetries() == 12);
				assertTrue(rpolicy.getNumNoDelayRetries() == 14);
				assertTrue(rpolicy.getBackOffFunction() == CnsBackoffFunction.arithmetic);
				
				
				
				//Test multi variable constructor
				CNSRetryPolicy rpolicy4 = new CNSRetryPolicy(2, 4, 6, 8, 10, 12, CnsBackoffFunction.exponential);
				
				
				//Test update
				String newRetryPolicy = "{"+
						   "\"minDelayTarget\":\"9\","+
						  "\"maxDelayTarget\":9," + 
						  "\"backoffFunction\":\"linear\","+
						  "\"numMinDelayRetries\":12," + 
						  "\"numMaxDelayRetries\":12,"+
						  "\"numNoDelayRetries\":14," +
						  "\"numRetries\":38"+  		        
					        "}";
				//Test update2
				String newRetryPolicy2 = "{"+
						  "\"minDelayTarget\":10,"+
						  "\"maxDelayTarget\":10," + 
						  "\"numRetries\":5"+  	    
					        "}";
				
				//Test update3
				String newRetryPolicy3 = "{"+
						  "\"minDelayTarget\":\"10\","+
						  "\"maxDelayTarget\":10," + 
						  "\"numRetries\":5,"+  	    
						  "\"backoffFunction\":\"crazygrowth\""+
					        "}";
				
				//Test update4
				String newRetryPolicy4 = "{"+
						  "\"minDelayTarget\":\"10\","+
						  "\"maxDelayTarget\":10," + 	    
						  "\"backoffFunction\":\"linear\""+
					        "}";
				
				//Test update5
				String newRetryPolicy5 = "{"+
						  "\"maxDelayTarget\":10," + 	
						  "\"numRetries\":5,"+  	
						  "\"backoffFunction\":\"linear\""+
					        "}";
				
				//Test update6
				String newRetryPolicy6 = "{"+
						  "\"minDelayTarget\":\"10\","+
						  "\"numRetries\":5,"+  	
						  "\"backoffFunction\":\"linear\""+
					        "}";
				
				//Test update7
				String newRetryPolicy7 = "{"+
						  "\"minDelayTarget\":10,"+
						  "\"maxDelayTarget\":9," + 	    
						  "\"numRetries\":5,"+  	
						  "\"backoffFunction\":\"linear\""+
					        "}";
				
				//Test update8
				String newRetryPolicy8 = "{"+
						  "\"minDelayTarget\":10,"+
						  "\"maxDelayTarget\":10," + 	    
						  "\"numRetries\":20,"+  
						  "\"numMinDelayRetries\":12," + 
						  "\"numMaxDelayRetries\":12,"+
						  "\"numNoDelayRetries\":14," +
						  "\"backoffFunction\":\"linear\""+
					        "}";
				
				//Test update9
				String newRetryPolicy9 = "{"+
						  "\"minDelayTarget\":10,"+
						  "\"maxDelayTarget\":\"cookie\"," + 	    
						  "\"numRetries\":20,"+  
						  "\"numMinDelayRetries\":12," + 
						  "\"numMaxDelayRetries\":12,"+
						  "\"numNoDelayRetries\":14," +
						  "\"backoffFunction\":\"linear\""+
					        "}";
				

				//Test update10
				String newRetryPolicy10 = "{"+
						  "\"minDelayTarget\":10,"+
						  "\"maxDelayTarget\":\"11\"," + 	    
						  "\"numRetries\":101,"+  
						  "\"numMinDelayRetries\":33," + 
						  "\"numMaxDelayRetries\":34,"+
						  "\"numNoDelayRetries\":34," +
						  "\"backoffFunction\":\"linear\""+
					        "}";
				
				//Test update11
				String newRetryPolicy11 = "{"+
						  "\"minDelayTarget\":10,"+
						  "\"maxDelayTarget\":\"11\"," + 	    
						  "\"numRetries\":100,"+  
						  "\"numMinDelayRetries\":10," + 
						  "\"numMaxDelayRetries\":11,"+
						  "\"numNoDelayRetries\":-1," +
						  "\"backoffFunction\":\"linear\""+
					        "}";
				
				//Test update12
				String newRetryPolicy12 = "{"+
						  "\"minDelayTarget\":10,"+
						  "\"maxDelayTarget\":\"3601\"," + 	    
						  "\"numRetries\":100,"+  
						  "\"numMinDelayRetries\":10," + 
						  "\"numMaxDelayRetries\":11,"+
						  "\"numNoDelayRetries\":0," +
						  "\"backoffFunction\":\"linear\""+
					        "}";
				
				json = new JSONObject(newRetryPolicy);
				rpolicy4.update(json);
				assertTrue(rpolicy4.getMinDelayTarget() == 9);
				assertTrue(rpolicy4.getMaxDelayTarget() == 9);
				assertTrue(rpolicy4.getNumRetries() == 38);
				assertTrue(rpolicy4.getNumMinDelayRetries() == 12);
				assertTrue(rpolicy4.getNumMaxDelayRetries() == 12);
				assertTrue(rpolicy4.getNumNoDelayRetries() == 14);
				assertTrue(rpolicy4.getBackOffFunction() == CnsBackoffFunction.linear);
				
				json = new JSONObject(retryPolicy3);
				rpolicy4.update(json);
				assertTrue(rpolicy4.getMinDelayTarget() == 20);
				assertTrue(rpolicy4.getMaxDelayTarget() == 20);
				assertTrue(rpolicy4.getNumRetries() == 3);
				assertTrue(rpolicy4.getNumMaxDelayRetries() == 0);
				assertTrue(rpolicy4.getNumMinDelayRetries() == 0);
				
				json = new JSONObject(newRetryPolicy2);
				rpolicy4.update(json);
				assertTrue(rpolicy4.getMinDelayTarget() == 10);
				assertTrue(rpolicy4.getMaxDelayTarget() == 10);
				assertTrue(rpolicy4.getNumRetries() == 5);
				assertTrue(rpolicy4.getNumMaxDelayRetries() == 0);
				assertTrue(rpolicy4.getNumMinDelayRetries() == 0);
				assertTrue(rpolicy4.getNumNoDelayRetries() == 0);
				assertTrue(rpolicy4.getBackOffFunction() == CnsBackoffFunction.linear);
				
				json = new JSONObject(newRetryPolicy3);
				boolean exceptionOccured = false;
				try {
					rpolicy4.update(json);
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
				
				json = new JSONObject(newRetryPolicy4);
				try {
					rpolicy4.update(json);
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
				
				json = new JSONObject(newRetryPolicy5);
				
				try {
					rpolicy4.update(json);
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
				
				json = new JSONObject(newRetryPolicy6);	
				try {
					rpolicy4.update(json);
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
				
				json = new JSONObject(newRetryPolicy7);	
				try {
					rpolicy4.update(json);
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
				
				json = new JSONObject(newRetryPolicy8);	
				try {
					rpolicy4.update(json);
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
				
				json = new JSONObject(newRetryPolicy9);	
				try {
					rpolicy4.update(json);
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
							
				json = new JSONObject(newRetryPolicy10);	
				try {
					rpolicy4.update(json);
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
				
				json = new JSONObject(newRetryPolicy11);	
				try {
					rpolicy4.update(json);
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
				
				json = new JSONObject(newRetryPolicy12);	
				try {
					rpolicy4.update(json);
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
