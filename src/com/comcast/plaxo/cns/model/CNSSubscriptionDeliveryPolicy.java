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
package com.comcast.plaxo.cns.model;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cns.util.CNSErrorCodes;

/**
 * Represents a subscription delivery policy
 * @author aseem
 *
 * Class is not thread-safe. Caller must ensure thread-safety
 */
public class CNSSubscriptionDeliveryPolicy {
	
	private CNSRetryPolicy healthyRetryPolicy;
	private CNSRetryPolicy sicklyRetryPolicy;
	private CNSThrottlePolicy throttlePolicy;
	
	private static Logger logger = Logger.getLogger(CNSSubscriptionDeliveryPolicy.class);
	
	public CNSRetryPolicy getHealthyRetryPolicy() {
		return healthyRetryPolicy;
	}
	
	public void setHealthyRetryPolicy(CNSRetryPolicy healthyRetryPolicy) {
		this.healthyRetryPolicy = healthyRetryPolicy;
	}
	
	public CNSThrottlePolicy getThrottlePolicy() {
		return throttlePolicy;
	}
	
	public void setThrottlePolicy(CNSThrottlePolicy throttlePolicy) {
		this.throttlePolicy = throttlePolicy;
	}
	
	/**
	 * Default constructor
	 */
	public CNSSubscriptionDeliveryPolicy() {
		healthyRetryPolicy = new CNSRetryPolicy();
		throttlePolicy = new CNSThrottlePolicy();
	}
	
	/**
	 * Create the object form its JSON representation
	 */
	public CNSSubscriptionDeliveryPolicy(JSONObject json) throws Exception {
		
		try {
			
			
			boolean containsHRP = false;
			boolean containsSRP = false;
			boolean containsTP = false;
			
			if (json != null && json.length() > 0) {
				for (Iterator<String> keys = json.keys(); keys.hasNext();) {
				    String key = keys.next();			    
				    if (key.equals("healthyRetryPolicy")) {
				        containsHRP = true;
				    } else if (key.equals("sicklyRetryPolicy")) {
				        containsSRP = true;
				    } else if (key.equals("throttlePolicy")) {
				        containsTP = true;
				    } else {
				        throw new Exception("Unrecognized key");
				    }
				}

				if (!containsHRP && !containsSRP && !containsTP) {
				    throw new Exception("missing variables");
				}
			}
			
			if (json.has("healthyRetryPolicy") && (json.get("healthyRetryPolicy") != JSONObject.NULL)) {
				
				try {
					
					healthyRetryPolicy = new CNSRetryPolicy(json.getJSONObject("healthyRetryPolicy"));
					
				} catch (Exception e) {
					
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						logger.error("event=construct_cns_subscription_delivery_policy status=failed", e);
						throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"DeliveryPolicy: healthyRetryPolicy." + message);
					}
				}
				
			} else {
				healthyRetryPolicy = new CNSRetryPolicy();
			}
			
			if (json.has("sicklyRetryPolicy") && (json.get("sicklyRetryPolicy") != JSONObject.NULL)) { 
				
				try {
					
					sicklyRetryPolicy = new CNSRetryPolicy(json.getJSONObject("sicklyRetryPolicy"));
					
				} catch (Exception e) {
					
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"DeliveryPolicy: sicklyRetryPolicy." + message);
					}
				}
				
			} else {
				sicklyRetryPolicy = null;
			}
			
			if (json.has("throttlePolicy") && (json.get("throttlePolicy") != JSONObject.NULL)) {
				
				try {
					
					throttlePolicy = new CNSThrottlePolicy(json.getJSONObject("throttlePolicy"));
					
				} catch (Exception e) {
					
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"DeliveryPolicy: throttlePolicy." + message);
					}
				}
				
			} else {
				throttlePolicy = new CNSThrottlePolicy();
			}
			
		} catch (Exception e) {
			
			if (e instanceof CMBException) {
				throw e;
			} else {
				throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"DeliveryPolicy: JSON exception");
			}
		}
		
	}
	
	public JSONObject toJSON() {
		
		try {
			
	    	JSONObject json = new JSONObject();
	    	
	    	if (healthyRetryPolicy != null) {
	    		json.put("healthyRetryPolicy", healthyRetryPolicy.toJSON());
	    	} else {
	    		json.put("healthyRetryPolicy", JSONObject.NULL);
	    	}
	    	
	    	if (sicklyRetryPolicy != null) {
	    		json.put("sicklyRetryPolicy", sicklyRetryPolicy.toJSON());
	    	} else {	    		
	    		json.put("sicklyRetryPolicy", JSONObject.NULL);
	    	}
	    	
	    	if (throttlePolicy != null) {
	    		json.put("throttlePolicy", throttlePolicy.toJSON());
	    	} else {
	    		json.put("throttlePolicy", JSONObject.NULL);
	    	}
	    	
	    	return json;
	    	
		} catch (Exception e) {
			logger.error("event=cns_subscription_delivery_policy_to_json status=failed", e);
		}
		
		return null;
	}
	@Override
	public String toString() {
		
		try {
			
			JSONObject json = this.toJSON();
		
			if (json != null) {
				return json.toString();
			}
			
			return null;
		
		} catch (Exception e) {
			logger.error("event=cns_subscription_delivery_policy_to_string status=failed", e);
			e.printStackTrace();
			return null;
		}
	}
	/**
     * Update this object by taking values from parameter (if present) or the default values
     *  Also do validation checks
     * @param json
     * @throws Exception
     */
	public void update(JSONObject json) throws JSONException, CMBException  {

		boolean error = false;
		String errorMessage = "";
		
		try {
		
			CNSRetryPolicy lhealthyRetryPolicy = null;
			CNSRetryPolicy lsicklyRetryPolicy = null;
			CNSThrottlePolicy lthrottlePolicy = null;
			
			if (json.has("healthyRetryPolicy") && (json.get("healthyRetryPolicy") != JSONObject.NULL)) {
				
				try {
					lhealthyRetryPolicy = new CNSRetryPolicy(json.getJSONObject("healthyRetryPolicy"));
				} catch (Exception e) {
					
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						errorMessage = "DeliveryPolicy: healthyRetryPolicy." + message;
						error = true;
					}
				}
				
			} else {
				lhealthyRetryPolicy = new CNSRetryPolicy();
			}
			
			if (json.has("sicklyRetryPolicy") && (json.get("sicklyRetryPolicy") != JSONObject.NULL)) { 
				
				try {
					lsicklyRetryPolicy = new CNSRetryPolicy(json.getJSONObject("sicklyRetryPolicy"));
				} catch (Exception e) {
					
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						errorMessage = "DeliveryPolicy: sicklyRetryPolicy." + message;
						error = true;
					}
				}
				
			} else {
				lsicklyRetryPolicy = null;
			}
			
			if (json.has("throttlePolicy") && (json.get("throttlePolicy") != JSONObject.NULL)) {
				
				try {
				
					lthrottlePolicy = new CNSThrottlePolicy(json.getJSONObject("throttlePolicy"));
					logger.debug("throttlePolicy: " + lthrottlePolicy.toString());
				
				} catch (Exception e) {
					
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						errorMessage = "DeliveryPolicy: throttlePolicy." + message;
						error = true;
					}
				}
				
			} else {
				lthrottlePolicy = new CNSThrottlePolicy();
			}
			
			if (!error) {
				healthyRetryPolicy = lhealthyRetryPolicy;
				sicklyRetryPolicy = lsicklyRetryPolicy;
				throttlePolicy = lthrottlePolicy;
			}
			
		} catch (Exception e) {
			logger.error("event=cns_update_subscription_delivery_policy status=failed", e);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"DeliveryPolicy: JSON exception");
		}
		
		if (error) {
			logger.error("event=cns_update_subscription_delivery_policy status=failed message=" + errorMessage);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,errorMessage);
		}
	}
	
	public CNSRetryPolicy getSicklyRetryPolicy() {
		return sicklyRetryPolicy;
	}
	
	public void setSicklyRetryPolicy(CNSRetryPolicy sicklyRetryPolicy) {
		this.sicklyRetryPolicy = sicklyRetryPolicy;
	}
}
