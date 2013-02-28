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
package com.comcast.cns.model;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.util.CNSErrorCodes;

/**
 * represents a delivery policy
 * @author bwolf, jorge
 *
 * Class is not thread-safe. Caller must ensure thread-safety
 */
public class CNSTopicDeliveryPolicy {

	private static Logger logger = Logger.getLogger(CNSTopicDeliveryPolicy.class); 
	private CNSRetryPolicy defaultHealthyRetryPolicy;
	private CNSRetryPolicy defaultSicklyRetryPolicy;
	private boolean disableSubscriptionOverrides;
	private CNSThrottlePolicy defaultThrottlePolicy;
	
	public CNSRetryPolicy getDefaultHealthyRetryPolicy() {
		return defaultHealthyRetryPolicy;
	}

	public void setDefaultHealthyRetryPolicy(CNSRetryPolicy defaultHealthyRetryPolicy) {
		this.defaultHealthyRetryPolicy = defaultHealthyRetryPolicy;
	}
	
	public CNSRetryPolicy getDefaultSicklyRetryPolicy() {
		return defaultSicklyRetryPolicy;
	}
	
	public void setDefaultSicklyRetryPolicy(CNSRetryPolicy defaultSicklyRetryPolicy) {
		this.defaultSicklyRetryPolicy = defaultSicklyRetryPolicy;
	}
	
	public CNSThrottlePolicy getDefaultThrottlePolicy() {
		return defaultThrottlePolicy;
	}
	
	public void setDefaultThrottlePolicy(CNSThrottlePolicy defaultThrottlePolicy) {
		this.defaultThrottlePolicy = defaultThrottlePolicy;
	}
	
	public boolean isDisableSubscriptionOverrides() {
		return disableSubscriptionOverrides;
	}
	
	public void setDisableSubscriptionOverrides(boolean disableSubscriptionOverrides) {
		this.disableSubscriptionOverrides = disableSubscriptionOverrides;
	}
		
	public CNSTopicDeliveryPolicy() {
		defaultHealthyRetryPolicy = new CNSRetryPolicy();
		defaultSicklyRetryPolicy = null;
		disableSubscriptionOverrides = false;
		defaultThrottlePolicy = new CNSThrottlePolicy();
	}

	private JSONObject httpJSONObject() {
		try {
	    	JSONObject json = new JSONObject();
	    	if (defaultHealthyRetryPolicy != null) {
	    		json.put("defaultHealthyRetryPolicy", defaultHealthyRetryPolicy.toJSON());
	    	} else {
	    		json.put("defaultHealthyRetryPolicy", JSONObject.NULL);
	    	}
	    	
	    	if (defaultSicklyRetryPolicy != null) {
	    		json.put("defaultSicklyRetryPolicy", defaultSicklyRetryPolicy.toJSON());
	    	} else {
	    		json.put("defaultSicklyRetryPolicy", JSONObject.NULL);
	    	}
	    	
	    	if (disableSubscriptionOverrides) {
	    		json.put("disableSubscriptionOverrides", true);
	    	} else {
	    		json.put("disableSubscriptionOverrides", false);
	    	}

	    	if (defaultThrottlePolicy != null)  {
	    		json.put("defaultThrottlePolicy", defaultThrottlePolicy.toJSON());
	    	} else {
	    		json.put("defaultThrottlePolicy", JSONObject.NULL);
	    	}
	    	
	    	return json;
		
		} catch (Exception e) {
			logger.error("event=cns_topic_delivery_policy_to_json", e);
		}

		return null;
	}
	
	public CNSTopicDeliveryPolicy(JSONObject json) throws CMBException {
		
		String errorMessage = "";
		boolean error = false;
		if (!json.has("http")) {
			errorMessage = "Topic Delivery Policy missing policy for http";
			logger.error("event=construct_cns_topic_delivery_policy message=" + errorMessage);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, errorMessage);
		}
		try{	
			JSONObject json2 = json.getJSONObject("http");
			if (json2.length() == 0) {
			    throw new Exception("Empty http protocol");
			}
			
			for (Iterator<String> keys = json2.keys(); keys.hasNext();) {
			    String key = keys.next();
				if (!(key.equals("defaultHealthyRetryPolicy") || key.equals("defaultSicklyRetryPolicy")  || key.equals("disableSubscriptionOverrides") ||
						key.equals("defaultThrottlePolicy"))) {
					throw new Exception("Empty http delivery policy does not have key:" + key);
				}
			}
			if (json2.has("defaultHealthyRetryPolicy") && (json2.get("defaultHealthyRetryPolicy") != JSONObject.NULL)) {
				try {
					defaultHealthyRetryPolicy = new CNSRetryPolicy(json2.getJSONObject("defaultHealthyRetryPolicy"));
				} catch (Exception e) {
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						errorMessage = "DeliveryPolicy: defaultHealthyRetryPolicy." + message;
						error = true;
					}
				}					
			} else {
				defaultHealthyRetryPolicy = new CNSRetryPolicy();
			}
			if ((json2.has("defaultSicklyRetryPolicy"))) {
				if (json2.get("defaultSicklyRetryPolicy") == JSONObject.NULL) defaultSicklyRetryPolicy = null;
				else {
					try {
						defaultSicklyRetryPolicy = new CNSRetryPolicy(json2.getJSONObject("defaultSicklyRetryPolicy"));
					} catch (Exception e) {
						if (e instanceof CNSModelConstructionException) {
							String message = ((CNSModelConstructionException) e).getErrormessage();
							errorMessage = "DeliveryPolicy: defaultSicklyRetryPolicy." + message;
							error = true;
						}
					}	
				}
			}
			
			if ((json2.has("disableSubscriptionOverrides"))) {
				disableSubscriptionOverrides = json2.getBoolean("disableSubscriptionOverrides");
			} else {
				disableSubscriptionOverrides = false;
			}
			
			if ((json2.has("defaultThrottlePolicy")) && (json2.get("defaultThrottlePolicy") != JSONObject.NULL)) {
				try {
					defaultThrottlePolicy = new CNSThrottlePolicy(json2.getJSONObject("defaultThrottlePolicy"));
				} catch (Exception e) {
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						errorMessage = "DeliveryPolicy: defaultThrottlePolicy." + message;
						error = true;
					}
				}	
			} else {
				defaultThrottlePolicy = new CNSThrottlePolicy();
			}
				
			
		} catch (Exception e) {
			logger.error("event=construct_cns_topic_delivery_policy", e);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,e.getMessage());
		}
		if (error) {
			logger.error("event=update_cns_topic_delivery_policy message=" + errorMessage);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, errorMessage);
		}
	}
		
	/**
     * Update this object by taking values from parameter (if present) or the default values
     *  Also do validation checks
     * @param json
     * @throws Exception
     */
	public void update(JSONObject json) throws CNSModelConstructionException, CMBException {
		if (!json.has("http")) {
			String errorMessage = "Topic Delivery Policy missing policy for http";
			logger.error("event=update_cns_topic_delivery_policy message=" + errorMessage);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, errorMessage);
		}
		String errorMessage = "";
		boolean error = false;
		try {	
			CNSRetryPolicy ldefaultHealthyRetryPolicy = null;
			CNSRetryPolicy ldefaultSicklyRetryPolicy = null;
			CNSThrottlePolicy ldefaultThrottlePolicy = null;
			boolean ldisableSubscriptionOverrides = false;
			
			
			JSONObject json2 = json.getJSONObject("http");
		
			if (json2.has("defaultHealthyRetryPolicy") && (json2.get("defaultHealthyRetryPolicy") != JSONObject.NULL)) {
				try {
					ldefaultHealthyRetryPolicy = new CNSRetryPolicy(json2.getJSONObject("defaultHealthyRetryPolicy"));
				} catch (Exception e) {
					if(e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						errorMessage = "DeliveryPolicy: defaultHealthyRetryPolicy." + message;
						error = true;
					}
				}	
			} else {
				ldefaultHealthyRetryPolicy = new CNSRetryPolicy();
			}
			if ((json2.has("defaultSicklyRetryPolicy")) &&(json2.get("defaultSicklyRetryPolicy") != JSONObject.NULL)) {
				try {
					ldefaultSicklyRetryPolicy = new CNSRetryPolicy(json2.getJSONObject("defaultSicklyRetryPolicy"));
				} catch (Exception e) {
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						errorMessage = "DeliveryPolicy: defaultSicklyRetryPolicy." + message;
						error = true;
					}
				}	
			} else {
				ldefaultSicklyRetryPolicy = null;
			}
			
			if ((json2.has("disableSubscriptionOverrides"))) {
				try {
					ldisableSubscriptionOverrides = json2.getBoolean("disableSubscriptionOverrides");
				} catch (Exception e) {
					errorMessage = "DeliveryPolicy: disableSubscriptionOverrides must be a boolean";
					error = true;
				}	
			} else {
				ldisableSubscriptionOverrides = false;
			}
			
			if ((json2.has("defaultThrottlePolicy")) && (json2.get("defaultThrottlePolicy") != JSONObject.NULL)) {
				try {
					ldefaultThrottlePolicy = new CNSThrottlePolicy(json2.getJSONObject("defaultThrottlePolicy"));
				} catch (Exception e) {
					if (e instanceof CNSModelConstructionException) {
						String message = ((CNSModelConstructionException) e).getErrormessage();
						errorMessage = "DeliveryPolicy: defaultThrottlePolicy." + message;
						error = true;
					}
				}	
			} else {
				ldefaultThrottlePolicy = new CNSThrottlePolicy();
			}
			if (!error) {
				defaultHealthyRetryPolicy = ldefaultHealthyRetryPolicy;
				defaultSicklyRetryPolicy = ldefaultSicklyRetryPolicy;
				disableSubscriptionOverrides = ldisableSubscriptionOverrides;
				defaultThrottlePolicy = ldefaultThrottlePolicy;
			}
			
		} catch (Exception e) {
			logger.error("event=update_cns_topic_delivery_policy", e);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"DeliveryPolicy: JSON exception");
		}
		if (error) {
			logger.error("event=update_cns_topic_delivery_policy message=" + errorMessage);
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, errorMessage);
		}
	}
	
	public JSONObject toJSON() {
		try {
	    	JSONObject json = new JSONObject();
	    	JSONObject httpJson = httpJSONObject();
	    	json.put("http", httpJson);
	    	return json;
		} catch (Exception e) {
			logger.error("event=cns_topic_delivery_policy_to_json", e);
		}
		return null;
	}
	
	public String toString() {
		try {
			JSONObject json = this.toJSON();
	    	if (json != null) {
	    		return json.toString();
	    	}
	    	return null;
		} catch (Exception e) {
			logger.error("event=cns_topic_delivery_policy_to_string", e);
			return null;
		}
	}
}
