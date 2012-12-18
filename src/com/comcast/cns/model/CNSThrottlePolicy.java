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

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents a throttling policy
 * @author bwolf, jorge
 * Class is not thread-safe. Caller must ensure thread-safety
 */
public class CNSThrottlePolicy {

	private Integer maxReceivesPerSecond;
	private static Logger logger = Logger.getLogger(CNSThrottlePolicy.class);

	public Integer getMaxReceivesPerSecond() {
		return maxReceivesPerSecond;
	}

	public void setMaxReceivesPerSecond(int maxReceivesPerSecond) {
		this.maxReceivesPerSecond = maxReceivesPerSecond;
	}
	
	public CNSThrottlePolicy() {
		maxReceivesPerSecond = null;
	}
	
	public CNSThrottlePolicy(JSONObject json) throws CNSModelConstructionException {
	
		boolean error = false;
		String errorMessage = "";
		
		try {
		
			if (json.has("maxReceivesPerSecond") && (json.get("maxReceivesPerSecond") != JSONObject.NULL)) {
				maxReceivesPerSecond = json.getInt("maxReceivesPerSecond");			
			} else if (json.keys().hasNext() && !json.has("maxReceivesPerSecond")) {
				error = true;
				errorMessage = "Unexpected JSON member";
				logger.error("event=construct_cns_throttle_policy status=failed message=" + errorMessage);
			}
			
		} catch (Exception e) {
			logger.error("event=construct_cns_throttle_policy status=failed", e);
			throw new CNSModelConstructionException("unable to create Throttle Policy");
		}

		if (error) {
			logger.error("event=construct_cns_throttle_policy status=failed message=" + errorMessage);
			throw new CNSModelConstructionException("throttlePolicy." + errorMessage);
		}
		
		logger.debug("event=construct_cns_throttle_policy status=success");
	}
	
	/**
     * Update this object by taking values from parameter (if present) or the default values
     *  Also do validation checks
     * @param json
     * @throws Exception
     */
	public void update(JSONObject json) throws CNSModelConstructionException {
		
		boolean error = false;
		String errorMessage = "";
		
		try {
			
			if (json.has("maxReceivesPerSecond")  && (json.get("maxReceivesPerSecond") != JSONObject.NULL)) {
				maxReceivesPerSecond = json.getInt("maxReceivesPerSecond");
			} else if (json.keys().hasNext() && !json.has("maxReceivesPerSecond")) {
				error = true;
				errorMessage = "Unexpected JSON member";
				logger.error("event=update_cns_throttle_policy status=failed reason=error: \"" + errorMessage + "\"");
			} else {
				maxReceivesPerSecond = null;
			}
			
		} catch (JSONException e) {
			logger.error("event=update_cns_throttle_policy status=failed reason=exception: \"" + e + "\"");
			throw new CNSModelConstructionException("unable to create Throttle Policy");
		}		

		if (error) {
			logger.error("event=update_cns_throttle_policy status=failed reason=error: \"" + errorMessage + "\"");
			throw new CNSModelConstructionException("throttlePolicy." + errorMessage);
		}
		
		logger.info("event=update_cns_throttle_policy status=success");
	} 
	
	
	public JSONObject toJSON() {

		try {
	    
			JSONObject json = new JSONObject();
	    	
			if (maxReceivesPerSecond != null) {
	    		json.put("maxReceivesPerSecond", maxReceivesPerSecond);	    	
	    	} else {
	    		json.put("maxReceivesPerSecond", JSONObject.NULL);	    	
	    	}
	    		
	    	return json;
		
		} catch (Exception e) {
			logger.error("event=cns_throttle_policy_to_json status=failed reason=exception: \"" + e + "\"");
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
			logger.error("event=cns_throttle_policy_to_string status=failed reason=exception: \"" + e + "\"");
			return null;
		}
	}
}
