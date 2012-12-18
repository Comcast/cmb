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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Class represents the retry policy.
 * @author jorge, bwolf
 *
 * Class is not thread-safe. Caller must ensure thread safety
 */
public class CNSRetryPolicy {
	
	private static Logger logger = Logger.getLogger(CNSRetryPolicy.class);
	
	public enum CnsBackoffFunction { linear, arithmetic, geometric, exponential;}

	private int minDelayTarget;
	private int maxDelayTarget;
	private int numRetries;
	private int numMaxDelayRetries;
	private int numMinDelayRetries;
	private int numNoDelayRetries;
	private CnsBackoffFunction backOffFunction;
	
	/*
	 * Create an retryPolicy object by getting the variables directly
	 */
	public CNSRetryPolicy(int minDelayTarget, int maxDelayTarget, int numRetries, int numMaxDelayRetries, int numMinDelayRetries, int numNoDelayRetries, CnsBackoffFunction backoffFunction) {

		this.minDelayTarget = minDelayTarget;
		this.maxDelayTarget = maxDelayTarget;
		this.numRetries = numRetries;
		this.numMaxDelayRetries = numMaxDelayRetries;
		this.numMinDelayRetries = numMinDelayRetries;
		this.numNoDelayRetries = numNoDelayRetries;
		this.backOffFunction = backoffFunction;
	}
	
	/*
	 * Create a default policy
	 */
	public CNSRetryPolicy() {

		this.minDelayTarget = 20;
		this.maxDelayTarget = 20;
		this.numRetries = 3;
		this.numMaxDelayRetries = 0;
		this.numMinDelayRetries = 0;
		this.numNoDelayRetries = 0;
		this.backOffFunction = CnsBackoffFunction.linear;
	}
	
	/**
	 * Create a policy from a JSONObject
	 * @param json The JSONObject that contains all the info for this Retry policy
	 * @return the RetryPolicy formatted to default, or the parameter passed.
	 */
	public CNSRetryPolicy(JSONObject json) throws CNSModelConstructionException  {

		boolean error = false;
		String message = "";
		
		try {
		
			this.minDelayTarget = 20;
			this.maxDelayTarget = 20;
			this.numRetries = 3;
			this.numMaxDelayRetries = 0;
			this.numMinDelayRetries = 0;
			this.numNoDelayRetries = 0;
			this.backOffFunction = CnsBackoffFunction.linear;
			
			if (json.has("minDelayTarget")) {
				minDelayTarget = json.getInt("minDelayTarget");
			} else {
				message = "minDelayTarget must be specified";
				error = true;
			}
			
		    if (json.has("maxDelayTarget")) {
		    	maxDelayTarget = json.getInt("maxDelayTarget");
		    } else {
		    	message = "maxDelayTarget must be specified";
		    	error = true;
		    }
		    
		    if (json.has("numRetries")) {
		    	numRetries = json.getInt("numRetries"); 
		    } else {
		    	message = "numRetries must be specified";
		    	error = true;
		    }
		    
		    if (json.has("numMaxDelayRetries")) {
		    	numMaxDelayRetries  = json.getInt("numMaxDelayRetries");
		    }
		    
		    if (json.has("numMinDelayRetries")) {
		    	numMinDelayRetries  = json.getInt("numMinDelayRetries");
		    }
		    
		    if (json.has("numNoDelayRetries")) {
		    	numNoDelayRetries  = json.getInt("numNoDelayRetries");
		    }
		    
		    if (json.has("backoffFunction")) {
		    	
		    	String backoffFunctionStr = json.getString("backoffFunction");
		    	
		    	try {
		    		backOffFunction = CnsBackoffFunction.valueOf(backoffFunctionStr);
		    	} catch (Exception e) {
		    		error = true;
		    		message = "invalid backoffFunction";
		    	}
		    }
		    
			if (minDelayTarget > maxDelayTarget) {
				message ="maxDelayTarget must be greater than or equal to minDelayTarget";
				error = true;
			}
			
			if (numRetries < numMaxDelayRetries + numMinDelayRetries + numNoDelayRetries) {
				message ="numRetries must be greater than or equal to total of numMinDelayRetries, numNoDelayRetries and numMaxDelayRetries";
				error = true;
			}
			
			if (numRetries > 100) {
				message ="numRetries must be less than or equal to 100";
				error = true;
			}
			
			if (numRetries < 0 || minDelayTarget < 0 || maxDelayTarget < 0 || numMaxDelayRetries < 0 || numMinDelayRetries < 0 || numNoDelayRetries < 0) {
				message ="all variables must be greater than or equal to 0";
				error = true;
			}
			
			if (maxDelayTarget > 3600) {
				message ="max delay target must be less than or equal to 3600";
				error = true;
			}
			
		} catch (JSONException e) {
			logger.error("event=construct_cns_retry_policy status=failed", e);
			throw new CNSModelConstructionException("JSON parameter format error");
		}
		
		if (error) {
			logger.error("event=construct_cns_retry_policy status=failed message=" + message);
			throw new CNSModelConstructionException(message);
		}
	}
	
	private void update(Integer lminDelayTarget, Integer lmaxDelayTarget, Integer lnumRetries, Integer lnumMinDelayRetries, Integer lnumMaxDelayRetries, Integer lnumNoDelayRetries, CnsBackoffFunction lbackOffFunction) {

		this.minDelayTarget = lminDelayTarget;
		this.maxDelayTarget = lmaxDelayTarget;
		this.numRetries = lnumRetries;
		this.numMinDelayRetries = lnumMinDelayRetries;
		this.numMaxDelayRetries = lnumMaxDelayRetries;
		this.numNoDelayRetries = lnumNoDelayRetries;
		this.backOffFunction = lbackOffFunction;
	}
	
	/**
	 * Update this object by taking values from parameter (if present) or the default values
	 *  Also do validation checks
	 * @param json
	 * @throws Exception
	 */
	public void update(JSONObject json) throws Exception {

		boolean error = false;
		String message = "";
		
		try {
			
			//Reset the values to default, then set it to the new values
			
			Integer lminDelayTarget = 20;
			Integer lmaxDelayTarget = 20;
			Integer lnumRetries = 3;
			Integer lnumMinDelayRetries = 0;
			Integer	lnumMaxDelayRetries = 0;
			Integer lnumNoDelayRetries = 0;
			CnsBackoffFunction lbackOffFunction = CnsBackoffFunction.linear;
			
			if (json.has("minDelayTarget")) {
				lminDelayTarget = json.getInt("minDelayTarget");				
			} else {
				message = "minDelayTarget must be specified";
				error = true;
			}
			
		    if (json.has("maxDelayTarget")) {
		    	lmaxDelayTarget = json.getInt("maxDelayTarget");
		    } else {
		    	message = "maxDelayTarget must be specified";
				error = true;
			}
		    
		    if (json.has("numRetries")) {
		    	lnumRetries = json.getInt("numRetries"); 
		    } else {
		    	message = "numRetries must be specified";
				error = true;
			}
		    
		    if (json.has("numMinDelayRetries")) {
		    	lnumMinDelayRetries  = json.getInt("numMinDelayRetries");
		    }
		    
		    if (json.has("numMaxDelayRetries")) {
		    	lnumMaxDelayRetries  = json.getInt("numMaxDelayRetries");	   
		    }
		    
		    if (json.has("numNoDelayRetries")) {
		    	lnumNoDelayRetries  = json.getInt("numNoDelayRetries");
		    }
		    
		    if (json.has("backoffFunction")) {
		    	
		    	String backoffFunctionStr = json.getString("backoffFunction");
		    	
		    	try {
		    		lbackOffFunction = CnsBackoffFunction.valueOf(backoffFunctionStr);
		    	} catch (Exception e) {
		    		error = true;
		    		message = "invalid backoffFunction";
		    	}
		    }
		    
		    if (!error) {
			    
		    	if (lminDelayTarget > lmaxDelayTarget) {
					message ="maxDelayTarget must be greater than or equal to minDelayTarget";
					error = true;
				}
		    	
				if (lnumRetries < lnumMaxDelayRetries + lnumMinDelayRetries + lnumNoDelayRetries) {
					message ="numRetries must be greater than or equal to total of numMinDelayRetries, numNoDelayRetries and numMaxDelayRetries";
					error = true;
				}
				
				if (lnumRetries > 100) {
					message ="numRetries must be less than or equal to 100";
					error = true;
				}
				
				if (lnumRetries < 0 || lminDelayTarget < 0 || lmaxDelayTarget < 0 || lnumMaxDelayRetries < 0 || lnumMinDelayRetries < 0 || lnumNoDelayRetries < 0) {
					message ="all variables must be greater than or equal to 0";
					error = true;
				}
				
				if (lmaxDelayTarget > 3600) {
					message ="maxDelayTarget must be less than or equal to 3600";
					error = true;
				}
				
				if (!error) {
					this.update(lminDelayTarget, lmaxDelayTarget, lnumRetries, lnumMinDelayRetries, lnumMaxDelayRetries, lnumNoDelayRetries, lbackOffFunction);
				}
			}
		
		} catch (Exception e) {

			logger.error("event=update_cns_retry_policy status=failed", e);
			throw new CNSModelConstructionException("JSON parameter format error");
		}
		
		if (error) {
			logger.error("event=update_cns_retry_policy status=failed message=" + message);
			throw new CNSModelConstructionException(message);
		}
	}
	
	public int getMinDelayTarget() {
		return minDelayTarget;
	}
	
	public void setMinDelayTarget(int minDelayTarget) {
		this.minDelayTarget = minDelayTarget;
	}
	
	public int getMaxDelayTarget() {
		return maxDelayTarget;
	}
	
	public void setMaxDelayTarget(int maxDelayTarget) {
		this.maxDelayTarget = maxDelayTarget;
	}
	
	public int getNumRetries() {
		return numRetries;
	}
	
	public void setNumRetries(int numRetries) {
		this.numRetries = numRetries;
	}
	
	public int getNumMaxDelayRetries() {
		return numMaxDelayRetries;
	}
	
	public void setNumMaxDelayRetries(int numMaxDelayRetries) {
		this.numMaxDelayRetries = numMaxDelayRetries;
	}
	
	public CnsBackoffFunction getBackOffFunction() {
		return backOffFunction;
	}
	
	public void setBackOffFunction(CnsBackoffFunction backOffFunction) {
		this.backOffFunction = backOffFunction;
	}
	
	public JSONObject toJSON() {
		
		try {
	    
			JSONObject json = new JSONObject();
	    	json.put("minDelayTarget", minDelayTarget);
	    	json.put("maxDelayTarget", maxDelayTarget);
	    	json.put("numRetries", numRetries);
	    	json.put("numMaxDelayRetries", numMaxDelayRetries);
	    	json.put("numMinDelayRetries", numMinDelayRetries);
	    	json.put("numNoDelayRetries", numNoDelayRetries);
	    	json.put("backoffFunction", backOffFunction.toString());
	    	
	    	return json;
		
		} catch (Exception e) {
			logger.error("event=cns_retry_policy_to_json status=failed", e);
		}
		
		return null;
	}
	@Override
	public String toString() {
		
		try {
		
			JSONObject json = this.toJSON();
	    	ByteArrayOutputStream out = new ByteArrayOutputStream();
			Writer writer = new PrintWriter(out); 
	    	json.write(writer);
	    	writer.flush();
	    
	    	return out.toString();
		
		} catch (Exception e) {
			return null;
		}
	}

	public int getNumMinDelayRetries() {
		return numMinDelayRetries;
	}

	public void setNumMinDelayRetries(int numMinDelayRetries) {
		this.numMinDelayRetries = numMinDelayRetries;
	}

	public int getNumNoDelayRetries() {
		return numNoDelayRetries;
	}

	public void setNumNoDelayRetries(int numNoDelayRetries) {
		this.numNoDelayRetries = numNoDelayRetries;
	}
}
