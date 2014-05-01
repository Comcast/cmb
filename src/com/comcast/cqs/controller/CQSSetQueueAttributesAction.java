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
package com.comcast.cqs.controller;

import java.util.HashMap;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.controller.CNSCreateTopicAction;
import com.comcast.cqs.io.CQSQueuePopulator;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.Util;
/**
 * Set queue attributes
 * @author bwolf, baosen
 *
 */
public class CQSSetQueueAttributesAction extends CQSAction {
	
	private static Logger logger = Logger.getLogger(CNSCreateTopicAction.class);

	public CQSSetQueueAttributesAction() {
		super("SetQueueAttributes");
	}		

	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
	    CQSQueue queue = CQSCache.getCachedQueue(user, request);
        
        HashMap<String, String> attributes = Util.fillAllSetAttributesRequests(request);
        HashMap<String, String> postVars = new HashMap<String, String>();
        
        for (String attributeName : attributes.keySet()) {
        	
            String value = attributes.get(attributeName);
        
            if (attributeName.equals(CQSConstants.VISIBILITY_TIMEOUT)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < 0 || v > CMBProperties.getInstance().getCQSMaxVisibilityTimeOut()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, CQSConstants.VISIBILITY_TIMEOUT + " must be between 0 and " + CMBProperties.getInstance().getCQSMaxVisibilityTimeOut() + " seconds");
                }
                
            	queue.setVisibilityTO(v);
                postVars.put(CQSConstants.COL_VISIBILITY_TO, value);
            
            } else if (attributeName.equals(CQSConstants.POLICY)) {
            	
        		if (value != null && !value.equals("")) {
	            	
        			// validate policy before updating
        			
        			try {
        				new CMBPolicy(value);
        			} catch (Exception ex) {
	                    logger.warn("event=invalid_policy queue_url=" + queue.getRelativeUrl() + " policy=" + value, ex);
	        			throw ex;
	        		}
        		}
            	
                queue.setPolicy(value);
                postVars.put(CQSConstants.COL_POLICY, value);
            
            } else if (attributeName.equals(CQSConstants.MAXIMUM_MESSAGE_SIZE)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v > CMBProperties.getInstance().getCQSMaxMessageSize()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, CQSConstants.MAXIMUM_MESSAGE_SIZE + " cannot be over " + CMBProperties.getInstance().getCQSMaxMessageSize() + " bytes");
                }
                
            	queue.setMaxMsgSize(v);
                postVars.put(CQSConstants.COL_MAX_MSG_SIZE, value);
            
            } else if (attributeName.equals(CQSConstants.MESSAGE_RETENTION_PERIOD)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < CMBProperties.getInstance().getCQSMinMessageRetentionPeriod() || v > CMBProperties.getInstance().getCQSMaxMessageRetentionPeriod()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, CQSConstants.MESSAGE_RETENTION_PERIOD + " must be between 1 and " + CMBProperties.getInstance().getCQSMaxMessageRetentionPeriod() + " seconds");
                }
            	
                queue.setMsgRetentionPeriod(v);
                postVars.put(CQSConstants.COL_MSG_RETENTION_PERIOD, value);
            
            } else if (attributeName.equals(CQSConstants.DELAY_SECONDS)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < 0 || v > CMBProperties.getInstance().getCQSMaxMessageDelaySeconds()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, CQSConstants.DELAY_SECONDS + " must be less than " + CMBProperties.getInstance().getCQSMaxMessageDelaySeconds() + " seconds");
                }
                
            	queue.setDelaySeconds(v);
                postVars.put(CQSConstants.COL_DELAY_SECONDS, value);
           
            } else if (attributeName.equals(CQSConstants.RECEIVE_MESSAGE_WAIT_TIME_SECONDS)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < 0 || v > CMBProperties.getInstance().getCMBRequestTimeoutSec()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, CQSConstants.RECEIVE_MESSAGE_WAIT_TIME_SECONDS + " must be 20 seconds or less");
                }
                
            	queue.setReceiveMessageWaitTimeSeconds(v);
                postVars.put(CQSConstants.COL_WAIT_TIME_SECONDS, value);
           
            } else if (attributeName.equals(CQSConstants.NUMBER_OF_PARTITIONS)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < 1) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.NUMBER_OF_PARTITIONS + " should be at least 1");
                }
                
            	queue.setNumberOfPartitions(v);
                postVars.put(CQSConstants.COL_NUMBER_PARTITIONS, value);
           
            } else if (attributeName.equals(CQSConstants.NUMBER_OF_SHARDS)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < 1 || v > 100) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.NUMBER_OF_SHARDS + " should be between 1 and 100");
                }
                
            	queue.setNumberOfShards(v);
                postVars.put(CQSConstants.COL_NUMBER_SHARDS, value);
                
            	for (int shard=0; shard<v; shard++) {
            		PersistenceFactory.getCQSMessagePersistence().checkCacheConsistency(queue.getRelativeUrl(), shard, false);
            	}

            } else if (attributeName.equals(CQSConstants.IS_COMPRESSED)) {
            	
            	boolean isCompressed = Boolean.parseBoolean(value);
            	queue.setCompressed(isCompressed);
                postVars.put(CQSConstants.COL_COMPRESSED, value);
            	
            } else {
                throw new CMBException(CMBErrorCodes.InvalidAttributeName, "Attribute.Name: " + attributeName + " is not a valid attribute");
            }
        }

        PersistenceFactory.getQueuePersistence().updateQueueAttribute(queue.getRelativeUrl(), postVars);
        
        String out = CQSQueuePopulator.setQueueAttributesResponse();
        writeResponse(out, response);
        
        return true;
	}
}
