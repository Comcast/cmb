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
package com.comcast.plaxo.cqs.controller;

import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.plaxo.cmb.common.model.CMBPolicy;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cns.controller.CNSCreateTopicAction;
import com.comcast.plaxo.cqs.io.CQSQueuePopulator;
import com.comcast.plaxo.cqs.model.CQSQueue;
import com.comcast.plaxo.cqs.util.CQSConstants;
import com.comcast.plaxo.cqs.util.Util;
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
	public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
	    CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
        String ownerId = request.getParameter("QueueOwnerAWSAccountId");

        if (ownerId == null) {
            ownerId = user.getUserId();
        }

        if (!ownerId.equals(user.getUserId())) {
        	
	        CMBPolicy policy = new CMBPolicy(queue.getPolicy());
	        
	        if (!policy.isAllowed(user, "CQS:" + this.actionName)) {
	            throw new CMBException(CMBErrorCodes.AccessDenied, "You don't have permission for " + this.actionName);
	        }
        }
        
        HashMap<String, String> attributes = Util.fillAllSetAttributesRequests(request);
        HashMap<String, String> postVars = new HashMap<String, String>();
        
        for (String attributeName : attributes.keySet()) {
        	
            String value = attributes.get(attributeName);
        
            if (attributeName.equals(CQSConstants.VISIBILITY_TIMEOUT)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < 0 || v > CMBProperties.getInstance().getMaxVisibilityTO()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "VisibilityTimeout must be from 0 to 12 hours");
                }
                
            	queue.setVisibilityTO(v);
                postVars.put(CQSConstants.COL_VISIBILITY_TO, attributes.get(attributeName));
            
            } else if (attributeName.equals(CQSConstants.POLICY)) {
            	
        		if (attributes.get(attributeName) != null && !attributes.get(attributeName).equals("")) {
	            	
        			// validate policy before updating
        			
        			try {
        				new CMBPolicy(attributes.get(attributeName));
        			} catch (Exception ex) {
	                    logger.warn("event=invalid_policy queue_url=" + queue.getRelativeUrl() + " policy=" + attributes.get(attributeName), ex);
	        			throw ex;
	        		}
        		}
            	
                queue.setPolicy(attributes.get(attributeName));
                postVars.put(CQSConstants.COL_POLICY, attributes.get(attributeName));
            
            } else if (attributeName.equals(CQSConstants.MAXIMUM_MESSAGE_SIZE)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v > CMBProperties.getInstance().getMaxMsgSize()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "MaximumMessageSize cannot be over " + CMBProperties.getInstance().getMaxMsgSize());
                }
                
            	queue.setMaxMsgSize(v);
                postVars.put(CQSConstants.COL_MAX_MSG_SIZE, attributes.get(attributeName));
            
            } else if (attributeName.equals(CQSConstants.MESSAGE_RETENTION_PERIOD)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < CMBProperties.getInstance().getMinMsgRetentionPeriod() || v > CMBProperties.getInstance().getMaxMsgRetentionPeriod()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "MessageRetentionPeriod must be from 1 minute to 14 days");
                }
            	
                queue.setMsgRetentionPeriod(v);
                postVars.put(CQSConstants.COL_MSG_RETENTION_PERIOD, attributes.get(attributeName));
            
            } else if (attributeName.equals(CQSConstants.DELAY_SECONDS)) {
                
            	if (!Util.isParsableToInt(value)) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value " + value + " for the parameter " + attributeName);
                }
                
            	int v = Integer.parseInt(value);
                
            	if (v < 0 || v > CMBProperties.getInstance().getMaxDelaySeconds()) {
                    throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "DelaySeconds must be less than 15 minutes");
                }
                
            	queue.setDelaySeconds(v);
                postVars.put(CQSConstants.COL_DELAY_SECONDS, attributes.get(attributeName));
           
            } else {
                throw new CMBException(CMBErrorCodes.InvalidAttributeName, "Attribute.Name: " + attributeName + " is not a valid attribute");
            }
        }

        PersistenceFactory.getQueuePersistence().updateQueueAttribute(queue.getRelativeUrl(), postVars);
        
        String out = CQSQueuePopulator.setQueueAttributesResponse();
        response.getWriter().print(out);
        
        return true;
	}
}
