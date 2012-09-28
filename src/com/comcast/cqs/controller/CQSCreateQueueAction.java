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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.io.CQSQueuePopulator;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;

/**
 * Create queue action
 * @author baosen, vvenkatraman, bwolf, aseem
 *
 */
public class CQSCreateQueueAction extends CQSAction {
	
	public CQSCreateQueueAction() {
		super("CreateQueue");
	}
	
	@Override
	public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
		
        String queueName = request.getParameter("QueueName");
        
        if (queueName == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "This request must contain the parameter QueueName");
        }
        
        queueName = queueName.trim();
        
        Pattern p = Pattern.compile("[a-zA-Z0-9-_]+");

        if (queueName == null || queueName.length() == 0) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "QueueName not found");
        }

        if (queueName.length() > CMBProperties.getInstance().getMaxQueueNameLength()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "QueueName " + queueName + " is too long. Maximum is " + CMBProperties.getInstance().getMaxQueueNameLength());
        }

        Matcher m = p.matcher(queueName);
        
        if (!m.matches()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "QueueName " + queueName + " is invalid. Only alphanumeric and hyphen and underscore allowed.");
        }

        CQSQueue newQueue = new CQSQueue(queueName, user.getUserId());
        CQSQueue existingQueue = PersistenceFactory.getQueuePersistence().getQueue(newQueue.getRelativeUrl());
        
        // Populating the attribute field if any are given
        boolean throwQueueExistsError = false;
        int index = 1;
        String attributeName = request.getParameter("Attribute." + index + ".Name");

        while (attributeName != null) {
            
            String attributeValue = request.getParameter("Attribute." + index + ".Value");

            if (attributeValue == null) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Attribute: " + attributeName + " does not have a corresponding attribute value");
            }

            if (attributeName.equals(CQSConstants.VISIBILITY_TIMEOUT)) {
            	
                if (existingQueue != null && existingQueue.getVisibilityTO() != Integer.parseInt(attributeValue)) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setVisibilityTO(Integer.parseInt(attributeValue));
                
            } else if (attributeName.equals(CQSConstants.POLICY)) {
            	
                if (existingQueue != null && !existingQueue.getPolicy().equals(attributeValue)) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setPolicy(attributeValue);
                
            } else if (attributeName.equals(CQSConstants.MAXIMUM_MESSAGE_SIZE)) {
            	
                if (existingQueue != null && existingQueue.getMaxMsgSize() != Integer.parseInt(attributeValue)) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setMaxMsgSize(Integer.parseInt(attributeValue));
                
            } else if (attributeName.equals(CQSConstants.MESSAGE_RETENTION_PERIOD)) {
            	
                if (existingQueue != null && existingQueue.getMsgRetentionPeriod() != Integer.parseInt(attributeValue)) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setMsgRetentionPeriod(Integer.parseInt(attributeValue));
                
            } else if (attributeName.equals(CQSConstants.DELAY_SECONDS)) {
            	
                if (existingQueue != null && existingQueue.getDelaySeconds() != Integer.parseInt(attributeValue)) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setDelaySeconds(Integer.parseInt(attributeValue));
                
            } else {
                throw new CMBException(CMBErrorCodes.InvalidRequest, "Attribute: " + attributeName + " is not a valid attribute");
            }

            index++;
            attributeName = request.getParameter("Attribute." + index + ".Name");
        }
        
        if (throwQueueExistsError) {
            throw new CMBException(CQSErrorCodes.QueueNameExists, "Queue name with " + queueName + " exists");
        }
        
        if (existingQueue == null) { // create queue only if it doesn't exist yet
        	PersistenceFactory.getQueuePersistence().createQueue(newQueue);
        }
        
        //initialize Cache state for the queue
        RedisCachedCassandraPersistence.getInstance().checkCacheConsistency(newQueue.getRelativeUrl(), false);
        
        String out = CQSQueuePopulator.getCreateQueueResponse(newQueue);

        response.getWriter().println(out);
        
        return true;
	}
	
	@Override
	public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) {
	    return true;
	}
}
