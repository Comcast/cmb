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

import javax.servlet.AsyncContext;
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
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

		String queueName = request.getParameter("QueueName");
        
        if (queueName == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "This request must contain the parameter QueueName");
        }
        
        queueName = queueName.trim();
        
        Pattern p = Pattern.compile("[a-zA-Z0-9-_]+");

        if (queueName == null || queueName.length() == 0) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "QueueName not found");
        }

        if (queueName.length() > CMBProperties.getInstance().getCQSMaxNameLength()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "QueueName " + queueName + " is too long. Maximum is " + CMBProperties.getInstance().getCQSMaxNameLength());
        }

        Matcher m = p.matcher(queueName);
        
        if (!m.matches()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "QueueName " + queueName + " is invalid. Only alphanumeric and hyphen and underscore allowed.");
        }

        CQSQueue newQueue = new CQSQueue(queueName, user.getUserId());
        CQSQueue existingQueue = PersistenceFactory.getQueuePersistence().getQueue(newQueue.getRelativeUrl());
        
        boolean throwQueueExistsError = false;
        int index = 1;
        String attributeName = request.getParameter("Attribute." + index + ".Name");

        while (attributeName != null) {
            
            String attributeValue = request.getParameter("Attribute." + index + ".Value");

            if (attributeValue == null) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Attribute: " + attributeName + " does not have a corresponding attribute value");
            }

            if (attributeName.equals(CQSConstants.VISIBILITY_TIMEOUT)) {
            	
            	int visibilityTo = 0;
            	
            	try {
            		visibilityTo = Integer.parseInt(attributeValue);
            	} catch (NumberFormatException ex) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.VISIBILITY_TIMEOUT + " must be integer value");
            	}
            	
        		if (visibilityTo < 0 || visibilityTo > CMBProperties.getInstance().getCQSMaxVisibilityTimeOut()) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.VISIBILITY_TIMEOUT + " should be from 0 to " + CMBProperties.getInstance().getCQSMaxVisibilityTimeOut());
        		}

        		if (existingQueue != null && existingQueue.getVisibilityTO() != visibilityTo) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setVisibilityTO(visibilityTo);
                
            } else if (attributeName.equals(CQSConstants.POLICY)) {
            	
                if (existingQueue != null && !existingQueue.getPolicy().equals(attributeValue)) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setPolicy(attributeValue);
                
            } else if (attributeName.equals(CQSConstants.MAXIMUM_MESSAGE_SIZE)) {
            	
            	int maxMessageSize = 0;
            	
            	try {
            		maxMessageSize = Integer.parseInt(attributeValue);
            	} catch (NumberFormatException ex) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.MAXIMUM_MESSAGE_SIZE + " must be integer value");
            	}
            	
        		if (maxMessageSize < 0 || maxMessageSize > CMBProperties.getInstance().getCQSMaxMessageSize()) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.MAXIMUM_MESSAGE_SIZE + " should be from 0 to " + CMBProperties.getInstance().getCQSMaxMessageSize());
        		}

        		if (existingQueue != null && existingQueue.getMaxMsgSize() != maxMessageSize) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setMaxMsgSize(maxMessageSize);
                
            } else if (attributeName.equals(CQSConstants.MESSAGE_RETENTION_PERIOD)) {
            	
            	int messageRetentionPeriod = 0;
            	
            	try {
            		messageRetentionPeriod = Integer.parseInt(attributeValue);
            	} catch (NumberFormatException ex) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.MESSAGE_RETENTION_PERIOD + " must be integer value");
            	}
            	
        		if (messageRetentionPeriod < 0 || messageRetentionPeriod > CMBProperties.getInstance().getCQSMaxMessageRetentionPeriod()) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.MESSAGE_RETENTION_PERIOD + " should be from 0 to " + CMBProperties.getInstance().getCQSMaxMessageRetentionPeriod());
        		}
            	
                if (existingQueue != null && existingQueue.getMsgRetentionPeriod() != messageRetentionPeriod) {
                    throwQueueExistsError = true;
                    break;
                }
                
                newQueue.setMsgRetentionPeriod(messageRetentionPeriod);
                
            } else if (attributeName.equals(CQSConstants.DELAY_SECONDS)) {
            	
                if (existingQueue != null && existingQueue.getDelaySeconds() != Integer.parseInt(attributeValue)) {
                    throwQueueExistsError = true;
                    break;
                }
                
                int delaySeconds = 0;
                
                try {
                	delaySeconds = Integer.parseInt(attributeValue);
                } catch (NumberFormatException ex) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, "DelaySeconds must be integer value");
                }
                
        		if (delaySeconds < 0 || delaySeconds > CMBProperties.getInstance().getCQSMaxMessageDelaySeconds()) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, "DelaySeconds should be from 0 to " + CMBProperties.getInstance().getCQSMaxMessageDelaySeconds());
        		}
                
                newQueue.setDelaySeconds(delaySeconds);
                
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
