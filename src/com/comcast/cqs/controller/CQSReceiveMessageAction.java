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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
/**
 * Receive message
 * @author aseem, baosen, bwolf, vvenkatraman
 *
 */
public class CQSReceiveMessageAction extends CQSAction {
	
    private static Logger logger = Logger.getLogger(CQSReceiveMessageAction.class);

	public CQSReceiveMessageAction() {
		super("ReceiveMessage");
	}
	
	public CQSReceiveMessageAction(String actionName) {
	    super(actionName);
	}
	
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
        
    	CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
        
        Map<String, String[]> requestParams = request.getParameterMap();
        List<String> filterAttributes = new ArrayList<String>();
        
        for (String k: requestParams.keySet()) {
        	if (k.contains(CQSConstants.ATTRIBUTE_NAME)) {
        		filterAttributes.add(requestParams.get(k)[0]);
        	}
        }
    	
    	request.setFilterAttributes(filterAttributes);
    	request.setQueue(queue);
    	
        HashMap<String, String> msgParam = new HashMap<String, String>();
        
        if (request.getParameter(CQSConstants.MAX_NUMBER_OF_MESSAGES) != null) {
            
        	int maxNumberOfMessages = Integer.parseInt(request.getParameter(CQSConstants.MAX_NUMBER_OF_MESSAGES));
            
        	if (maxNumberOfMessages < 1 || maxNumberOfMessages > CMBProperties.getInstance().getCQSMaxReceiveMessageCount()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, "The value for MaxNumberOfMessages is not valid (must be from 1 to " + CMBProperties.getInstance().getCQSMaxReceiveMessageCount() + ").");
            }
        	
            msgParam.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "" + maxNumberOfMessages);
        }

        if (request.getParameter(CQSConstants.VISIBILITY_TIMEOUT) != null) {
        	msgParam.put(CQSConstants.VISIBILITY_TIMEOUT, request.getParameter(CQSConstants.VISIBILITY_TIMEOUT));
        }
        
        // receive timeout overrides queue default timeout if present 
        
        int waitTimeSeconds = queue.getReceiveMessageWaitTimeSeconds();
        
        if (request.getParameter(CQSConstants.WAIT_TIME_SECONDS) != null) {
        	try {
        		waitTimeSeconds = Integer.parseInt(request.getParameter(CQSConstants.WAIT_TIME_SECONDS));
        	} catch (NumberFormatException ex) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.WAIT_TIME_SECONDS + " must be an integer number.");
        	}
        }
        	
        if (waitTimeSeconds > 0) {
        	
        	// we are already setting wait time in main controller servlet, we are just doing
        	// this here again to throw appropriate error messages for invalid parameters
        	
        	if (!CMBProperties.getInstance().isCQSLongPollEnabled()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Long polling not enabled.");
        	}

        	if (waitTimeSeconds < 1 || waitTimeSeconds > 20) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.WAIT_TIME_SECONDS + " must be an integer number between 1 and 20.");
        	}
        	
        	//asyncContext.setTimeout(waitTimeSeconds * 1000);
            //request.setWaitTime(waitTimeSeconds * 1000);
        }

        List<CQSMessage> messageList = PersistenceFactory.getCQSMessagePersistence().receiveMessage(queue, msgParam);
        
        // wait for long poll if desired
        
        if (messageList.size() == 0 && waitTimeSeconds > 0) {
        	
        	// put context on async queue to wait for long poll
        	
        	logger.debug("event=queueing_context queue_arn=" + queue.getArn() + " wait_time_sec=" + waitTimeSeconds);
        	
        	CQSLongPollReceiver.contextQueues.putIfAbsent(queue.getArn(), new ConcurrentLinkedQueue<AsyncContext>());
			ConcurrentLinkedQueue<AsyncContext> contextQueue = CQSLongPollReceiver.contextQueues.get(queue.getArn());
			
			if (contextQueue.offer(asyncContext)) {
	            request.setIsQueuedForProcessing(true);
			}
			
        	/*CQSLongPollReceiver.queueMonitors.putIfAbsent(queue.getArn(), new Object());
        	Object monitor = CQSLongPollReceiver.queueMonitors.get(queue.getArn());
        	long referenceTime = System.currentTimeMillis();
        	
        	synchronized (monitor) {
        		
        		while (messageList.size() == 0) {
        		
        			long now = System.currentTimeMillis();
        			long waitPeriodMillis = waitTimeSeconds*1000 - (now-referenceTime); 
        			
        			if (waitPeriodMillis <= 0) {
        				break;
        			}
        			
        			logger.info("event=waiting_for_longpoll millis=" + waitPeriodMillis);
        			
        			monitor.wait(waitPeriodMillis);
        			messageList = PersistenceFactory.getCQSMessagePersistence().receiveMessage(queue, msgParam);
        		}
        		
    			logger.info("event=done_waiting");
        	}*/

        } else {

            CQSMonitor.getInstance().addNumberOfMessagesReturned(queue.getRelativeUrl(), messageList.size());
            List<String> receiptHandles = new ArrayList<String>();
            
            for (CQSMessage message : messageList) {
            	receiptHandles.add(message.getReceiptHandle());
            }
            
            request.setReceiptHandles(receiptHandles);
            String out = CQSMessagePopulator.getReceiveMessageResponseAfterSerializing(messageList, filterAttributes);
            response.getWriter().println(out);
            
        }
        
        return messageList != null && messageList.size() > 0 ? true : false;
    }
}
