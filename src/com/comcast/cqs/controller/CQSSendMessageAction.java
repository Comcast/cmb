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

import java.util.Calendar;
import java.util.HashMap;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
/**
 * Send message
 * @author aseem, bwolf, vvenkatraman, baosen
 *
 */
public class CQSSendMessageAction extends CQSAction {
	
    protected static Logger logger = Logger.getLogger(CQSSendMessageAction.class);
	
	public CQSSendMessageAction() {
		super("SendMessage");
	}
	
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
	    CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
		long ts2 = System.currentTimeMillis();
        
        String messageBody = request.getParameter(CQSConstants.MESSAGE_BODY);

        if (messageBody == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "MessageBody not found");
        }

        if (messageBody.length() > CMBProperties.getInstance().getCQSMaxMessageSize()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Value for parameter MessageBody is invalid. Reason: Message body must be shorter than " + CMBProperties.getInstance().getCQSMaxMessageSize() + " bytes");
        }
        
        if (!Util.isValidUnicode(messageBody)) {
            throw new CMBException(CMBErrorCodes.InvalidMessageContents, "The message contains characters outside the allowed set.");
        }

        HashMap<String, String> attributes = new HashMap<String, String>();
        
        String delaySecondsReq = request.getParameter(CQSConstants.DELAY_SECONDS);

        if (delaySecondsReq != null) {

        	try {
            
        		int delaySeconds = Integer.parseInt(delaySecondsReq);
                
        		if (delaySeconds < 0 || delaySeconds > CMBProperties.getInstance().getCQSMaxMessageDelaySeconds()) {
                    throw new CMBException(CMBErrorCodes.InvalidParameterValue, "DelaySeconds should be from 0 to " + CMBProperties.getInstance().getCQSMaxMessageDelaySeconds());
                } else {
                    attributes.put(CQSConstants.DELAY_SECONDS, "" + delaySeconds);
                }
        		
            } catch (NumberFormatException e) {
                // ignore the exception
                logger.error("Exception when parsing DelaySeconds: " + delaySecondsReq);
            }
        }
        
        attributes.put(CQSConstants.SENDER_ID, user.getUserId());
        attributes.put(CQSConstants.SENT_TIMESTAMP, "" + Calendar.getInstance().getTimeInMillis());
        attributes.put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, "0");
        attributes.put(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, "");
        
        CQSMessage message = new CQSMessage(messageBody, attributes);
        CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.SendMessageArgumentCheck, (System.currentTimeMillis() - ts2));

        String receiptHandle = PersistenceFactory.getCQSMessagePersistence().sendMessage(queue, message);

        if (receiptHandle == null || receiptHandle.isEmpty()) {
            throw new CMBException(CMBErrorCodes.InternalError, "Failed to add message to queue");
        }
        
        message.setReceiptHandle(receiptHandle);
        message.setMessageId(receiptHandle);
        String out = CQSMessagePopulator.getSendMessageResponse(message);
        response.getWriter().println(out);
        CQSMonitor.getInstance().addNumberOfMessagesReceived(queue.getRelativeUrl(), 1);
        
        CQSLongPollSender.send(queue.getArn());

        return true;
	}
}
