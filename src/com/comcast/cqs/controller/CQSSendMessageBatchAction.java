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
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSBatchResultErrorEntry;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
import com.comcast.cqs.util.Util;
/**
 * Send message in a batch
 * @author aseem, baosen, vvenkatraman, bwolf
 *
 */
public class CQSSendMessageBatchAction extends CQSAction {
	
    protected static Logger logger = Logger.getLogger(CQSSendMessageBatchAction.class);
	private static Random rand = new Random();
	
	public CQSSendMessageBatchAction() {
		super("SendMessageBatch");
	}
		
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

	    CQSQueue queue = CQSCache.getCachedQueue(user, request);
        List<CQSMessage> msgList = new ArrayList<CQSMessage>();
        List<String> idList = new ArrayList<String>();
        List<CQSBatchResultErrorEntry> invalidBodyIdList = new ArrayList<CQSBatchResultErrorEntry>();

        int totalMessageSize = 0;
        int index = 1;
        
        String suppliedId = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + ".Id");
        String messageBody = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + "." + CQSConstants.MESSAGE_BODY);
       
        while (suppliedId != null) {
            
        	if (!Util.isValidId(suppliedId)) {
                throw new CMBException(CQSErrorCodes.InvalidBatchEntryId, "Id " + suppliedId + " is invalid. Only alphanumeric, hyphen, and underscore are allowed. It can be at most " + CMBProperties.getInstance().getCQSMaxMessageSuppliedIdLength() + " letters long.");
            }
            
        	if (idList.contains(suppliedId)) {
                throw new CMBException(CQSErrorCodes.BatchEntryIdsNotDistinct, "Id " + suppliedId + " repeated");
            }
        	
            idList.add(suppliedId);
            
            if (messageBody == null || messageBody.isEmpty()) {
                invalidBodyIdList.add(new CQSBatchResultErrorEntry(suppliedId, true, "EmptyValue", "No value found for " + this.actionName + CQSConstants.REQUEST_ENTRY + index + "." + CQSConstants.MESSAGE_BODY));
            } else if (!com.comcast.cmb.common.util.Util.isValidUnicode(messageBody)) {
            	invalidBodyIdList.add(new CQSBatchResultErrorEntry(suppliedId, true, "InvalidMessageContents", "Invalid character was found in the message body."));
            } else {
                
            	HashMap<String, String> attributes = new HashMap<String, String>();
                String delaySecondsStr = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + "." + CQSConstants.DELAY_SECONDS);
                
                if (delaySecondsStr != null) {
                
                	Integer delaySeconds = 0;
                	
                	try {
                		delaySeconds = Integer.parseInt(delaySecondsStr);
                	} catch (NumberFormatException ex) {
                        throw new CMBException(CMBErrorCodes.InvalidParameterValue, "DelaySeconds must be integer value");
                	}
                    
                    if (delaySeconds < 0 || delaySeconds > CMBProperties.getInstance().getCQSMaxMessageDelaySeconds()) {
                        throw new CMBException(CMBErrorCodes.InvalidParameterValue, "DelaySeconds should be from 0 to " + CMBProperties.getInstance().getCQSMaxMessageDelaySeconds());
                    } else {
                        attributes.put(CQSConstants.DELAY_SECONDS, "" + delaySeconds);
                    }
                }
                
                attributes.put(CQSConstants.SENDER_ID, user.getUserId());
                attributes.put(CQSConstants.SENT_TIMESTAMP, "" + Calendar.getInstance().getTimeInMillis());
                attributes.put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, "0");
                attributes.put(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, "");
                
                CQSMessage msg = new CQSMessage(messageBody, attributes);
                
                msg.setSuppliedMessageId(suppliedId);
                msgList.add(msg);
            }
            
            if (msgList.size() > CMBProperties.getInstance().getCQSMaxMessageCountBatch()) {
                throw new CMBException(CQSErrorCodes.TooManyEntriesInBatchRequest, "Maximum number of entries per request are " + CMBProperties.getInstance().getCQSMaxMessageCountBatch() + ". You have sent " + msgList.size() + ".");
            }
            
            totalMessageSize += messageBody == null ? 0 : messageBody.length();
            
            if (totalMessageSize > CMBProperties.getInstance().getCQSMaxMessageSizeBatch()) {
                throw new CMBException(CQSErrorCodes.BatchRequestTooLong, "Batch requests cannot be longer than " + CMBProperties.getInstance().getCQSMaxMessageSizeBatch() + " bytes");
            }
            
            index++;
            
            suppliedId = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + ".Id");
            messageBody = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + "." + CQSConstants.MESSAGE_BODY);
        }
        
        if (msgList.size() == 0) {
            throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "Both user supplied message Id and message body are required");
        }
        
	    int shard = 0;
	    
	    if (queue.getNumberOfShards() > 1) {
	    	shard = rand.nextInt(queue.getNumberOfShards());
	    }
        
		Map<String, String> result = PersistenceFactory.getCQSMessagePersistence().sendMessageBatch(queue, shard, msgList);

		try {
			CQSLongPollSenderNG.send(queue.getArn());
        } catch (Exception ex) {
        	logger.warn("event=failed_to_send_longpoll_notification", ex);
        }
		
        List<String> receiptHandles = new ArrayList<String>();

        for (CQSMessage message: msgList) {
        	message.setMessageId(result.get(message.getSuppliedMessageId()));
        	message.setReceiptHandle(result.get(message.getSuppliedMessageId()));
        	receiptHandles.add(message.getReceiptHandle());
        }
        
        request.setReceiptHandles(receiptHandles);
        String out = CQSMessagePopulator.getSendMessageBatchResponse(msgList, invalidBodyIdList);
        response.getWriter().println(out);
        
        CQSMonitor.getInstance().addNumberOfMessagesReceived(queue.getRelativeUrl(), msgList.size());
        
        return true;
	}
}