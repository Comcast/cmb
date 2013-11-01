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

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSBatchResultErrorEntry;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
import com.comcast.cqs.util.Util;

/**
 * Delete message in a batch
 * @author baosen, vvenkatraman, bwolf
 *
 */
public class CQSDeleteMessageBatchAction extends CQSAction {
	
	public CQSDeleteMessageBatchAction() {
		super("DeleteMessageBatch");
	}		
	
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

	    CQSQueue queue = CQSCache.getCachedQueue(user, request);
        Map<String, String> idMap = new HashMap<String, String>();
        List<String> idList = new ArrayList<String>();
        List<CQSBatchResultErrorEntry> failedList = new ArrayList<CQSBatchResultErrorEntry>();
        int index = 1;
        
        String suppliedId = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + ".Id");
        String receiptHandle = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + "." + CQSConstants.RECEIPT_HANDLE);
        
        while (suppliedId != null && receiptHandle != null) {
            
        	if (!Util.isValidId(suppliedId)) {
                throw new CMBException(CQSErrorCodes.InvalidBatchEntryId, "Id " + suppliedId + " is invalid. Only alphanumeric, hyphen, and underscore are allowed. It can be at most " + CMBProperties.getInstance().getCQSMaxMessageSuppliedIdLength() + " letters long.");
            }
            
        	if (idList.contains(suppliedId)) {
                throw new CMBException(CQSErrorCodes.BatchEntryIdsNotDistinct, "You supplied same identifier for two messages");
            }
            
        	idList.add(suppliedId);
            
        	if (receiptHandle.isEmpty()) {
                failedList.add(new CQSBatchResultErrorEntry(suppliedId, true, "EmptyValue", "No Value Found for " + this.actionName + CQSConstants.REQUEST_ENTRY + index + "." + CQSConstants.RECEIPT_HANDLE));
            } else {
                idMap.put(suppliedId, receiptHandle);
            }
            
        	index++;
            suppliedId = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + ".Id");
            receiptHandle = request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + "." + CQSConstants.RECEIPT_HANDLE);
        }
        
        if (idMap.size() == 0) {
            throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "Both user supplied message Id and receiptHandle are required");
        }

        for (Map.Entry<String, String> entry : idMap.entrySet()) {
            receiptHandle = entry.getValue();
        	PersistenceFactory.getCQSMessagePersistence().deleteMessage(queue.getRelativeUrl(), receiptHandle);
        }
        
        String out = CQSMessagePopulator.getDeleteMessageBatchResponse(new ArrayList<String>(idMap.keySet()), failedList);
        writeResponse(out, response);
        
        return true;
    }
}
