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

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
/**
 * Delete message
 * @author baosen, vvenkatraman, bwolf
 *
 */
public class CQSDeleteMessageAction extends CQSAction {	
	
	public CQSDeleteMessageAction() {
		super("DeleteMessage");
	}	
	
	private boolean isValidReceiptHandle(String receiptHandle) {

		if (receiptHandle == null) {
			return false;
		}
		
		String elements[] = receiptHandle.split(":"); 
		
		if (elements.length < 3) {
			return false;
		}
		
		try {
			Long.parseLong(elements[elements.length-1]);
			Long.parseLong(elements[elements.length-2]);
		} catch (Exception ex) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
	    
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

		CQSQueue queue = CQSCache.getCachedQueue(user, request);
	    
        String receiptHandle = request.getParameter(CQSConstants.RECEIPT_HANDLE);

        if (receiptHandle == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "ReceiptHandle not found");
        }
        
        if (!isValidReceiptHandle(receiptHandle)) {
            throw new CMBException(CQSErrorCodes.ReceiptHandleInvalid, "The input receipt handle is invalid");
        }

        PersistenceFactory.getCQSMessagePersistence().deleteMessage(queue.getRelativeUrl(), receiptHandle);
        
        String out = CQSMessagePopulator.getDeleteMessageResponse();
        writeResponse(out, response);
        
        return true;
    }
}
