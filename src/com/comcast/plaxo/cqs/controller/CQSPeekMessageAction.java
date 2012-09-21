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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cqs.io.CQSMessagePopulator;
import com.comcast.plaxo.cqs.model.CQSMessage;
import com.comcast.plaxo.cqs.model.CQSQueue;
import com.comcast.plaxo.cqs.util.CQSConstants;

/**
 * Peek message in queue
 * @author bwolf
 *
 */
public class CQSPeekMessageAction extends CQSAction {
	
	public CQSPeekMessageAction() {
		super("PeekMessage");
	}

	@Override
	public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
	    CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
		List<CQSMessage> messageList = getMessages(request, true, queue);
        
        Map<String, String[]> requestParams = request.getParameterMap();
        List<String> filterAttributes = new ArrayList<String>();
        
        for (String k: requestParams.keySet()) {
        	if (k.contains(CQSConstants.ATTRIBUTE_NAME)) {
        		filterAttributes.add(requestParams.get(k)[0]);
        	}
        }
        
        String out = CQSMessagePopulator.getReceiveMessageResponseAfterSerializing(messageList, filterAttributes);
        
        response.getWriter().println(out);
        
        return messageList == null ? false : true;
	}
	
	protected List<CQSMessage> getMessages(HttpServletRequest request, boolean useParams, CQSQueue queue) throws PersistenceException, CMBException, IOException, NoSuchAlgorithmException, InterruptedException {
		
		String previousReceiptHandle = request.getParameter("PreviousReceiptHandle");
		String nextReceiptHandle = request.getParameter("NextReceiptHandle");
		
		int maxNumberOfMessages = CMBProperties.getInstance().getMaxReceiveMessageCount();
		        
        if (useParams && request.getParameter(CQSConstants.MAX_NUMBER_OF_MESSAGES) != null) {
            
        	maxNumberOfMessages = Integer.parseInt(request.getParameter(CQSConstants.MAX_NUMBER_OF_MESSAGES));
            
        	if (maxNumberOfMessages < 1 || maxNumberOfMessages > CMBProperties.getInstance().getMaxReceiveMessageCount()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, "The value for MaxNumberOfMessages is not valid (must be from 1 to " + CMBProperties.getInstance().getMaxReceiveMessageCount() + ").");
            }
        	
        }

        List<CQSMessage> messageList = PersistenceFactory.getCQSMessagePersistence().peekQueue(queue.getRelativeUrl(), previousReceiptHandle, nextReceiptHandle, maxNumberOfMessages);
        
        //CQSMonitor.Inst.addNumberOfMessagesReturned(queueUrl, messageList.size());
		
        return messageList;
	}
}
