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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
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

	public CQSReceiveMessageAction() {
		super("ReceiveMessage");
	}
	
	public CQSReceiveMessageAction(String actionName) {
	    super(actionName);
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
        
        return messageList != null && messageList.size() > 0 ? true : false;
    }
	
	protected List<CQSMessage> getMessages(HttpServletRequest request, boolean useParams, CQSQueue queue) throws PersistenceException, CMBException, IOException, NoSuchAlgorithmException, InterruptedException {
		
        HashMap<String, String> msgParam = new HashMap<String, String>();
        
        if (useParams && request.getParameter(CQSConstants.MAX_NUMBER_OF_MESSAGES) != null) {
            
        	int maxNumberOfMessages = Integer.parseInt(request.getParameter(CQSConstants.MAX_NUMBER_OF_MESSAGES));
            
        	if (maxNumberOfMessages < 1 || maxNumberOfMessages > CMBProperties.getInstance().getMaxReceiveMessageCount()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, "The value for MaxNumberOfMessages is not valid (must be from 1 to " + CMBProperties.getInstance().getMaxReceiveMessageCount() + ").");
            }
        	
            msgParam.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "" + maxNumberOfMessages);
        }

        if (useParams && request.getParameter(CQSConstants.VISIBILITY_TIMEOUT) != null) {
        	msgParam.put(CQSConstants.VISIBILITY_TIMEOUT, request.getParameter(CQSConstants.VISIBILITY_TIMEOUT));
        }
                
        List<CQSMessage> messageList = PersistenceFactory.getCQSMessagePersistence().receiveMessage(queue, msgParam);
        
        CQSMonitor.Inst.addNumberOfMessagesReturned(queue.getRelativeUrl(), messageList.size());
		
        return messageList;
	}

}
