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

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cqs.model.CQSMessage;
import com.comcast.plaxo.cqs.model.CQSQueue;
/**
 * 
 * @author baosen, bwolf, vvenkatraman
 *
 */
public class CQSReceiveMessageBodyAction extends CQSReceiveMessageAction {
	
	public CQSReceiveMessageBodyAction() {
		super("ReceiveMessageBody");
	}
	
	@Override
	public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
	    CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
		List<CQSMessage> messageList = getMessages(request, false, queue);
        String out = "";
        response.setContentType("text/html");
        
        if (messageList == null || messageList.size() > 0) {
        	
        	out += messageList.get(0).getBody();
	        
        	try {	        	        	    
				new JSONObject(messageList.get(0).getBody());
	        	response.setContentType("application/json");
	        } catch (JSONException ex) {
	        	// do nothing
	        }
        }

        response.getWriter().println(out);		
        
        return true;
    }
}
