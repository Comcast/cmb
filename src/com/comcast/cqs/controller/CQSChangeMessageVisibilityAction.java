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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;

/**
 * Change message visibility
 * @author baosen, vvenkatraman, bwolf
 *
 */
public class CQSChangeMessageVisibilityAction extends CQSAction {	
	
	public CQSChangeMessageVisibilityAction() {
		super("ChangeMessageVisibility");
	}

	@Override
	public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
	    CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
        String receiptHandle = request.getParameter(CQSConstants.RECEIPT_HANDLE);
        
        if (receiptHandle == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "ReceiptHandle not found");
        }

        String visibilityTimeout = request.getParameter(CQSConstants.VISIBILITY_TIMEOUT);
        
        if ( visibilityTimeout == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "VisibilityTimeout not found");
        }

        int visibilityTO = Integer.parseInt(visibilityTimeout);

        if (visibilityTO < 0 || visibilityTO > CMBProperties.getInstance().getMaxVisibilityTO()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "VisibilityTimeout is limited from 0 to " + CMBProperties.getInstance().getMaxVisibilityTO() + " seconds");
        }
        
        @SuppressWarnings("unused")
		boolean res = PersistenceFactory.getCQSMessagePersistence().changeMessageVisibility(queue, receiptHandle, visibilityTO);

        String out = CQSMessagePopulator.getChangeMessageVisibilityResponse();
        response.getWriter().println(out);
        
        return true;
	}
}