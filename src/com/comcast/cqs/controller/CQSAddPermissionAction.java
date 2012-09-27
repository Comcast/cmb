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

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.plaxo.cmb.common.model.CMBPolicy;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cqs.io.CQSQueuePopulator;
import com.comcast.plaxo.cqs.model.CQSQueue;
import com.comcast.plaxo.cqs.util.CQSConstants;
import com.comcast.plaxo.cqs.util.CQSErrorCodes;
import com.comcast.plaxo.cqs.util.Util;

/**
 * Add permissions
 * @author vvenkatraman, bwolf, baosen
 */
public class CQSAddPermissionAction extends CQSAction {

    public CQSAddPermissionAction() {
        super("AddPermission");
    }       

    @Override
    public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {

        CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
        String label = request.getParameter(CQSConstants.LABEL);

        if (label == null) {
        	throw new CMBException(CMBErrorCodes.ValidationError, "Validation error detected: Value null at 'label' failed to satisfy constraint: Member must not be null");
        }

        if (!Util.isValidId(label)) {
            throw new CMBException(CQSErrorCodes.InvalidBatchEntryId, "Label " + label + " is invalid. Only alphanumeric, hyphen, and underscore are allowed. It can be at most " + CMBProperties.getInstance().getMaxMessageSuppliedIdLength() + " letters long.");
        }
        
        List<String> userList = new ArrayList<String>();
        int index = 1;
        
        String userId = request.getParameter(CQSConstants.AWS_ACCOUNT_ID + "." + index);

        IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
        
        while (userId != null) {

        	if (userId.equals("*") || userHandler.getUserById(userId) != null) { // only add user if they exist
                userList.add(userId);
            }
            
        	index++;
            userId = request.getParameter(CQSConstants.AWS_ACCOUNT_ID + "." + index);
        }
        
        if (userList.size() == 0) {
            throw new CMBException(CMBErrorCodes.NotFound, "AWSAccountId is required");
        }

        List<String> actionList = new ArrayList<String>();
        index = 1;
        String action = request.getParameter(CQSConstants.ACTION_NAME + "." + index);

        while (action != null) {
        	actionList.add(action);
            index++;
            action = request.getParameter(CQSConstants.ACTION_NAME + "." + index);
        }
        
        if (actionList.size() == 0) {
            throw new CMBException(CMBErrorCodes.NotFound, "ActionName is required");
        }
        
        CMBPolicy policy = new CMBPolicy(queue.getPolicy());
        
        if (policy.addStatement(CMBPolicy.SERVICE.CQS, label, "Allow", userList, actionList, queue.getArn(), null)) {
            PersistenceFactory.getQueuePersistence().updatePolicy(queue.getRelativeUrl(), policy.toString());
            queue.setPolicy(policy.toString());
        } else {
        	throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Value " + label + " for parameter Label is invalid. Reason: Already exists.");
        }
        
        String out = CQSQueuePopulator.getAddPermissionResponse();
        response.getWriter().print(out);
        
        return true;
    }

}
