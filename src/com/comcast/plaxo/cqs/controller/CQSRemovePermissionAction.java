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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.plaxo.cmb.common.model.CMBPolicy;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cqs.io.CQSQueuePopulator;
import com.comcast.plaxo.cqs.model.CQSQueue;
import com.comcast.plaxo.cqs.util.CQSConstants;
import com.comcast.plaxo.cqs.util.CQSErrorCodes;
import com.comcast.plaxo.cqs.util.Util;
/**
 * Renmove permissions
 * @author bwolf, vvenkatraman, baosen
 *
 */
public class CQSRemovePermissionAction extends CQSAction {

    public CQSRemovePermissionAction() {
        super("RemovePermission");
    }       

    @Override
    public boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
        CQSQueue queue = CQSControllerServlet.getCachedQueue(user, request);
        String label = request.getParameter(CQSConstants.LABEL);

        if (!Util.isValidId(label)) {
            throw new CMBException(CQSErrorCodes.InvalidBatchEntryId, "Label " + label + " is invalid. Only alphanumeric, hyphen, and underscore are allowed. It can be at most " + CMBProperties.getInstance().getMaxMessageSuppliedIdLength() + " letters long.");
        }
        
        CMBPolicy policy = new CMBPolicy(queue.getPolicy());
        
        if (policy.removeStatement(label)) {
            String policyStr = policy.toString();
            PersistenceFactory.getQueuePersistence().updatePolicy(queue.getRelativeUrl(), policyStr);
            queue.setPolicy(policyStr);
        }
        
        String out = CQSQueuePopulator.getRemovePermissionResponse();

        response.getWriter().print(out);
        
        return true;
    }

}
