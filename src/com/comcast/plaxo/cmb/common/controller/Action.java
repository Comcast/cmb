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
package com.comcast.plaxo.cmb.common.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.plaxo.cmb.common.model.CMBPolicy;
import com.comcast.plaxo.cmb.common.model.User;
/**
 * Abstract class representing all actions that can be performed by calling the API
 * @author aseem, bwolf, vvenkatraman, baosen
 */
public abstract class Action {
	
	protected final String actionName;
	
	public Action(String actionName) {
	    this.actionName = actionName;
	}
	
	public String getName() {
		return actionName;
	}
	
    /**
     * Perform servlet action for cqs or cns
     * @param user user object for authenticated user
     * @param request http request object
     * @param response http response object
     * @throws Exception
     * @return true if this action was performed, false otherwise. It is largely dependent
     *  on the sub-classes to override this return value with what makes sense.
     */
	public abstract boolean doAction(User user, HttpServletRequest request, HttpServletResponse response) throws Exception;
	
	/**
	 * check if an action on resource is allowed
	 * @param policy  contains a set of statement for user's permission of actions on resource
	 * @param user  authenticated user to perform the action
	 * @param action  a string for action
	 */
    public abstract boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception;
    
    /**
     * Sub-classes should override this as necessary
     * @return true if this action requries auth
     */
    public boolean isAuthRequired() {
        return true;
    }
}
