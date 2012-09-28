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
package com.comcast.cns.controller;

import java.util.HashMap;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.comcast.cmb.common.controller.Action;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.controller.HealthCheckShallow;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.cns.persistence.ICNSTopicPersistence;



/**
 * Servlet for handling all CNS actions
 * @author vvenkatraman, jorge, michael, bwolf, aseem, baosen
 */
public class CNSControllerServlet extends CMBControllerServlet {
	
	private static final long serialVersionUID = 1L;
	protected static volatile ICNSTopicPersistence topicHandler;
	private static volatile ICNSSubscriptionPersistence subscriptionHandler;
    private static volatile HashMap<String, Action> actionMap;

    /**
     * NodeName global constant is used to identify this process uniquely across all API servers
     * and is used to identify creators of recovery logs
     */
    private static volatile String NodeName = UUID.randomUUID().toString();
    
    public static String getNodeName() {
    	return NodeName;
    }
    
    /**
     * Used by unit tests to ensure we can start with a fresh baseline for testing recovery
     * and overflow logic   
     */
    public static void resetNodeName() {
    	NodeName = UUID.randomUUID().toString();
    }
    
    /**
     * Default constructor. 
     */
    public CNSControllerServlet() {
    }

    protected boolean isValidAction(String action) throws ServletException {
    	
    	if (action == null) {
    		return false;
    	}
    	
        if (actionMap == null) {
        	init();
        }
        
        return actionMap.containsKey(action);
    }    
    
    @Override    
    public void init() throws ServletException {
        
    	super.init();
    	
    	topicHandler = PersistenceFactory.getTopicPersistence();
    	subscriptionHandler = PersistenceFactory.getSubscriptionPersistence();
    	
    	final CNSConfirmSubscriptionAction confSub = new CNSConfirmSubscriptionAction();
    	final CNSPublishAction pub = new CNSPublishAction();
    	final CNSCreateTopicAction createT = new CNSCreateTopicAction();
    	final CNSDeleteTopicAction delT = new CNSDeleteTopicAction();
    	final CNSListTopicsAction listT = new CNSListTopicsAction();
    	final CNSSubscribeAction sub = new CNSSubscribeAction();
    	final CNSUnsubscribeAction unSub = new CNSUnsubscribeAction();
    	final CNSListSubscriptionsAction listSub = new CNSListSubscriptionsAction();
    	final CNSListSubscriptionsByTopicAction listSybTnyT = new CNSListSubscriptionsByTopicAction();
    	final CNSSetSubscriptionAttributesAction setSubA = new CNSSetSubscriptionAttributesAction();
    	final CNSGetSubscriptionAttributesAction getSubA = new CNSGetSubscriptionAttributesAction();
    	final CNSSetTopicAttributesAction setTA = new CNSSetTopicAttributesAction();
    	final CNSGetTopicAttributesAction getTA = new CNSGetTopicAttributesAction();
    	final CNSAddPermissionAction addPerm = new CNSAddPermissionAction();
    	final CNSRemovePermissionAction remPerm = new CNSRemovePermissionAction();
    	final HealthCheckShallow healthC = new HealthCheckShallow();
    	
    	actionMap = new HashMap<String, Action>(){{
    	    put(confSub.getName(), confSub);    	    
    	    put(pub.getName(), pub);
    	    put(createT.getName(), createT);
    	    put(delT.getName(), delT);
    	    put(listT.getName(), listT);
    	    put(sub.getName(), sub);
    	    put(unSub.getName(), unSub);
    	    put(listSub.getName(), listSub);
    	    put(listSybTnyT.getName(), listSybTnyT);
    	    put(setSubA.getName(), setSubA);
    	    put(getSubA.getName(), getSubA);
    	    put(setTA.getName(), setTA);
    	    put(getTA.getName(), getTA);
    	    put(addPerm.getName(), addPerm);
    	    put(remPerm.getName(), remPerm);
    	    put(healthC.getName(), healthC);
    	}};
    }
    
    @Override
    protected boolean isAuthenticationRequired(String action) {
        
        if (!actionMap.containsKey(action)) {
            throw new IllegalArgumentException("action not supported:" + action);
        }
        return actionMap.get(action).isAuthRequired();        
    }

    @Override
    protected boolean handleAction(String action, User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
    	
    	if (!CMBProperties.getInstance().getCNSServiceEnabled()) {
            throw new CMBException(CMBErrorCodes.InternalError, "CNS service is disabled");
    	}
    	
    	if (actionMap == null || subscriptionHandler == null || topicHandler == null) {
    		init();
    	}
    	
    	if (isAuthenticationRequired(action)) {

    		String topicArn = request.getParameter("TopicArn");
            
    		CNSTopicAttributes attributes = CNSCache.getTopicAttributes(topicArn);	        
            if (attributes != null) {
                if (!actionMap.get(action).isActionAllowed(user, request, "CNS", new CMBPolicy(attributes.getPolicy()))) {
                    throw new CMBException(CMBErrorCodes.AccessDenied, "You don't have permission for " + actionMap.get(action).getName());
                }
            }
    	}

    	if (actionMap.containsKey(action)) {
    		return actionMap.get(action).doAction(user, request, response);
    	}
    	
        throw new CMBException(CMBErrorCodes.InvalidAction, action + " is not a valid action");
    }
    
}
