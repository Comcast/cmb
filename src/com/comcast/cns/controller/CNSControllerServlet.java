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
import com.comcast.cmb.common.controller.ClearCache;
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
    	
    	final CNSConfirmSubscriptionAction confirmSubscription = new CNSConfirmSubscriptionAction();
    	final CNSPublishAction publish = new CNSPublishAction();
    	final CNSCreateTopicAction createTopic = new CNSCreateTopicAction();
    	final CNSDeleteTopicAction deleteTopic = new CNSDeleteTopicAction();
    	final CNSListTopicsAction listTopics = new CNSListTopicsAction();
    	final CNSSubscribeAction subscribe = new CNSSubscribeAction();
    	final CNSUnsubscribeAction unsubscribe = new CNSUnsubscribeAction();
    	final CNSListSubscriptionsAction listSubscriptions = new CNSListSubscriptionsAction();
    	final CNSListSubscriptionsByTopicAction listSubscriptionsByTopic = new CNSListSubscriptionsByTopicAction();
    	final CNSSetSubscriptionAttributesAction setSubscriptionAttributes = new CNSSetSubscriptionAttributesAction();
    	final CNSGetSubscriptionAttributesAction getSubscriptionAttributes = new CNSGetSubscriptionAttributesAction();
    	final CNSSetTopicAttributesAction setTopicAttributes = new CNSSetTopicAttributesAction();
    	final CNSGetTopicAttributesAction getTopicAttributes = new CNSGetTopicAttributesAction();
    	final CNSAddPermissionAction addPermission = new CNSAddPermissionAction();
    	final CNSRemovePermissionAction removePermission = new CNSRemovePermissionAction();
    	final CNSGetWorkerStatsAction getWorkerStats = new CNSGetWorkerStatsAction();
    	final CNSManageWorkerAction manageWorker = new CNSManageWorkerAction();
    	final HealthCheckShallow healthCheckShallow = new HealthCheckShallow();
        final ClearCache clearCache = new ClearCache();
    	
    	actionMap = new HashMap<String, Action>(){{
    	    put(confirmSubscription.getName(), confirmSubscription);    	    
    	    put(publish.getName(), publish);
    	    put(createTopic.getName(), createTopic);
    	    put(deleteTopic.getName(), deleteTopic);
    	    put(listTopics.getName(), listTopics);
    	    put(subscribe.getName(), subscribe);
    	    put(unsubscribe.getName(), unsubscribe);
    	    put(listSubscriptions.getName(), listSubscriptions);
    	    put(listSubscriptionsByTopic.getName(), listSubscriptionsByTopic);
    	    put(setSubscriptionAttributes.getName(), setSubscriptionAttributes);
    	    put(getSubscriptionAttributes.getName(), getSubscriptionAttributes);
    	    put(setTopicAttributes.getName(), setTopicAttributes);
    	    put(getTopicAttributes.getName(), getTopicAttributes);
    	    put(addPermission.getName(), addPermission);
    	    put(removePermission.getName(), removePermission);
    	    put(healthCheckShallow.getName(), healthCheckShallow);
    	    put(getWorkerStats.getName(), getWorkerStats);
    	    put(manageWorker.getName(), manageWorker);
    	    put(clearCache.getName(), clearCache);
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
