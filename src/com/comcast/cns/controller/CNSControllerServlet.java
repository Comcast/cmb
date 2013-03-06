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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import me.prettyprint.hector.api.HConsistencyLevel;

import com.comcast.cmb.common.controller.Action;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.controller.HealthCheckShallow;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.cns.persistence.ICNSTopicPersistence;
import com.comcast.cqs.controller.CQSManageServiceAction;

/**
 * Servlet for handling all CNS actions
 * @author vvenkatraman, jorge, michael, bwolf, aseem, baosen
 */
public class CNSControllerServlet extends CMBControllerServlet {
	
	private static final long serialVersionUID = 1L;
	protected static volatile ICNSTopicPersistence topicHandler;
	private static volatile ICNSSubscriptionPersistence subscriptionHandler;
    private static volatile HashMap<String, Action> actionMap;

    public static volatile AtomicLong lastCNSPingMinute = new AtomicLong(0);
    
    private static Logger logger = Logger.getLogger(CNSControllerServlet.class);

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
    	
        try {
        	
	    	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
	        ObjectName name = new ObjectName("com.comcast.cns.controller:type=CNSMonitorMBean");
	        
	        if (!mbs.isRegistered(name)) {
	        	mbs.registerMBean(CNSMonitor.getInstance(), name);
	        }
	        
        } catch (Exception ex) {
        	logger.warn("event=failed_to_register_monitor", ex);
        }
    	
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
        final CQSManageServiceAction clearCache = new CQSManageServiceAction();
        final CNSGetAPIStatsAction getAPIStats = new CNSGetAPIStatsAction();
    	
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
            put("healthCheckShallow", healthCheckShallow); // for backward-compatibility
    	    put(getWorkerStats.getName(), getWorkerStats);
    	    put(manageWorker.getName(), manageWorker);
    	    put(clearCache.getName(), clearCache);
    	    put(getAPIStats.getName(), getAPIStats);
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
    protected boolean handleAction(String action, User user, AsyncContext asyncContext) throws Exception {
    	
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
    	
        long now = System.currentTimeMillis();
    	
    	if (lastCNSPingMinute.getAndSet(now/(1000*60)) != now/(1000*60)) {

        	try {

        		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCNSKeyspace());

        		// write ping
        		
        		String hostAddress = InetAddress.getLocalHost().getHostAddress();
        		String serviceUrl = CMBProperties.getInstance().getCNSServerUrl();
        		String servicePort = "";
        		
        		int startIdx = serviceUrl.indexOf(":", "https://".length());
        		int endIdx = serviceUrl.indexOf("/", startIdx);
        		
        		if (startIdx>0 && endIdx>startIdx) {
        			servicePort = serviceUrl.substring(startIdx+1, endIdx);
        		}
        		
        		logger.info("event=ping version=" + CMBControllerServlet.VERSION + " ip=" + hostAddress);
        		
        		Map<String, String> values = new HashMap<String, String>();
        		
	        	values.put("timestamp", now + "");
	        	values.put("jmxport", System.getProperty("com.sun.management.jmxremote.port", "0"));
	        	values.put("dataCenter", CMBProperties.getInstance().getCMBDataCenter());
	        	values.put("serviceUrl", serviceUrl);
	        	
                cassandraHandler.insertOrUpdateRow(hostAddress + ":" + servicePort, "CNSAPIServers", values, HConsistencyLevel.QUORUM);
                
        	} catch (Exception ex) {
        		logger.warn("event=ping_failed", ex);
        	}
        }
    	
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
    		return actionMap.get(action).doAction(user, asyncContext);
    	}
    	
        throw new CMBException(CMBErrorCodes.InvalidAction, action + " is not a valid action");
    }
}
