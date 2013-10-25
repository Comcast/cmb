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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.controller.HealthCheckShallow;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
/**
 * The main controller for CQS
 * @author baosen, bwolf, vvenkatraman, aseem
 *
 */
public class CQSControllerServlet extends CMBControllerServlet {

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(CQSControllerServlet.class);
    
    private static volatile HashMap<String, CQSAction> actionMap;
    
    protected static volatile ICQSMessagePersistence messagePersistence = null;  
    
    public static volatile AtomicLong lastCQSPingMinute = new AtomicLong(0);
    
    public void initPersistence() {
        messagePersistence = PersistenceFactory.getCQSMessagePersistence();
    }

    @Override
    public void init() throws ServletException {
        
    	try {
        
    		super.init();
    		
            try {
	    		if (CMBProperties.getInstance().isCQSLongPollEnabled()) {
		    		CQSLongPollReceiver.listen();
		            CQSLongPollSender.init();
	            }
            } catch (Exception ex) {
            	logger.warn("event=failed_to_start_longpoll_module", ex);
            }
        
            initPersistence();
            initActions();

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
            ObjectName name = new ObjectName("com.comcast.cqs.controller:type=CQSMonitorMBean");
            
            if (!mbs.isRegistered(name)) {
            	mbs.registerMBean(CQSMonitor.getInstance(), name);
            }
            
    	} catch (Exception ex) {
            throw new ServletException(ex);
        }
    }

    @SuppressWarnings("serial")
    private void initActions() {
        
        final CQSCreateQueueAction createQueueAction = new CQSCreateQueueAction();
        final CQSDeleteQueueAction deleteQueueAction = new CQSDeleteQueueAction();
        final CQSListQueuesAction listQueuesAction = new CQSListQueuesAction();
        final CQSGetQueueUrlAction getQueueUrlAction = new CQSGetQueueUrlAction();
        final CQSSendMessageAction sendMessageAction = new CQSSendMessageAction();
        final CQSReceiveMessageAction receiveMessageAction = new CQSReceiveMessageAction();
        final CQSDeleteMessageAction deleteMessageAction = new CQSDeleteMessageAction();
        final CQSChangeMessageVisibilityAction changeMessageVisibilityAction = new CQSChangeMessageVisibilityAction();
        final CQSReceiveMessageBodyAction receiveMessageBodyAction = new CQSReceiveMessageBodyAction();
        final CQSSendMessageBatchAction sendMessageBatchAction = new CQSSendMessageBatchAction();
        final CQSDeleteMessageBatchAction deleteMessageBatchAction = new CQSDeleteMessageBatchAction();
        final CQSChangeMessageVisibilityBatchAction changeMessageVisibilityBatchAction = new CQSChangeMessageVisibilityBatchAction();
        final CQSAddPermissionAction addPermissionAction = new CQSAddPermissionAction();
        final CQSRemovePermissionAction removePermissionAction = new CQSRemovePermissionAction();
        final CQSGetQueueAttributesAction getQueueAttributesAction = new CQSGetQueueAttributesAction();
        final CQSSetQueueAttributesAction setQueueAttributesAction = new CQSSetQueueAttributesAction();
        final CQSClearQueueAction clearQueueAction = new CQSClearQueueAction();
        final CQSPeekMessageAction peekMessageAction = new CQSPeekMessageAction();
        final CQSGetAPIStatsAction getAPIStats = new CQSGetAPIStatsAction();
        final HealthCheckShallow healthCheckShallow = new HealthCheckShallow();
        final CQSManageServiceAction clearCache = new CQSManageServiceAction();
        
        actionMap = new HashMap<String, CQSAction>() {{
            put(createQueueAction.getName(), createQueueAction);
            put(deleteQueueAction.getName(), deleteQueueAction);
            put(listQueuesAction.getName(), listQueuesAction);
            put(getQueueUrlAction.getName(), getQueueUrlAction);
            put(sendMessageAction.getName(), sendMessageAction);
            put(receiveMessageAction.getName(), receiveMessageAction);
            put(peekMessageAction.getName(), peekMessageAction);
            put(deleteMessageAction.getName(), deleteMessageAction);
            put(changeMessageVisibilityAction.getName(), changeMessageVisibilityAction);
            put(receiveMessageBodyAction.getName(), receiveMessageBodyAction);
            put(sendMessageBatchAction.getName(), sendMessageBatchAction);
            put(deleteMessageBatchAction.getName(), deleteMessageBatchAction);
            put(changeMessageVisibilityBatchAction.getName(), changeMessageVisibilityBatchAction);
            put(addPermissionAction.getName(), addPermissionAction);
            put(removePermissionAction.getName(), removePermissionAction);
            put(getQueueAttributesAction.getName(), getQueueAttributesAction);
            put(setQueueAttributesAction.getName(), setQueueAttributesAction);
            put(clearQueueAction.getName(), clearQueueAction);
            put(healthCheckShallow.getName(), healthCheckShallow);
            put("healthCheckShallow", healthCheckShallow); // for backward-compatibility
            put(clearCache.getName(), clearCache);
            put(getAPIStats.getName(), getAPIStats);
        }};
    }
    

    public static void writeHeartBeat() {
    	
        long now = System.currentTimeMillis();
    	
    	if (lastCQSPingMinute.getAndSet(now/(1000*60)) != now/(1000*60)) {

        	try {

        		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCQSKeyspace());

        		// write ping
        		
        		String serverIp = InetAddress.getLocalHost().getHostAddress();
        		String serverPort = CMBProperties.getInstance().getCQSServerPort() + "";
        		
        		logger.info("event=ping version=" + CMBControllerServlet.VERSION + " ip=" + serverIp + " port=" + serverPort);
        		
        		Map<String, String> values = new HashMap<String, String>();
	        	
        		values.put("timestamp", now + "");
	        	values.put("port", CMBProperties.getInstance().getCQSLongPollPort() + "");
	        	values.put("jmxport", System.getProperty("com.sun.management.jmxremote.port", "0"));
	        	values.put("dataCenter", CMBProperties.getInstance().getCMBDataCenter());
	        	values.put("serviceUrl", CMBProperties.getInstance().getCQSServiceUrl());
	        	values.put("redisServerList", CMBProperties.getInstance().getRedisServerList());
	        	
                cassandraHandler.insertOrUpdateRow(serverIp + ":" + serverPort, "CQSAPIServers", values, CMBProperties.getInstance().getWriteConsistencyLevel());
                
        	} catch (Exception ex) {
        		logger.warn("event=ping_failed", ex);
        	}
        }
    }
    
    @Override
    protected boolean handleAction(String action, User user, AsyncContext asyncContext) throws Exception {
    	
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
    	
        writeHeartBeat();
    	
    	if (!CMBProperties.getInstance().getCQSServiceEnabled()) {
            throw new CMBException(CMBErrorCodes.InternalError, "CQS service is disabled");
    	}
    	
        if (!actionMap.containsKey(action)) {
            throw new CMBException(CMBErrorCodes.InvalidAction, action + " is not a valid action");
        }
        
    	long ts1 = System.currentTimeMillis();
        
        if (messagePersistence == null || actionMap == null) {
            init();
        }
            	
        CQSQueue queue = null;
        
        if (!action.equals("CreateQueue") && !action.equals("healthCheckShallow") && !action.equals("HealthCheck") && !action.equals("ManageService") && !action.equals("GetQueueUrl") && !action.equals("ListQueues") && !action.equals("GetAPIStats")) {
            queue = CQSCache.getCachedQueue(user, request);
    	}

        if (isAuthenticationRequired(action)) {
        
            CMBPolicy policy = new CMBPolicy();
            
            if (queue != null) {
            	policy.fromString(queue.getPolicy());
            }
            
            if (!actionMap.get(action).isActionAllowed(user, request, "CQS", policy)) {
                throw new CMBException(CMBErrorCodes.AccessDenied, "You don't have permission for " + actionMap.get(action).getName());
            }
        }
        
    	valueAccumulator.addToCounter(AccumulatorName.CQSPreDoAction, System.currentTimeMillis() - ts1);

    	return actionMap.get(action).doAction(user, asyncContext);
    }

    @Override
    protected boolean isAuthenticationRequired(String action) {
        
        if (!actionMap.containsKey(action)) {
            throw new IllegalArgumentException("action not supported:" + action);
        }
        return actionMap.get(action).isAuthRequired();        
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
    public void destroy() {
        
    	try {
    		
            logger.info("event=servlet_destroy");
            
            RedisCachedCassandraPersistence.executor.shutdown();
            RedisCachedCassandraPersistence.revisibilityExecutor.shutdown();
            CQSLongPollSender.shutdown();
            CQSLongPollReceiver.shutdown();
            
        } catch(Exception e) {
            logger.error("event=exception_while_shutting_down_executors", e);
        }
    }
}
