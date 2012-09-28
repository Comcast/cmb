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
import java.util.HashMap;
import java.util.concurrent.Callable;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.controller.HealthCheckShallow;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ExpiringCache;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ExpiringCache.CacheFullException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.persistence.ICQSQueuePersistence;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
import com.comcast.cqs.util.CQSErrorCodes;
/**
 * The main controller for CQS
 * @author baosen, bwolf, vvenkatraman, aseem
 *
 */
public class CQSControllerServlet extends CMBControllerServlet {

    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(CQSControllerServlet.class);
    
    private static volatile HashMap<String, CQSAction> actionMap;
    
    protected static volatile ICQSQueuePersistence queuePersistence = null;
    protected static volatile ICQSMessagePersistence messagePersistence = null;    
    
    static ExpiringCache<String, CQSQueue> queueCache = new ExpiringCache<String, CQSQueue>(CMBProperties.getInstance().getCqsCacheSizeLimit());
    
    public static class QueueCallable implements Callable<CQSQueue> {
        
    	String queueUrl = null;
        
    	public QueueCallable(String key) {
            this.queueUrl = key;
        }
        
    	@Override
        public CQSQueue call() throws Exception {
            CQSQueue queue = queuePersistence.getQueue(queueUrl);
            return queue;
        }
    }
    

    public void initPersistence() {
        queuePersistence = PersistenceFactory.getQueuePersistence();
        messagePersistence = PersistenceFactory.getCQSMessagePersistence();
    }

    @Override
    public void init() throws ServletException {
        
    	try {
        
    		super.init();
            initPersistence();
            initActions();

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
            ObjectName name = new ObjectName("com.comcast.cqs.controller:type=CQSMonitorMBean");
            
            if (!mbs.isRegistered(name)) {
            	mbs.registerMBean(CQSMonitor.Inst, name);
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
        final HealthCheckShallow healthCheckShallow = new HealthCheckShallow();
        
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
        }};
    }
    
    /**
     * 
     * @param queueUrl
     * @return Cached instance of CQSQueue given queueUrl. If none exists, we call QueueCallable and cache it
     * for config amount of time
     * @throws Exception
     */
    public static CQSQueue getCachedQueue(String queueUrl) throws Exception {
        try {
            return queueCache.getAndSetIfNotPresent(queueUrl, new QueueCallable(queueUrl), CMBProperties.getInstance().getCqsCacheExpiring() * 1000);
        } catch (CacheFullException e) {
            return new QueueCallable(queueUrl).call();
        } 
    }
    /**
     * Get a CQSQueue instance given the request and user. If no queue is found, Exception is thrown
     * @param user
     * @param request
     * @return
     * @throws Exception
     */
    public static CQSQueue getCachedQueue(User user, HttpServletRequest request) throws Exception {
        String queueUrl = null;
        CQSQueue queue = null;

        queueUrl = request.getRequestURL().toString();

        if (queueUrl != null && !queueUrl.equals("") && !queueUrl.equals("/")) {

            if (queueUrl.startsWith("http://")) {

                queueUrl = queueUrl.substring("http://".length());

                if (queueUrl.contains("/")) {
                    queueUrl = queueUrl.substring(queueUrl.indexOf("/"));
                }

            } else if (queueUrl.startsWith("https://")) {

                queueUrl = queueUrl.substring("https://".length());

                if (queueUrl.contains("/")) {
                    queueUrl = queueUrl.substring(queueUrl.indexOf("/"));
                }
            } 

            if (queueUrl.startsWith("/")) {
                queueUrl = queueUrl.substring(1);
            }

            if (queueUrl.endsWith("/")) {
                queueUrl = queueUrl.substring(0, queueUrl.length()-1);
            }

            // auto prefix with user id if omitted in request

            if (!queueUrl.contains("/") && user != null) {
                queueUrl = user.getUserId() + "/" + queueUrl;
            }

            queue = getCachedQueue(queueUrl);

            if (queue == null) {
                throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue with url " + request.getRequestURL().toString() + " doesn't exist");
            }
        } else {
            throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue with url " + request.getRequestURL().toString() + " doesn't exist");
        }
        return queue;
    }
    
    @Override
    protected boolean handleAction(String action, User user, HttpServletRequest request, HttpServletResponse response) throws Exception {
    	
    	if (!CMBProperties.getInstance().getCQSServiceEnabled()) {
            throw new CMBException(CMBErrorCodes.InternalError, "CQS service is disabled");
    	}
    	
        if (!actionMap.containsKey(action)) {
            throw new CMBException(CMBErrorCodes.InvalidAction, action + " is not a valid action");
        }
        
    	long ts1 = System.currentTimeMillis();
        
        if (queuePersistence == null || messagePersistence == null || actionMap == null) {
            init();
        }
            	
        CQSQueue queue = null;
        
        if (!action.equals("CreateQueue") && !action.equals("healthCheckShallow") && !action.equals("GetQueueUrl") && !action.equals("ListQueues")) {
            queue = getCachedQueue(user, request);
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

    	return actionMap.get(action).doAction(user, request, response);
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
        } catch(Exception e) {
            logger.error("event=exception_while_shutting_down_executors", e);
        }
    }
}
