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
package com.comcast.cmb.common.controller;

import com.comcast.cmb.common.model.IAuthModule;
import com.comcast.cmb.common.model.ReceiptModule;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.model.UserAuthModule;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.common.util.ValueAccumulator;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cns.controller.CNSControllerServlet;
import com.comcast.cqs.controller.CQSControllerServlet;
import com.comcast.cqs.controller.CQSHttpServletRequest;

import org.apache.log4j.Logger;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract class for all servlets
 * @author aseem, bwolf, baosen, vvenkatraman, jorge, michael
 *
 */
abstract public class CMBControllerServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    
    private static Logger logger = Logger.getLogger(CMBControllerServlet.class);
    
    private static volatile ScheduledThreadPoolExecutor workerPool; 
    
    private static volatile boolean initialized = false;
    
    protected IUserPersistence userHandler = null;
    protected IAuthModule authModule = null;
    
    /**
     * This instance of the acccumulator is used throughout the code to accumulate response times
     * for a particular thread 
     */
    public final static ValueAccumulator valueAccumulator = new ValueAccumulator();
    
    public final static String VERSION = "2.2.18";

    public volatile static ConcurrentHashMap<String, AtomicLong> callStats;
    public volatile static ConcurrentHashMap<String, AtomicLong> callFailureStats;
    
    @Override    
    public void init() throws ServletException {
        
    	try {
            
    		if (!initialized) {
	        	
            	Util.initLog4j();
	            CMBProperties.getInstance();
	            workerPool = new ScheduledThreadPoolExecutor(CMBProperties.getInstance().getNumDeliveryHandlers());
	            callStats = new ConcurrentHashMap<String, AtomicLong>();
	            callFailureStats = new ConcurrentHashMap<String, AtomicLong>();
	            initialized = true;
            }
    		
        } catch (Exception ex) {
        	throw new ServletException(ex);
        }
    }
    
    protected void requestInit() {
        userHandler = PersistenceFactory.getUserPersistence();
        authModule = new UserAuthModule();
        authModule.setUserPersistence(userHandler);
    }
    
    /**
     * handles the action
     * @param action
     * @param user the user making the call
     * @param request
     * @param response
     * @return true if this action was performed, false otherwise
     * @throws Exception
     */
    abstract protected boolean handleAction(String action, User user, AsyncContext asyncContext) throws Exception;
    
    /**
     * 
     * @param action
     * @return true if the action is supported by this servlet, false otherwise
     * @throws ServletException
     */
    abstract protected boolean isValidAction(String action) throws ServletException;

    /**
     * @param action
     * @return true if validation is required. False otherwise
     * Should be overriden by subclasses
     */
    protected boolean isAuthenticationRequired(String action) {
        return true;
    }
    
    private void handleRequest(AsyncContext asyncContext) throws ServletException, IOException {  
    	
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
    	
    	User user = null;
        String action = request.getParameter("Action");
        long ts1 = System.currentTimeMillis();
        
        try {
        	
            valueAccumulator.initializeAllCounters();
            
        	requestInit();
        	
            response.setContentType("text/xml");
            
            if (request.getRequestURL() == null) {
            	logger.error("event=bad_request_found");
            }
        	
            if (!isValidAction(action)) {
            	
            	Enumeration<String> keys = request.getParameterNames();
            	String params = "";
            	
            	while (keys.hasMoreElements()) {
            		String key = keys.nextElement();
            		params += " key=" + key + " value=" + request.getParameter(key);
            	}

            	logger.warn("event=found_invalid_action action=" + action + params);
                throw new CMBException(CMBErrorCodes.InvalidAction, action + " is not a valid action");
            }

            if (isAuthenticationRequired(action)) {
                user = authModule.authenticateByRequest(request);
            } else {
                user = null;
            }
            
            long ts3 = System.currentTimeMillis();

            valueAccumulator.addToCounter(AccumulatorName.CMBControllerPreHandleAction, (ts3 - ts1));

            boolean actionPerformed = handleAction(action, user, asyncContext);
            
            response.setStatus(200);
            long ts2 = System.currentTimeMillis();
            
            if (action != null && action.equals("ReceiveMessage") && !actionPerformed && user != null && user.getUserName().equals(CMBProperties.getInstance().getCnsUserName())) {

            	// Return code for ReceiveMessage() is number of messages received. If it is zero, do not write a log line to dial 
            	// down logging for CNS polling of producer and consumer queues.
            	
            } else {
            
        		StringBuffer logLine = new StringBuffer("");
        		
        		logLine.append("event=handleRequest status=success client_ip=").append(request.getRemoteAddr()).append(" action=").append(action).append(" responseTimeMS=").append((ts2-ts1)).
        		append((request.getParameter("TopicArn") != null ? " topic_arn=" + request.getParameter("TopicArn") : "")).
        		append((request.getParameter("SubscriptionArn") != null ? " subscription_arn=" + request.getParameter("SubscriptionArn") : "")).
        		append((request.getParameter("Label") != null ? " label=" + request.getParameter("Label") : "")).
        		append((request.getParameter("NextToken") != null ? " next_token=" + request.getParameter("NextToken") : "")).
        		append((request.getParameter("Name") != null ? " name=" + request.getParameter("Name") : "")).
        		append((request.getParameter("Token") != null ? " token=" + request.getParameter("Token") : "")).
        		append((request.getParameter("Endpoint") != null ? " endpoint=" + request.getParameter("Endpoint") : "")).
        		append((request.getParameter("Protocol") != null ? " protocol=" + request.getParameter("Protocol") : "")).
        		append((request.getParameter("ReceiptHandle") != null ? " receipt_handle=" + request.getParameter("ReceiptHandle") : "")).
        		append((request.getParameter("VisibilityTimeout") != null ? " visibility_timeout=" + request.getParameter("VisibilityTimeout") : "")).
        		append((request.getParameter("QueueName") != null ? " queue_name=" + request.getParameter("QueueName") : "")).
        		append((request.getParameter("QueueNamePrefix") != null ? " queue_name_prefix=" + request.getParameter("QueueNamePrefix") : "")).
        		append((request.getParameter("DelaySeconds") != null ? " delay_seconds=" + request.getParameter("DelaySeconds") : "")).
        		append(((this instanceof CQSControllerServlet) ? (" queue_url=" + request.getRequestURL()) : ""));
                
        		logLine.append(" CassandraTimeMS=" + valueAccumulator.getCounter(AccumulatorName.CassandraTime));
        		logLine.append(" CassandraReadNum=" + valueAccumulator.getCounter(AccumulatorName.CassandraRead));
        		logLine.append(" CassandraWriteNum=" + valueAccumulator.getCounter(AccumulatorName.CassandraWrite));
                
        		logLine.append(((this instanceof CNSControllerServlet) ? (" CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime)) : ""));
        		logLine.append(((this instanceof CQSControllerServlet) ? (" RedisTimeMS=" + valueAccumulator.getCounter(AccumulatorName.RedisTime)) : ""));
                
        		logLine.append((user != null ? " user_name=" + user.getUserName() : ""));
        		
        		logger.info(logLine);
	            
	            if (action != null && !action.equals("")) {
		            
		            callStats.putIfAbsent(action, new AtomicLong());
		            
		            if (callStats.get(action).incrementAndGet() == Long.MAX_VALUE - 100) {
			            callStats = new ConcurrentHashMap<String, AtomicLong>();
		            }
	            }
            }
       
        } catch (Exception ex) {
            
        	long ts2 = System.currentTimeMillis();
        	
    		StringBuffer logLine = new StringBuffer("");
    		
    		logLine.append("event=handleRequest status=failed client_ip=").append(request.getRemoteAddr()).append(" action=").append(action).append(" responseTimeMS=").append((ts2-ts1)).
    		append((request.getParameter("TopicArn") != null ? " topic_arn=" + request.getParameter("TopicArn") : "")).
    		append((request.getParameter("SubscriptionArn") != null ? " subscription_arn=" + request.getParameter("SubscriptionArn") : "")).
    		append((request.getParameter("Label") != null ? " label=" + request.getParameter("Label") : "")).
    		append((request.getParameter("NextToken") != null ? " next_token=" + request.getParameter("NextToken") : "")).
    		append((request.getParameter("Name") != null ? " name=" + request.getParameter("Name") : "")).
    		append((request.getParameter("Token") != null ? " token=" + request.getParameter("Token") : "")).
    		append((request.getParameter("Endpoint") != null ? " endpoint=" + request.getParameter("Endpoint") : "")).
    		append((request.getParameter("Protocol") != null ? " protocol=" + request.getParameter("Protocol") : "")).
    		append((request.getParameter("ReceiptHandle") != null ? " receipt_handle=" + request.getParameter("ReceiptHandle") : "")).
    		append((request.getParameter("VisibilityTimeout") != null ? " visibility_timeout=" + request.getParameter("VisibilityTimeout") : "")).
    		append((request.getParameter("QueueName") != null ? " queue_name=" + request.getParameter("QueueName") : "")).
    		append((request.getParameter("QueueNamePrefix") != null ? " queue_name_prefix=" + request.getParameter("QueueNamePrefix") : "")).
    		append((request.getParameter("DelaySeconds") != null ? " delay_seconds=" + request.getParameter("DelaySeconds") : "")).
    		append(((this instanceof CQSControllerServlet) ? (" queue_url=" + request.getRequestURL()) : ""));
            
    		logLine.append(" CassandraTimeMS=" + valueAccumulator.getCounter(AccumulatorName.CassandraTime));
    		logLine.append(" CassandraReadNum=" + valueAccumulator.getCounter(AccumulatorName.CassandraRead));
    		logLine.append(" CassandraWriteNum=" + valueAccumulator.getCounter(AccumulatorName.CassandraWrite));
            
    		logLine.append(((this instanceof CNSControllerServlet) ? (" CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime)) : ""));
    		logLine.append(((this instanceof CQSControllerServlet) ? (" RedisTimeMS=" + valueAccumulator.getCounter(AccumulatorName.RedisTime)) : ""));
            
    		logLine.append((user != null ? " user_name=" + user.getUserName() : ""));
    		
    		logger.error(logLine, ex);

            int httpCode = CMBErrorCodes.InternalError.getHttpCode();
            String code = CMBErrorCodes.InternalError.getCMBCode();
            String message = "There is an internal problem with CMB";
            
            if (ex instanceof CMBException) {
                httpCode = ((CMBException) ex).getHttpCode();
                code = ((CMBException) ex).getCMBCode();
                message = ex.getMessage();
            }

            String errXml = createErrorResponse(code, message);

            response.setStatus(httpCode);
            response.getWriter().println(errXml);
            
            if (action != null && !action.equals("")) {

            	callFailureStats.putIfAbsent(action, new AtomicLong());
            
	            if (callFailureStats.get(action).incrementAndGet() == Long.MAX_VALUE - 100) {
		            callFailureStats = new ConcurrentHashMap<String, AtomicLong>();
	            }
            }
            
        } finally {
            valueAccumulator.deleteAllCounters();
        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	doPost(request, response);
    }
    
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
    	if (!request.isAsyncSupported()) {
    		throw new ServletException("Servlet container does not support asynchronous calls");
    	}

    	final AsyncContext asyncContext = request.startAsync(new CQSHttpServletRequest(request), response);
    	
     	workerPool.submit(new Runnable() {

     		public void run() {

    	    	try {
    				ReceiptModule.init();
    				handleRequest(asyncContext);
    			} catch (Exception ex) {
    				logger.error("event=async_api_call_failure", ex);
    			}
    			
    	    	if (!((CQSHttpServletRequest)asyncContext.getRequest()).isQueuedForProcessing()) {
    	    		asyncContext.complete();
    	    	}
    	    }
     	});
    }
    
    public static String createErrorResponse(String code, String errorMsg) {
        StringBuffer message = new StringBuffer("<ErrorResponse>\n")
                .append("\t<Error>\n")
                .append("\t\t<Type>Sender</Type>\n")
                .append("\t\t<Code>").append(code).append("</Code>\n")
                .append("\t\t<Message>").append(errorMsg).append("</Message>\n")
                .append("\t\t<Detail/>\n")
                .append("\t</Error>\n")
                .append("\t<RequestId>").append(ReceiptModule.getReceiptId()).append("</RequestId>\n")
                .append("</ErrorResponse>\n");
        return message.toString();
    }
}
