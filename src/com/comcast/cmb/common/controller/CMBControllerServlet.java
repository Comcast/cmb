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
import java.util.concurrent.ScheduledThreadPoolExecutor;

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
    public final static String VERSION = "2.2.13";

    @Override    
    public void init() throws ServletException {
        try {
            if (!initialized) {
	        	Util.initLog4j();
	            CMBProperties.getInstance();
	            workerPool = new ScheduledThreadPoolExecutor(CMBProperties.getInstance().getNumDeliveryHandlers());
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
    abstract protected boolean handleAction(String action, User user, HttpServletRequest request, HttpServletResponse response) throws Exception;
    
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
    
    private void handleRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {  
    	
        User user = null;
        String action = request.getParameter("Action");
        long ts1 = System.currentTimeMillis();
        
        try {
        	
            valueAccumulator.initializeAllCounters();
            
        	requestInit();
        	
            response.setContentType("text/xml");
        	
            if (!isValidAction(action)) {
                throw new CMBException(CMBErrorCodes.InvalidAction, action + " is not a valid action");
            }

            if (isAuthenticationRequired(action)) {
                user = authModule.authenticateByRequest(request);
            } else {
                user = null;
            }
            
            long ts3 = System.currentTimeMillis();

            valueAccumulator.addToCounter(AccumulatorName.CMBControllerPreHandleAction, (ts3 - ts1));

            boolean actionPerformed = handleAction(action, user, request, response);
            response.setStatus(200);
            long ts2 = System.currentTimeMillis();
            
            if (action != null && action.equals("ReceiveMessage") && !actionPerformed && user != null && user.getUserName().equals(CMBProperties.getInstance().getCnsUserName())) {

            	// Return code for ReceiveMessage() is number of messages received. If it is zero, do not write a log line to dial 
            	// down logging for CNS polling of producer and consumer queues.
            	
            } else if (action != null && action.equals("GetQueueUrl") && user != null && user.getUserName().equals(CMBProperties.getInstance().getCnsUserName())) {
            	
            	// Do not log GetQueueUrl if cns internal user calls it to dial down logging for CNS polling of producer and consumer queues.
            
            } else {
            
	            logger.info(
	            		
	            		"event=handleRequest status=success client_ip=" + request.getRemoteAddr() + " action=" + action + " responseTimeMS=" + (ts2-ts1) + 
	                    (request.getParameter("TopicArn") != null ? " topic_arn=" + request.getParameter("TopicArn") : "") +
	                    (request.getParameter("SubscriptionArn") != null ? " subscription_arn=" + request.getParameter("SubscriptionArn") : "") +
	                    (request.getParameter("Label") != null ? " label=" + request.getParameter("Label") : "") +
	                    (request.getParameter("NextToken") != null ? " next_token=" + request.getParameter("NextToken") : "") +
	                    (request.getParameter("Name") != null ? " name=" + request.getParameter("Name") : "") +
	                    (request.getParameter("Token") != null ? " token=" + request.getParameter("Token") : "") +
	                    (request.getParameter("Endpoint") != null ? " endpoint=" + request.getParameter("Endpoint") : "") +
	                    (request.getParameter("Protocol") != null ? " protocol=" + request.getParameter("Protocol") : "") +
	                    (request.getParameter("ReceiptHandle") != null ? " receipt_handle=" + request.getParameter("ReceiptHandle") : "") +
	                    (request.getParameter("VisibilityTimeout") != null ? " visibility_timeout=" + request.getParameter("VisibilityTimeout") : "") +
	                    (request.getParameter("QueueName") != null ? " queue_name=" + request.getParameter("QueueName") : "") +
	                    (request.getParameter("QueueNamePrefix") != null ? " queue_name_prefix=" + request.getParameter("QueueNamePrefix") : "") +
	                    (request.getParameter("DelaySeconds") != null ? " delay_seconds=" + request.getParameter("DelaySeconds") : "") +
	                    ((this instanceof CQSControllerServlet) ? (" queue_url=" + request.getRequestURL()) : "") +
	                    
	            		" CassandraTimeMS=" + valueAccumulator.getCounter(AccumulatorName.CassandraTime) + 
	                    " CassandraReadNum=" + valueAccumulator.getCounter(AccumulatorName.CassandraRead) + 
	                    " CassandraWriteNum=" + valueAccumulator.getCounter(AccumulatorName.CassandraWrite) +
	                    
	                    ((this instanceof CNSControllerServlet) ? (" CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime)) : "") +
	                    ((this instanceof CQSControllerServlet) ? (" RedisTimeMS=" + valueAccumulator.getCounter(AccumulatorName.RedisTime)) : "") +
	                    
	                    (user != null ? " user_name=" + user.getUserName() : "")
	                    
	            ); 
	                    
            }
       
        } catch (Exception ex) {
            
        	long ts2 = System.currentTimeMillis();
        	
            logger.error(
            		
            		"event=handleRequest status=failed client_ip=" + request.getRemoteAddr() + " action=" + action + " responseTimeMS=" + (ts2-ts1) + 
                    (request.getParameter("TopicArn") != null ? " topic_arn=" + request.getParameter("TopicArn") : "") +
                    (request.getParameter("SubscriptionArn") != null ? " subscription_arn=" + request.getParameter("SubscriptionArn") : "") +
                    (request.getParameter("Label") != null ? " label=" + request.getParameter("Label") : "") +
                    (request.getParameter("NextToken") != null ? " next_token=" + request.getParameter("NextToken") : "") +
                    (request.getParameter("Name") != null ? " name=" + request.getParameter("Name") : "") +
                    (request.getParameter("Token") != null ? " token=" + request.getParameter("Token") : "") +
                    (request.getParameter("Endpoint") != null ? " endpoint=" + request.getParameter("Endpoint") : "") +
                    (request.getParameter("Protocol") != null ? " protocol=" + request.getParameter("Protocol") : "") +
                    (request.getParameter("ReceiptHandle") != null ? " receipt_handle=" + request.getParameter("ReceiptHandle") : "") +
                    (request.getParameter("VisibilityTimeout") != null ? " visibility_timeout=" + request.getParameter("VisibilityTimeout") : "") +
                    (request.getParameter("QueueName") != null ? " queue_name=" + request.getParameter("QueueName") : "") +
                    (request.getParameter("QueueNamePrefix") != null ? " queue_name_prefix=" + request.getParameter("QueueNamePrefix") : "") +
                    (request.getParameter("DelaySeconds") != null ? " delay_seconds=" + request.getParameter("DelaySeconds") : "") +
                    ((this instanceof CQSControllerServlet) ? (" queue_url=" + request.getRequestURL()) : "") +
                    
            		" CassandraTimeMS=" + valueAccumulator.getCounter(AccumulatorName.CassandraTime) + 
                    " CassandraReadNum=" + valueAccumulator.getCounter(AccumulatorName.CassandraRead) + 
                    " CassandraWriteNum=" + valueAccumulator.getCounter(AccumulatorName.CassandraWrite) +
                    
                    ((this instanceof CNSControllerServlet) ? (" CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime)) : "") +
                    ((this instanceof CQSControllerServlet) ? (" RedisTimeMS=" + valueAccumulator.getCounter(AccumulatorName.RedisTime)) : "") +
                    
                    (user != null ? " user_name=" + user.getUserName() : ""), ex
                    
            ); 

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

    	// create the async context, otherwise getAsyncContext() will be null

    	final AsyncContext asyncContext = request.startAsync(new CQSHttpServletRequest(request), response);
    	
    	if (asyncContext == null) {

    		// this should only be happening for certain unit tests
    		
    		handleRequest(request, response);
    		
    	} else {

	    	// spawn task in background thread
	    	
	    	workerPool.submit(new Runnable() {
	    	
	    		public void run() {
	
	    			try {
	    				
	    				ReceiptModule.init();
	
	    				if (asyncContext.getRequest() instanceof CQSHttpServletRequest && asyncContext.getResponse() instanceof HttpServletResponse) {
	    				
	    					CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
	    					HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
	    					
	    					handleRequest(request, response);
	    				}
	
	    			} catch (Exception ex) {
	    				logger.error("event=failure", ex);
	    			}
	
	    			if (!((CQSHttpServletRequest)asyncContext.getRequest()).isQueuedForProcessing()) {
	    				asyncContext.complete();
	    			}
	    		}
	    	});
    	}
    }

    public static String createErrorResponse(String code, String errorMsg) {
        StringBuffer message = new StringBuffer("<ErrorResponse>")
                .append("<Error>")
                .append("<Type>Sender</Type>")
                .append("<Code>").append(code).append("</Code>")
                .append("<Message>").append(errorMsg).append("</Message>")
                .append("<Detail/>")
                .append("</Error>")
                .append("<RequestId>").append(ReceiptModule.getReceiptId()).append("</RequestId>")
                .append("</ErrorResponse>");
        return message.toString();
    }
}
