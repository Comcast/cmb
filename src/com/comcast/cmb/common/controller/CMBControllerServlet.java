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
import com.comcast.cqs.controller.CQSCache;
import com.comcast.cqs.controller.CQSControllerServlet;
import com.comcast.cqs.controller.CQSHttpServletRequest;
import com.comcast.cqs.controller.CQSLongPollReceiver;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;

import org.apache.log4j.Logger;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    
    public final static String VERSION = "2.2.25";

    public volatile static ConcurrentHashMap<String, AtomicLong> callStats;
    public volatile static ConcurrentHashMap<String, AtomicLong> callFailureStats;
    
    @Override    
    public void init() throws ServletException {
        
    	try {
            
    		if (!initialized) {
	        	
            	Util.initLog4j();
	            CMBProperties.getInstance();
	            workerPool = new ScheduledThreadPoolExecutor(CMBProperties.getInstance().getCNSNumPublisherDeliveryHandlers());
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
    
    private String getParameterString(AsyncContext asyncContext) {
    	
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
    	Enumeration<String> keys = request.getParameterNames();
    	StringBuffer params = new StringBuffer("");
    	
    	while (keys.hasMoreElements()) {
    		
    		String key = keys.nextElement();
    		String value = request.getParameter(key);
    		
    		if (value != null && value.length() > CMBProperties.getInstance().getCMBRequestParameterValueMaxLength()) {
    			value = value.substring(0, CMBProperties.getInstance().getCMBRequestParameterValueMaxLength()) + "...";
    		}
    		
    		if (value != null) {
    			value = value.replace("\n", "\\n").replace("\r", "\\r");
    		}
    		
    		params.append(key).append("=").append(value).append(" ");
    	}
    	
    	return params.toString();
    }
    
    private void handleRequest(AsyncContext asyncContext) throws ServletException, IOException {  
    	
        CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
    	
    	User user = null;
        String action = request.getParameter("Action");
        long ts1 = System.currentTimeMillis();
        
        try {
        	
            valueAccumulator.initializeAllCounters();
            
        	requestInit();
        	
            response.setContentType("text/xml");
            
            if (!isValidAction(action)) {
            	logger.warn("event=invalid_action action=" + action + " url=" + request.getRequestURL());
                String errXml = createErrorResponse(CMBErrorCodes.InvalidAction.getCMBCode(), action + " is not a valid action");
                response.setStatus(CMBErrorCodes.InvalidAction.getHttpCode());
                response.getWriter().println(errXml);
                return;
            }

            if (isAuthenticationRequired(action)) {
                user = authModule.authenticateByRequest(request);
            } else {
                user = null;
            }
            
            long ts3 = System.currentTimeMillis();

            valueAccumulator.addToCounter(AccumulatorName.CMBControllerPreHandleAction, (ts3 - ts1));

            handleAction(action, user, asyncContext);
            
            response.setStatus(200);
            long ts2 = System.currentTimeMillis();
            
    		StringBuffer logLine = new StringBuffer("");
    		
    		logLine.append("event=request status=success client=").append(request.getRemoteAddr());
    		
    		logLine.append(((this instanceof CQSControllerServlet) ? (" queue_url=" + request.getRequestURL()) : ""));
    		
    		if (request.getReceiptHandles() != null && request.getReceiptHandles().size() > 0) {
    			logLine.append(" receipt_handles=").append(request.getReceiptHandles().get(0));
    			for (int i=1; i<request.getReceiptHandles().size(); i++) {
    				logLine.append("," + request.getReceiptHandles().get(i));
    			}
    		}
    		
    		logLine.append(" ").append(getParameterString(asyncContext));
    		logLine.append((user != null ? "user=" + user.getUserName() : ""));

    		logLine.append(" resp_ms=").append((ts2-ts1));
    		logLine.append(" cass_ms=" + valueAccumulator.getCounter(AccumulatorName.CassandraTime));
    		logLine.append(" cass_num_rd=" + valueAccumulator.getCounter(AccumulatorName.CassandraRead));
    		logLine.append(" cass_num_wr=" + valueAccumulator.getCounter(AccumulatorName.CassandraWrite));
    		logLine.append(((this instanceof CNSControllerServlet) ? (" cnscqs_ms=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime)) : ""));
    		logLine.append(((this instanceof CQSControllerServlet) ? (" redis_ms=" + valueAccumulator.getCounter(AccumulatorName.RedisTime)) : ""));
    		
    		logger.info(logLine);
            
            if (action != null && !action.equals("")) {
	            
	            callStats.putIfAbsent(action, new AtomicLong());
	            
	            if (callStats.get(action).incrementAndGet() == Long.MAX_VALUE - 100) {
		            callStats = new ConcurrentHashMap<String, AtomicLong>();
	            }
            }
       
        } catch (Exception ex) {
            
        	long ts2 = System.currentTimeMillis();
        	
    		StringBuffer logLine = new StringBuffer("");
    		
    		logLine.append("event=request status=failed client=").append(request.getRemoteAddr());
    		
    		logLine.append(((this instanceof CQSControllerServlet) ? (" queue_url=" + request.getRequestURL()) : ""));
    		
    		if (request.getReceiptHandles() != null && request.getReceiptHandles().size() > 0) {
    			logLine.append(" receipt_handles=").append(request.getReceiptHandles().get(0));
    			for (int i=1; i<request.getReceiptHandles().size(); i++) {
    				logLine.append("," + request.getReceiptHandles().get(i));
    			}
    		}
    		
    		logLine.append(" ").append(getParameterString(asyncContext));
    		logLine.append((user != null ? "user=" + user.getUserName() : ""));

    		logLine.append(" resp_ms=").append((ts2-ts1));
    		logLine.append(" cass_ms=" + valueAccumulator.getCounter(AccumulatorName.CassandraTime));
    		logLine.append(" cass_num_rd=" + valueAccumulator.getCounter(AccumulatorName.CassandraRead));
    		logLine.append(" cass_num_wr=" + valueAccumulator.getCounter(AccumulatorName.CassandraWrite));
    		logLine.append(((this instanceof CNSControllerServlet) ? (" cnscqs_ms=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime)) : ""));
    		logLine.append(((this instanceof CQSControllerServlet) ? (" redis_ms=" + valueAccumulator.getCounter(AccumulatorName.RedisTime)) : ""));
    		
    		logger.error(logLine, ex);

            int httpCode = CMBErrorCodes.InternalError.getHttpCode();
            String code = CMBErrorCodes.InternalError.getCMBCode();
            String message = "There is an internal problem with CMB";
            
            if (ex instanceof CMBException) {
                httpCode = ((CMBException)ex).getHttpCode();
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
    	
    	// jetty appears to require calling setTimeout on http handler thread so we are setting 
    	// wait time seconds for long polling receive message here
    	
    	if (request.getParameter("Action") != null && request.getParameter("Action").equals("ReceiveMessage") && request.getParameter(CQSConstants.WAIT_TIME_SECONDS) != null) {

        	try {

        		int waitTimeSeconds = Integer.parseInt(request.getParameter(CQSConstants.WAIT_TIME_SECONDS));
	        	
        		if (waitTimeSeconds >= 1 && waitTimeSeconds <= 20) {
		        	asyncContext.setTimeout(waitTimeSeconds * 1000);
		            ((CQSHttpServletRequest)asyncContext.getRequest()).setWaitTime(waitTimeSeconds * 1000);
		        	logger.info("event=set_message_timeout secs=" + waitTimeSeconds);
	        	}
        		
        	} catch (Exception ex) {
        		logger.warn("event=ignoring_suspicious_wait_time_parameter value=" + request.getParameter(CQSConstants.WAIT_TIME_SECONDS));
        	}
        	
    	} else if (request.getParameter("Action") != null && request.getParameter("Action").equals("ReceiveMessage")) {
    		
    		String queueUrl = com.comcast.cqs.util.Util.getRelativeForAbsoluteQueueUrl(request.getRequestURL().toString());
    		CQSQueue queue;
    		int waitTimeSeconds = 0;
    		
			try {
				
				queue = CQSCache.getCachedQueue(queueUrl);

				if (queue != null && queue.getReceiveMessageWaitTimeSeconds() > 0) {
					waitTimeSeconds = queue.getReceiveMessageWaitTimeSeconds();
	    		}
				
			} catch (Exception ex) {
				logger.warn("event=lookup_queue queue_url=" + queueUrl, ex);
			}
    		
			if (waitTimeSeconds > 0) {
	        	asyncContext.setTimeout(waitTimeSeconds * 1000);
	            ((CQSHttpServletRequest)asyncContext.getRequest()).setWaitTime(waitTimeSeconds * 1000);
	        	logger.info("event=set_queue_timeout secs=" + waitTimeSeconds + " queue_url=" + queueUrl);
			} else {
	        	asyncContext.setTimeout(20 * 1000);
	        	logger.info("event=set_default_timeout secs=20");
			}
    		
    	} else {
        	asyncContext.setTimeout(20 * 1000);
        	logger.info("event=set_default_timeout secs=20");
        }
        
        asyncContext.addListener(new AsyncListener() {

    		@Override
			public void onComplete(AsyncEvent asyncEvent) throws IOException {
			}
			
    		@Override
			public void onError(AsyncEvent asyncEvent) throws IOException {

    			int httpCode = CMBErrorCodes.InternalError.getHttpCode();
                String code = CMBErrorCodes.InternalError.getCMBCode();
                String message = "There is an internal problem with CMB";
                
                if (asyncEvent.getThrowable() instanceof CMBException) {
                    httpCode = ((CMBException)asyncEvent.getThrowable()).getHttpCode();
                    code = ((CMBException)asyncEvent.getThrowable()).getCMBCode();
                    message = asyncEvent.getThrowable().getMessage();
                }

                String errXml = CMBControllerServlet.createErrorResponse(code, message);

                ((HttpServletResponse)asyncEvent.getSuppliedResponse()).setStatus(httpCode);
	            asyncEvent.getSuppliedResponse().getWriter().println(errXml);
	            
    			if (!(asyncEvent.getSuppliedRequest() instanceof CQSHttpServletRequest)) {
					logger.error("event=invalid_request stage=on_error");
					return;
    			}    			

    			CQSQueue queue = ((CQSHttpServletRequest)asyncEvent.getSuppliedRequest()).getQueue();
        		AsyncContext asyncContext = asyncEvent.getAsyncContext(); 

    			if (queue != null) {
	            	
        			logger.info("event=on_error queue_url=" + queue.getAbsoluteUrl());

        			ConcurrentLinkedQueue<AsyncContext> queueContextsList = CQSLongPollReceiver.contextQueues.get(queue.getArn());
	            	
            		if (queueContextsList != null && asyncContext != null) {
	            		queueContextsList.remove(asyncContext);
	            	}

    			} else {
    				logger.info("event=on_error");
    			}
	            
	            asyncContext.complete();
    		}
			
    		@Override
			public void onTimeout(AsyncEvent asyncEvent) throws IOException {
    			
    			String out = CQSMessagePopulator.getReceiveMessageResponseAfterSerializing(new ArrayList<CQSMessage>(), new ArrayList<String>());
	            asyncEvent.getSuppliedResponse().getWriter().println(out);

	            if (!(asyncEvent.getSuppliedRequest() instanceof CQSHttpServletRequest)) {
					logger.error("event=invalid_request stage=on_timeout");
					return;
    			}    			
	            
    			CQSQueue queue = ((CQSHttpServletRequest)asyncEvent.getSuppliedRequest()).getQueue();
    			
        		AsyncContext asyncContext = asyncEvent.getAsyncContext(); 

	            if (queue != null) {
	            	
	    			logger.info("event=on_timeout queue_url=" + queue.getAbsoluteUrl());
	    			
	            	ConcurrentLinkedQueue<AsyncContext> queueContextsList = CQSLongPollReceiver.contextQueues.get(queue.getArn());
	            	
            		if (queueContextsList != null && asyncContext != null) {
	            		queueContextsList.remove(asyncContext);
	            	}
            		
	            } else {
	    			logger.info("event=on_timeout");
	            }
				
	            asyncContext.complete();
			}

			@Override
			public void onStartAsync(AsyncEvent asyncEvent) throws IOException {
			}
    	});
    	
     	workerPool.submit(new Runnable() {

     		public void run() {

    	    	try {
    				ReceiptModule.init();
    				handleRequest(asyncContext);
    			} catch (Exception ex) {
    				logger.error("event=async_api_call_failure receipt_id=" + ReceiptModule.getReceiptId(), ex);
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
