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
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract class for all servlets
 * @author aseem, bwolf, baosen, vvenkatraman, jorge, michael
 *
 */
abstract public class CMBControllerServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(CMBControllerServlet.class);

	public static volatile ScheduledThreadPoolExecutor workerPool; 

	private static volatile boolean initialized = false;

	protected IUserPersistence userHandler = null;
	protected IAuthModule authModule = null;

	/**
	 * This instance of the acccumulator is used throughout the code to accumulate response times
	 * for a particular thread 
	 */
	public final static ValueAccumulator valueAccumulator = new ValueAccumulator();

	public final static String VERSION = "2.2.45";
	
	public final static int HARD_TIMEOUT_SEC = CMBProperties.getInstance().getCMBRequestTimeoutSec();

	public volatile static ConcurrentHashMap<String, AtomicLong> callStats;
	public volatile static ConcurrentHashMap<String, AtomicLong> callFailureStats;

	public final static int NUM_MINUTES = 60;
	public final static int NUM_BUCKETS = 10;

	public volatile static AtomicLong[][] callResponseTimes1MS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
	public volatile static AtomicLong[][] callResponseTimes10MS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
	public volatile static AtomicLong[][] callResponseTimes100MS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
	public volatile static AtomicLong[][] callResponseTimes1000MS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
	public volatile static AtomicLong[][] callResponseTimesRedisMS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
	public volatile static AtomicLong[][] callResponseTimesCassandraMS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];

	public volatile static ConcurrentHashMap<String, AtomicLong[][]> callResponseTimesByApi;

	public volatile static AtomicInteger currentMinute;

	public volatile static String[] recentErrors = new String[10];
	public volatile static int recentErrorIdx = -1;

	public static final long startTime = System.currentTimeMillis();

	@Override    
	public void init() throws ServletException {

		try {

			if (!initialized) {

				Util.initLog4j();
				CMBProperties.getInstance();
				workerPool = new ScheduledThreadPoolExecutor(CMBProperties.getInstance().getCMBWorkerPoolSize());
				initStats();
				initialized = true;
			}

		} catch (Exception ex) {
			throw new ServletException(ex);
		}
	}

	protected void initRequest() {

		userHandler = PersistenceFactory.getUserPersistence();
		authModule = new UserAuthModule();
		authModule.setUserPersistence(userHandler);
	}

	public static void initStats() {

		callStats = new ConcurrentHashMap<String, AtomicLong>();
		callFailureStats = new ConcurrentHashMap<String, AtomicLong>();

		callResponseTimes1MS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
		callResponseTimes10MS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
		callResponseTimes100MS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
		callResponseTimes1000MS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
		callResponseTimesRedisMS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
		callResponseTimesCassandraMS = new AtomicLong[NUM_MINUTES][NUM_BUCKETS];
		callResponseTimesByApi = new ConcurrentHashMap<String, AtomicLong[][]>();

		for (int i=0; i<NUM_MINUTES; i++) {
			for (int k=0; k<NUM_BUCKETS; k++) {
				callResponseTimes1MS[i][k] = new AtomicLong();
				callResponseTimes10MS[i][k] = new AtomicLong();
				callResponseTimes100MS[i][k] = new AtomicLong();
				callResponseTimes1000MS[i][k] = new AtomicLong();
				callResponseTimesRedisMS[i][k] = new AtomicLong();
				callResponseTimesCassandraMS[i][k] = new AtomicLong();
			}
		}

		currentMinute = new AtomicInteger(new Date(System.currentTimeMillis()).getMinutes());

		recentErrors = new String[10];
		recentErrorIdx = -1;
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
		boolean logMessageBodyFlag = true;

		while (keys.hasMoreElements()) {

			String key = keys.nextElement();
			String value = request.getParameter(key);
			int length = 0;

			if (value != null) {
				length = value.length();
			}
			
			if (key.equals(CQSConstants.MESSAGE_BODY)) {
				if (CMBProperties.getInstance().getMaxMessagePayloadLogLength() > 0) {
					if (value != null && value.length() > CMBProperties.getInstance().getMaxMessagePayloadLogLength()) {
						value = value.substring(0, CMBProperties.getInstance().getMaxMessagePayloadLogLength())
								+ "...";
						logMessageBodyFlag = true;
					}
				} else {
					logMessageBodyFlag = false;
					value = null;
				}
			}
			
			if (value != null) {
				if (value.indexOf('\n') >= 0) {
					value = value.replace("\n", "\\n");
				}
				if (value.indexOf('\r') >= 0) {
					value = value.replace("\r", "\\r");
				}
			}

			if (!key.equals("MessageBody") || logMessageBodyFlag == true){
				params.append(key).append("=").append(value).append(" ");
			}
			if (key.equals("MessageBody")) {
				params.append("msg_size=").append(length).append(" ");
			}
		}

		return params.toString();
	}

	private String getUrlQueryString(AsyncContext asyncContext) {

		HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
		Enumeration<String> keys = request.getParameterNames();
		StringBuffer params = new StringBuffer("?");

		while (keys.hasMoreElements()) {

			String key = keys.nextElement();
			String value = request.getParameter(key);

			if (value != null && value.length() > CMBProperties.getInstance().getMaxMessagePayloadLogLength()) {
				value = value.substring(0, CMBProperties.getInstance().getMaxMessagePayloadLogLength()) + "...";
			}

			if (value != null) {
				if (value.indexOf('\n') >= 0) {
					value = value.replace("\n", "\\n");
				}
				if (value.indexOf('\r') >= 0) {
					value = value.replace("\r", "\\r");
				}
			}

			params.append(key).append("=").append(value).append("&");
		}

		return params.toString();
	}

	private void logStats(String action, long responseTimeMS, long redisTimeMS, long cassandraTimeMS) {
		
		try {
			
			if (!initialized) {
				return;
			}

			if (action != null && !action.equals("")) {

				callStats.putIfAbsent(action, new AtomicLong());

				if (callStats.get(action).incrementAndGet() == Long.MAX_VALUE - 100) {
					callStats = new ConcurrentHashMap<String, AtomicLong>();
				}

				int newMinute = new Date(System.currentTimeMillis()).getMinutes();
				int oldMinute = currentMinute.getAndSet(newMinute);

				if (newMinute != oldMinute) {
					int eraseIdx = (newMinute+1)%60;
					for (int i=0; i<NUM_BUCKETS; i++) {
						callResponseTimes1MS[eraseIdx][i].set(0);
						callResponseTimes10MS[eraseIdx][i].set(0);
						callResponseTimes100MS[eraseIdx][i].set(0);
						callResponseTimes1000MS[eraseIdx][i].set(0);
						callResponseTimesRedisMS[eraseIdx][i].set(0);
						callResponseTimesCassandraMS[eraseIdx][i].set(0);
						for (String ac : callResponseTimesByApi.keySet()) {
							callResponseTimesByApi.get(ac)[eraseIdx][i].set(0);
						}
					}
				}

				int responseTimeIdx;

				// response time percentiles

				responseTimeIdx = (int)(responseTimeMS)/1;

				if (responseTimeIdx > NUM_BUCKETS-1) {
					responseTimeIdx = NUM_BUCKETS-1;
				} else if (responseTimeIdx < 0) {
					responseTimeIdx = 0;
				}

				callResponseTimes1MS[newMinute][responseTimeIdx].incrementAndGet();

				responseTimeIdx = (int)(responseTimeMS)/10;

				if (responseTimeIdx > NUM_BUCKETS-1) {
					responseTimeIdx = NUM_BUCKETS-1;
				} else if (responseTimeIdx < 0) {
					responseTimeIdx = 0;
				}

				callResponseTimes10MS[newMinute][responseTimeIdx].incrementAndGet();

				responseTimeIdx = (int)(responseTimeMS)/100;

				if (responseTimeIdx > NUM_BUCKETS-1) {
					responseTimeIdx = NUM_BUCKETS-1;
				} else if (responseTimeIdx < 0) {
					responseTimeIdx = 0;
				}

				callResponseTimes100MS[newMinute][responseTimeIdx].incrementAndGet();

				responseTimeIdx = (int)(responseTimeMS)/1000;

				if (responseTimeIdx > NUM_BUCKETS-1) {
					responseTimeIdx = NUM_BUCKETS-1;
				} else if (responseTimeIdx < 0) {
					responseTimeIdx = 0;
				}

				callResponseTimes1000MS[newMinute][responseTimeIdx].incrementAndGet();

				AtomicLong[][] callResponseTimes = callResponseTimesByApi.get(action);

				if (callResponseTimes != null) {
				
					// resolution for the api specific response time array is always 10 ms
	
					responseTimeIdx = (int)(responseTimeMS)/10;
	
					if (responseTimeIdx > NUM_BUCKETS-1) {
						responseTimeIdx = NUM_BUCKETS-1;
					} else if (responseTimeIdx < 0) {
						responseTimeIdx = 0;
					}
					
					callResponseTimes[newMinute][responseTimeIdx].incrementAndGet();
				}

				// redis time

				responseTimeIdx = (int)(redisTimeMS)/1;

				if (responseTimeIdx > NUM_BUCKETS-1) {
					responseTimeIdx = NUM_BUCKETS-1;
				} else if (responseTimeIdx < 0) {
					responseTimeIdx = 0;
				}

				callResponseTimesRedisMS[newMinute][responseTimeIdx].incrementAndGet();

				// cassandra time

				responseTimeIdx = (int)(cassandraTimeMS)/1;

				if (responseTimeIdx > NUM_BUCKETS-1) {
					responseTimeIdx = NUM_BUCKETS-1;
				} else if (responseTimeIdx < 0) {
					responseTimeIdx = 0;
				}

				callResponseTimesCassandraMS[newMinute][responseTimeIdx].incrementAndGet();
			}
		} catch (Exception ex) {
			logger.warn("event=failed_to_log_stats", ex);
		}
	}

	private String getLogLine(AsyncContext asyncContext, CQSHttpServletRequest request, User user, long responseTimeMS, String status) {

		StringBuffer logLine = new StringBuffer("");

		logLine.append("event=req status="+status+" client=").append(request.getRemoteAddr());

		logLine.append(((this instanceof CQSControllerServlet) ? (" queue=" + request.getPathInfo()) : ""));

		if (request.getReceiptHandles() != null && request.getReceiptHandles().size() > 0) {
			for (int i=0; i<request.getReceiptHandles().size(); i++) {
				logLine.append(" ReceiptHandle=").append(request.getReceiptHandles().get(i));
			}
		}

		logLine.append(" ").append(getParameterString(asyncContext));
		logLine.append((user != null ? "user=" + user.getUserName() : ""));
		
		if (request.getAttribute("lp") == null) {
			//if status is timeout for normal action, it does not have below info
			if((status != null) && (!status.equals("timeout"))){
				logLine.append(" resp_ms=").append(responseTimeMS);
				logLine.append(" cass_ms=" + valueAccumulator.getCounter(AccumulatorName.CassandraTime));
				logLine.append(" cass_num_rd=" + valueAccumulator.getCounter(AccumulatorName.CassandraRead));
				logLine.append(" cass_num_wr=" + valueAccumulator.getCounter(AccumulatorName.CassandraWrite));
				logLine.append(((this instanceof CNSControllerServlet) ? (" cnscqs_ms=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime)) : ""));
				logLine.append(((this instanceof CQSControllerServlet) ? (" redis_ms=" + valueAccumulator.getCounter(AccumulatorName.RedisTime)) : ""));
				logLine.append(" io_ms=" + valueAccumulator.getCounter(AccumulatorName.IOTime));
				logLine.append(" asyncq_ms=" + valueAccumulator.getCounter(AccumulatorName.AsyncQueueTime));
				logLine.append(" auth_ms=" + valueAccumulator.getCounter(AccumulatorName.CMBControllerPreHandleAction));				
			} 
		} else if (request.getAttribute("lp").equals("yy")) {  // long poll receive with messages

			logLine.append(" resp_ms=").append(responseTimeMS);
			logLine.append(" cass_ms=" + request.getAttribute("cass_ms"));
			logLine.append(" cass_num_rd=" + request.getAttribute("cass_num_rd"));
			logLine.append(" cass_num_wr=" + request.getAttribute("cass_num_wr"));
			logLine.append(" redis_ms=" + request.getAttribute("redis_ms"));
			logLine.append(" io_ms=" + request.getAttribute("io_ms"));
			logLine.append(" lp_ms=").append(System.currentTimeMillis()-request.getRequestReceivedTimestamp());

		} else if (request.getAttribute("lp").equals("yn")) { // long poll receive without messages
			logLine.append(" lp_ms=").append(System.currentTimeMillis()-request.getRequestReceivedTimestamp());			
		}
		
		//logLine.append(" async_pool_queue=").append(CMBControllerServlet.workerPool.getQueue().size()).
		//append(" async_pool_size=").append(CMBControllerServlet.workerPool.getActiveCount());
		
		/*if (CMBProperties.getInstance().getCQSServiceEnabled()) {
			logLine.append(" cqs_pool_size=").append(CMB.cqsServer.getThreadPool().getThreads());
		}*/
		
		/*if (CMBProperties.getInstance().getCNSServiceEnabled()) {
			logLine.append(" cns_pool_size=").append(CMB.cnsServer.getThreadPool().getThreads());
		}*/
		
		// log external headers from proxy if present
		
		String rid = request.getHeader("CMB-RID");

		if (rid != null) {
			logLine.append(" rid=").append(rid);
		}
		
		String extStartTime = request.getHeader("START-TIME");
		
		if (extStartTime != null) {
			logLine.append(" ext_ts=").append(extStartTime);
		}

		return logLine.toString();
	}
	
	private String getFullRequestUrl(AsyncContext asyncContext) {
		HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
		return "event=raw_request url="  + request.getRequestURL() + "/" + this.getUrlQueryString(asyncContext);
	}

	private void handleRequest(AsyncContext asyncContext) throws ServletException, IOException {  

		CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
		HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

		User user = null;
		String action = request.getParameter("Action");
		long ts1 = System.currentTimeMillis();

		try {

			valueAccumulator.initializeAllCounters();
			
			valueAccumulator.addToCounter(AccumulatorName.AsyncQueueTime, System.currentTimeMillis()-request.getRequestReceivedTimestamp());

			initRequest();
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
			//if it is waiting for long poll receive, do not log now. Wait till asyn request finished.
			if(request.getAttribute("lp")==null){
				String logLine = getLogLine(asyncContext, request, user, ts2-ts1, "ok");
				logger.info(logLine);
			} 
			//String rawRequest = this.getFullRequestUrl(asyncContext);
			//logger.info(rawRequest);

			if (CMBProperties.getInstance().isCMBStatsEnabled()) {
				logStats(action, ts2-ts1, valueAccumulator.getCounter(AccumulatorName.RedisTime), valueAccumulator.getCounter(AccumulatorName.CassandraTime));
			}

		} catch (Exception ex) {

			long ts2 = System.currentTimeMillis();
			String logLine = getLogLine(asyncContext, request, user, ts2-ts1, "failed");
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
			Action.writeResponse(errXml, response);

			if (CMBProperties.getInstance().isCMBStatsEnabled() && action != null && !action.equals("")) {

				callFailureStats.putIfAbsent(action, new AtomicLong());

				if (callFailureStats.get(action).incrementAndGet() == Long.MAX_VALUE - 100) {
					callFailureStats = new ConcurrentHashMap<String, AtomicLong>();
				}

				StringBuffer errorDetail = new StringBuffer(new Date() + "|" + logLine + "|" + ex.getMessage() + "\n");

				for (StackTraceElement element : ex.getStackTrace()) {
					errorDetail.append(element).append("\n");
				}

				// not thread safe but that's ok here

				recentErrorIdx = (recentErrorIdx+1)%recentErrors.length;
				recentErrors[recentErrorIdx] = errorDetail.toString();
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
		
		HttpServletRequest wrappedRequest = new CQSHttpServletRequest(request);
		final AsyncContext asyncContext = request.startAsync(wrappedRequest, response);

		// jetty appears to require calling setTimeout on http handler thread so we are setting 
		// wait time seconds for long polling receive message here
		String actionParam = wrappedRequest.getParameter("Action");
		String waitTimeSecondsParam = wrappedRequest.getParameter(CQSConstants.WAIT_TIME_SECONDS);

		if (actionParam != null && actionParam.equals("ReceiveMessage") && waitTimeSecondsParam != null) {

			try {

				int waitTimeSeconds = Integer.parseInt(waitTimeSecondsParam);

				if (waitTimeSeconds >= 1 && waitTimeSeconds <= CMBProperties.getInstance().getCMBRequestTimeoutSec()) {
					asyncContext.setTimeout(waitTimeSeconds*1000);
					((CQSHttpServletRequest)asyncContext.getRequest()).setWaitTime(waitTimeSeconds*1000);
					logger.debug("event=set_message_timeout secs=" + waitTimeSeconds);
				}

			} catch (Exception ex) {
				logger.warn("event=ignoring_suspicious_wait_time_parameter value=" + waitTimeSecondsParam);
			}

		} else if (actionParam != null && actionParam.equals("ReceiveMessage")) {

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
				asyncContext.setTimeout(waitTimeSeconds*1000);
				((CQSHttpServletRequest)asyncContext.getRequest()).setWaitTime(waitTimeSeconds*1000);
				logger.debug("event=set_queue_timeout secs=" + waitTimeSeconds + " queue_url=" + queueUrl);
			} else {
				asyncContext.setTimeout(HARD_TIMEOUT_SEC*1000);
				logger.debug("event=set_default_timeout secs="+CMBProperties.getInstance().getCMBRequestTimeoutSec());
			}

		} else {
			asyncContext.setTimeout(HARD_TIMEOUT_SEC*1000);
			logger.debug("event=set_default_timeout secs=" + CMBProperties.getInstance().getCMBRequestTimeoutSec());
		}

		asyncContext.addListener(new AsyncListener() {

			@Override
			public void onComplete(AsyncEvent asyncEvent) throws IOException {

				AsyncContext asyncContext = asyncEvent.getAsyncContext(); 
				CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
				String action = request.getParameter("Action");
				
				if (action != null && action.equals("ReceiveMessage") && request.getAttribute("lp") != null) {
				
					User user = authModule.getUserByRequest(request);
					Object lp_ms = request.getAttribute("lp_ms");
					String logLine = null;
					
					if (lp_ms != null) {
						logLine = getLogLine(asyncContext, request, user, (Long)request.getAttribute("lp_ms"), "ok");
					} else {
						logLine = getLogLine(asyncContext, request, user, 0, "ok");
					}
					
					logger.info(logLine);
				}
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
				HttpServletResponse response = ((HttpServletResponse)asyncEvent.getSuppliedResponse());
				response.setStatus(httpCode);
				Action.writeResponse(errXml, response);

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
				
				if (!(asyncEvent.getSuppliedRequest() instanceof CQSHttpServletRequest)) {
					logger.error("event=invalid_request stage=on_timeout");
					return;
				} 
				
				//first check if it is long poll receive, if so, complete and return.
				AsyncContext asyncContext = asyncEvent.getAsyncContext(); 
				CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
				String action = request.getParameter("Action");

				if (action != null && action.equals("ReceiveMessage")) {
				
					String out = CQSMessagePopulator.getReceiveMessageResponseAfterSerializing(new ArrayList<CQSMessage>(), new ArrayList<String>(), new ArrayList<String>());
					asyncEvent.getSuppliedResponse().getWriter().println(out);
					CQSQueue queue = ((CQSHttpServletRequest)asyncEvent.getSuppliedRequest()).getQueue();
					asyncContext = asyncEvent.getAsyncContext(); 

					if (queue != null) {

						logger.debug("event=on_timeout queue_url=" + queue.getAbsoluteUrl());
						ConcurrentLinkedQueue<AsyncContext> queueContextsList = CQSLongPollReceiver.contextQueues.get(queue.getArn());

						if (queueContextsList != null && asyncContext != null) {
							queueContextsList.remove(asyncContext);
						}

					} else {
						logger.debug("event=on_timeout");
					}

					asyncContext.complete();
					return;						
				}				
				
				//for other timeout show log error and return error response.
				
				int httpCode = CMBErrorCodes.InternalError.getHttpCode();
				String code = CMBErrorCodes.InternalError.getCMBCode();
				String message = "CMB timeout after "+CMBProperties.getInstance().getCMBRequestTimeoutSec()+" seconds";

				if (asyncEvent.getThrowable() instanceof CMBException) {
					httpCode = ((CMBException)asyncEvent.getThrowable()).getHttpCode();
					code = ((CMBException)asyncEvent.getThrowable()).getCMBCode();
					message = asyncEvent.getThrowable().getMessage();
				}

				String errXml = CMBControllerServlet.createErrorResponse(code, message);
				HttpServletResponse response = ((HttpServletResponse)asyncEvent.getSuppliedResponse());
				response.setStatus(httpCode);
				Action.writeResponse(errXml, response);

				if (!(asyncEvent.getSuppliedRequest() instanceof CQSHttpServletRequest)) {
					logger.error("event=invalid_request stage=on_timeout");
					return;
				}    			

				CQSQueue queue = ((CQSHttpServletRequest)asyncEvent.getSuppliedRequest()).getQueue();
				asyncContext = asyncEvent.getAsyncContext(); 

				if (queue != null) {

					logger.error("event=on_timeout queue_url=" + queue.getAbsoluteUrl());
					ConcurrentLinkedQueue<AsyncContext> queueContextsList = CQSLongPollReceiver.contextQueues.get(queue.getArn());

					if (queueContextsList != null && asyncContext != null) {
						queueContextsList.remove(asyncContext);
					}

				} else {
					logger.error("event=on_timeout "+getLogLine(asyncContext, request, authModule.getUserByRequest(request), 0, "timeout"));
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
