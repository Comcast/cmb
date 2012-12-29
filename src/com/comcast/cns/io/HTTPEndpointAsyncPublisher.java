package com.comcast.cns.io;

/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBProperties;

/**
 * Asynchronous HTTP/1.1 client.
 * 
 * This example demonstrates how HttpCore NIO can be used to execute multiple HTTP requests asynchronously using only one I/O thread.
 */

public class HTTPEndpointAsyncPublisher implements IEndpointPublisher {
	
	private String endpoint;
	private String message;
	private User user;
	private IHttpPostReactor postReactor;
	
	private static HttpProcessor httpProcessor;
	private static HttpParams httpParams;
	private static BasicNIOConnPool connectionPool;
	
	private static Logger logger = Logger.getLogger(HTTPEndpointAsyncPublisher.class);

	static {
		
		try {
			
	        // HTTP parameters for the client
	        httpParams = new SyncBasicHttpParams();
	        httpParams
	            .setIntParameter(CoreConnectionPNames.SO_TIMEOUT, CMBProperties.getInstance().getHttpTimeoutSeconds() * 1000)
	            .setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, CMBProperties.getInstance().getHttpTimeoutSeconds() * 1000)
	            .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
	            .setParameter(CoreProtocolPNames.USER_AGENT, "CNS/" + CMBControllerServlet.VERSION);
	        // Create HTTP protocol processing chain
	        httpProcessor = new ImmutableHttpProcessor(new HttpRequestInterceptor[] {
	                // Use standard client-side protocol interceptors
	                new RequestContent(),
	                new RequestTargetHost(),
	                new RequestConnControl(),
	                new RequestUserAgent(),
	                new RequestExpectContinue()});
	        // Create client-side HTTP protocol handler
	        HttpAsyncRequestExecutor protocolHandler = new HttpAsyncRequestExecutor();
	        // Create client-side I/O event dispatch
	        final IOEventDispatch ioEventDispatch = new DefaultHttpClientIODispatch(protocolHandler, httpParams);
	        // Create client-side I/O reactor
	        final ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
	        // Create HTTP connection pool
	        connectionPool = new BasicNIOConnPool(ioReactor, httpParams);
	        // Limit total number of connections to just two
	        connectionPool.setDefaultMaxPerRoute(2); // maybe adjust pool size
	        connectionPool.setMaxTotal(2);
	        
	        // Run the I/O reactor in a separate thread
	        
	        Thread t = new Thread(new Runnable() {
	
	            public void run() {
	                try {
	                    ioReactor.execute(ioEventDispatch);
	                } catch (InterruptedIOException ex) {
	        			logger.error("event=failed_to_initialize_async_http_client action=exiting", ex);
	                } catch (IOException ex) {
	        			logger.error("event=failed_to_initialize_async_http_client action=exiting", ex);
	                }
	            }
	
	        });
	        
	        t.start();
	        
		} catch (IOReactorException ex) {
			logger.error("event=failed_to_initialize_async_http_client action=exiting", ex);
		}
	}
	
	public HTTPEndpointAsyncPublisher(IHttpPostReactor postReactor) {
		this.postReactor = postReactor;
	}
	
	@Override
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public void setMessage(String message) {
		this.message = message;     
	}

	@Override
	public String getEndpoint() {
		return endpoint;
	}

	@Override
	public String getMessage() {        
		return message;
	}

	@Override
	public void setUser(User user) {
		this.user = user;       
	}

	@Override
	public User getUser() {
		return user;
	}

	@Override
	public void setSubject(String subject) {
	}

	@Override
	public String getSubject() {
		return null;
	}

	@Override
	public void send() throws Exception {
		
        HttpAsyncRequester requester = new HttpAsyncRequester(httpProcessor, new DefaultConnectionReuseStrategy(), httpParams);
        URL url = new URL(endpoint);
        final HttpHost target = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        
        BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest("POST", url.getPath());
        request.setEntity(new NStringEntity(message));
        
        requester.execute(
                new BasicAsyncRequestProducer(target, request),
                new BasicAsyncResponseConsumer(),
                connectionPool,
                new BasicHttpContext(),
                new FutureCallback<HttpResponse>() {

		            public void completed(final HttpResponse response) {
		                System.out.println(target + "->" + response.getStatusLine());
		                postReactor.onSuccess();
		            }
		
		            public void failed(final Exception ex) {
		                System.out.println(target + "->" + ex);
		                postReactor.onFailure(0);
		            }
		
		            public void cancelled() {
		                System.out.println(target + " cancelled");
		                postReactor.onFailure(1);
		            }
        });
    }
}
