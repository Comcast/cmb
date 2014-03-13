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

package com.comcast.cns.io;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URL;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
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
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSMessage.CNSMessageStructure;
import com.comcast.cns.model.CNSMessage.CNSMessageType;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;

/**
 * Asynchronous HTTP/1.1 client.
 * 
 * This example demonstrates how HttpCore NIO can be used to execute multiple HTTP requests asynchronously using only one I/O thread.
 */

public class HTTPEndpointAsyncPublisher extends AbstractEndpointPublisher{

	private IPublisherCallback callback;

	private static HttpProcessor httpProcessor;
	private static HttpParams httpParams;
	private static BasicNIOConnPool connectionPool;

	private static Logger logger = Logger.getLogger(HTTPEndpointAsyncPublisher.class);

	static {

		try {

			httpParams = new SyncBasicHttpParams();
			httpParams
			.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, CMBProperties.getInstance().getCNSPublisherHttpTimeoutSeconds() * 1000)
			.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, CMBProperties.getInstance().getCNSPublisherHttpTimeoutSeconds() * 1000)
			.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
			.setParameter(CoreProtocolPNames.USER_AGENT, "CNS/" + CMBControllerServlet.VERSION);

			httpProcessor = new ImmutableHttpProcessor(new HttpRequestInterceptor[] {
					new RequestContent(),
					new RequestTargetHost(),
					new RequestConnControl(),
					new RequestUserAgent(),
					new RequestExpectContinue()});

			HttpAsyncRequestExecutor protocolHandler = new HttpAsyncRequestExecutor();
			final IOEventDispatch ioEventDispatch = new DefaultHttpClientIODispatch(protocolHandler, httpParams);
			final ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();

			connectionPool = new BasicNIOConnPool(ioReactor, httpParams);
			connectionPool.setDefaultMaxPerRoute(2); // maybe adjust pool size
			connectionPool.setMaxTotal(8);

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

	public HTTPEndpointAsyncPublisher(IPublisherCallback callback) {
		this.callback = callback;
	}

	@Override
	public void send() throws Exception {

		HttpAsyncRequester requester = new HttpAsyncRequester(httpProcessor, new DefaultConnectionReuseStrategy(), httpParams);
		final URL url = new URL(endpoint);
		final HttpHost target = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());

		BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest("POST", url.getPath() + (url.getQuery() == null ? "" : "?" + url.getQuery()));
		composeHeader(request);

		String msg = null;
		
		if (message.getMessageStructure() == CNSMessageStructure.json) {
			msg = message.getProtocolSpecificMessage(CnsSubscriptionProtocol.http);
		} else {
			msg = message.getMessage();
		}
		
		if (!rawMessageDelivery && message.getMessageType() == CNSMessageType.Notification) {
			msg = com.comcast.cns.util.Util.generateMessageJson(message, CnsSubscriptionProtocol.http);
		}
		
		logger.debug("event=send_async_http_request endpoint=" + endpoint + "\" message=\"" + msg + "\"");

		request.setEntity(new NStringEntity(msg));

		requester.execute(
				new BasicAsyncRequestProducer(target, request),
				new BasicAsyncResponseConsumer(),
				connectionPool,
				new BasicHttpContext(),
				new FutureCallback<HttpResponse>() {

					public void completed(final HttpResponse response) {

						int statusCode = response.getStatusLine().getStatusCode();

						// accept all 2xx status codes

						if (statusCode >= 200 && statusCode < 300) {
							callback.onSuccess();
						} else {
							logger.warn(target + "://" + url.getPath() + "?" + url.getQuery() + " -> " + response.getStatusLine());
							callback.onFailure(statusCode);
						}
					}

					public void failed(final Exception ex) {
						logger.warn(target + " " + url.getPath() + " " + url.getQuery(), ex);
						callback.onFailure(0);
					}

					public void cancelled() {
						logger.warn(target + " " + url.getPath() + " " + url.getQuery() + " -> " + "cancelled");
						callback.onFailure(1);
					}
				});
	}

	private void composeHeader(BasicHttpEntityEnclosingRequest request) {		

		request.setHeader("x-amz-sns-message-type", this.getMessageType());
		request.setHeader("x-amz-sns-message-id", this.getMessageId());
		request.setHeader("x-amz-sns-topic-arn", this.getTopicArn());
		request.setHeader("x-amz-sns-subscription-arn", this.getSubscriptionArn());
		request.setHeader("User-Agent", "Cloud Notification Service Agent");

		if (this.getRawMessageDelivery()) {
			request.addHeader("x-amz-raw-message", "true");
		}
	}
}
