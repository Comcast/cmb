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

import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSMessage.CNSMessageStructure;
import com.comcast.cns.model.CNSMessage.CNSMessageType;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;

/**
 * Following class uses the HttpClient library version 4.2.1 
 * @author aseem, bwolf
 */
public class HTTPEndpointSyncPublisher extends AbstractEndpointPublisher {

	private final static SchemeRegistry schemeRegistry = new SchemeRegistry();
	private final static PoolingClientConnectionManager cm;
	private final static HttpClient httpClient;

	static {

		int timeoutMillis = CMBProperties.getInstance().getCNSPublisherHttpTimeoutSeconds() * 1000;

		HttpParams params = new BasicHttpParams();

		if (CMBProperties.getInstance().getCNSHttpProxy() != null && !CMBProperties.getInstance().getCNSHttpProxy().equals("")) {
			String p[] = CMBProperties.getInstance().getCNSHttpProxy().split(":");
			if (p.length == 2) {
				HttpHost proxy = new HttpHost(p[0], Integer.parseInt(p[1]), "http");
				params.setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
			}
		}

		HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1); 

		schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
		schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));

		HttpConnectionParams.setConnectionTimeout(params, timeoutMillis);
		HttpConnectionParams.setSoTimeout(params, timeoutMillis);

		cm = new PoolingClientConnectionManager(schemeRegistry);
		// increase max total connection to 250
		cm.setMaxTotal(CMBProperties.getInstance().getCNSPublisherHttpEndpointConnectionPoolSize());
		// increase default max connection per route to 20
		cm.setDefaultMaxPerRoute(CMBProperties.getInstance().getCNSPublisherHttpEndpointConnectionsPerRouteSize());

		httpClient = new DefaultHttpClient(cm, params);
	}

	private static Logger logger = Logger.getLogger(HTTPEndpointSyncPublisher.class);

	public static int getNumConnectionsInPool() {
		return cm.getTotalStats().getAvailable();
	}

	@Override
	public void send() throws Exception {

		if ((message == null) || (endpoint == null)) {
			logger.debug("event=send_http_request error_code=MissingParameters endpoint=" + endpoint + "\" message=\"" + message + "\"");
			throw new Exception("Message and Endpoint must both be set");
		}

		HttpPost httpPost = new HttpPost(endpoint);

		String msg = null;
		
		if (message.getMessageStructure() == CNSMessageStructure.json) {
			msg = message.getProtocolSpecificMessage(CnsSubscriptionProtocol.http);
		} else {
			msg = message.getMessage();
		}
		
		if (!rawMessageDelivery && message.getMessageType() == CNSMessageType.Notification) {
			msg = com.comcast.cns.util.Util.generateMessageJson(message, CnsSubscriptionProtocol.http);
		}
		
		logger.debug("event=send_sync_http_request endpoint=" + endpoint + "\" message=\"" + msg + "\"");

		StringEntity stringEntity = new StringEntity(msg);
		httpPost.setEntity(stringEntity);
		composeHeader(httpPost);

		HttpResponse response = httpClient.execute(httpPost);
		int statusCode = response.getStatusLine().getStatusCode();

		HttpEntity entity = response.getEntity();

		// accept all 2xx status codes

		if (statusCode > 200 || statusCode >= 300) {

			if (entity != null) {

				InputStream instream = entity.getContent();
				InputStreamReader responseReader = new InputStreamReader(instream);
				StringBuffer buffer = new StringBuffer();

				char []arr = new char[1024];
				int size = 0;

				while ((size = responseReader.read(arr, 0, arr.length)) != -1) {
					buffer.append(arr, 0, size);
				}      

				instream.close();
			}

			logger.debug("event=http_post_error endpoint=" + endpoint + " error_code=" + statusCode);
			throw new CMBException(new CMBErrorCodes(statusCode, "HttpError"), "Unreachable endpoint " + endpoint + " returns status code " + statusCode);

		} else {

			if (entity != null) {
				EntityUtils.consume(entity);
			}
		}
	}

	private void composeHeader(HttpRequestBase httpRequest) {		

		httpRequest.setHeader("x-amz-sns-message-type", this.getMessageType());
		httpRequest.setHeader("x-amz-sns-message-id", this.getMessageId());
		httpRequest.setHeader("x-amz-sns-topic-arn", this.getTopicArn());
		httpRequest.setHeader("x-amz-sns-subscription-arn", this.getSubscriptionArn());
		httpRequest.setHeader("User-Agent", "Cloud Notification Service Agent");

		if (this.getRawMessageDelivery()) {
			httpRequest.addHeader("x-amz-raw-message", "true");
		}
	}
}
