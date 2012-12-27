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
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
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

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;

/**
 * Following class uses the HttpClient library version 4.1.3 
 * @author aseem, bwolf
 */
public class HTTPEndpointSyncPublisher implements IEndpointPublisher {

	private final static SchemeRegistry schemeRegistry = new SchemeRegistry();
	private final static PoolingClientConnectionManager cm;
	private final static HttpClient httpClient;

	static {
		
		int timeoutMillis = CMBProperties.getInstance().getHttpTimeoutSeconds() * 1000;

		HttpParams params = new BasicHttpParams();
		HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1); 

		schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
		schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));

		HttpConnectionParams.setConnectionTimeout(params, timeoutMillis);
		HttpConnectionParams.setSoTimeout(params, timeoutMillis);

		cm = new PoolingClientConnectionManager(schemeRegistry);
		// increase max total connection to 250
		cm.setMaxTotal(CMBProperties.getInstance().getHttpPublisherEndpointConnectionPoolSize());
		// increase default max connection per route to 20
		cm.setDefaultMaxPerRoute(CMBProperties.getInstance().getHttpPublisherEndpointConnectionsPerRouteSize());

		httpClient = new DefaultHttpClient(cm, params);
	}

	private String endpoint;
	private String message;
	private User user;
	private static Logger logger = Logger.getLogger(HTTPEndpointSyncPublisher.class);

	public static int getNumConnectionsInPool() {
		return cm.getTotalStats().getAvailable();
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

		logger.debug("event=send_http_request endpoint=" + endpoint + "\" message=\"" + message + "\"");

		if ((message == null) || (endpoint == null)) {
			logger.debug("event=send_http_request status=failure errorType=MissingParameters endpoint=" + endpoint + "\" message=\"" + message + "\"");
			throw new Exception("Message and Endpoint must both be set");
		}

		HttpPost httpPost = new HttpPost(endpoint);
		StringEntity stringEntity = new StringEntity(message);
		httpPost.setEntity(stringEntity);

		HttpResponse response = httpClient.execute(httpPost);
		int statusCode = response.getStatusLine().getStatusCode();

		HttpEntity entity = response.getEntity();

		if (statusCode != 200 && statusCode != 201) {

			if (entity != null) {

				InputStream instream = entity.getContent();
				InputStreamReader responseReader = new InputStreamReader(instream);
				StringBuffer responseB = new StringBuffer();

				char []arr = new char[1024];
				int size = 0;

				while ((size = responseReader.read(arr, 0, arr.length)) != -1) {
					responseB.append(arr, 0, size);
				}      

				instream.close();
			}

			logger.debug("event=http_post_error endpoint=" + endpoint + " error_code=" + statusCode);
			throw new CMBException(new CMBErrorCodes(statusCode, "HttpError"), "Failed to deliver to " + endpoint + " with error code " + statusCode);

		} else {

			if (entity != null) {
				EntityUtils.consume(entity);
			}
		}
	}
}
