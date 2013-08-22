/** Copyright 2012 Comcast Corporation
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

import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBProperties;

/**
 * Class wraps synchronous endpoint publishers in asynchronous framework
 * @author bwolf
 *
 */
public class EndpointAsyncPublisherWrapper extends AbstractEndpointPublisher{
	
	private final IPublisherCallback callback;
    private final IEndpointPublisher publisher;
	
    private static volatile ScheduledThreadPoolExecutor deliveryHandlers = new ScheduledThreadPoolExecutor(CMBProperties.getInstance().getCNSNumPublisherDeliveryHandlers());

	public EndpointAsyncPublisherWrapper(IPublisherCallback callback, IEndpointPublisher publisher) {
		this.callback = callback;
		this.publisher = publisher;
	}

	@Override
	public void send() throws Exception {
		
		deliveryHandlers.submit(new Runnable() {
			public void run() {
				try {
					publisher.send();
					callback.onSuccess();
				} catch (Exception ex) {
					callback.onFailure(0);
				}
			}
		});
	}

	@Override
	public void setEndpoint(String endpoint) {
		publisher.setEndpoint(endpoint);
	}

	@Override
	public String getEndpoint() {
		return publisher.getEndpoint();
	}

	@Override
	public void setMessage(String message) {
		publisher.setMessage(message);
	}

	@Override
	public String getMessage() {
		return publisher.getMessage();
	}

	@Override
	public void setUser(User user) {
		publisher.setUser(user);
	}

	@Override
	public User getUser() {
		return publisher.getUser();
	}

	@Override
	public void setSubject(String subject) {
		publisher.setSubject(subject);
	}

	@Override
	public String getSubject() {
		return publisher.getSubject();
	}
}
