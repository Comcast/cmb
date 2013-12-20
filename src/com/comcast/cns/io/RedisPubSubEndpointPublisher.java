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

import org.apache.log4j.Logger;


import com.comcast.cmb.common.util.CMBProperties;
import java.util.regex.*;
import redis.clients.jedis.Jedis;



/**
 * Endpoint publisher for Redis pub/sub endpoints
 * @author whatha200
 *
 */
public class RedisPubSubEndpointPublisher extends AbstractEndpointPublisher {
	protected static Logger logger = Logger.getLogger(RedisPubSubEndpointPublisher.class);

	
	@Override
	public void send() throws Exception {
		// TODO: Add a CNSRedisPubSubPublisherTimeoutSeconds property and getter
		int timeoutMS = CMBProperties.getInstance().getRedisPubSubEndpointConnectionTimeoutMS();

			
		Matcher m = com.comcast.cns.util.Util.redisPubSubPattern.matcher(endpoint);
		if (m.matches()) {
			String password = m.group(2);
			String host = m.group(3);
			String port = m.group(4);
			String channel = m.group(5);
			String msg = message.getMessage();
			long startTime = System.currentTimeMillis();
			Jedis jedis = new Jedis(host, Integer.parseInt(port),timeoutMS);
			if (password != null) {
				jedis.auth(password);
			}
			
			long clients = jedis.publish(channel, msg);
			long durationMS = System.currentTimeMillis() - startTime;
			logger.info("event=send_redis endpoint=" + host + ":" + port + "/" + channel + " num_clients=" + clients + " resp_ms=" + durationMS);
		} else {
			logger.info("event=send_redis error=badEndpoint endpoint="+endpoint + " topicArn=" + message.getTopicArn());
		}
	}
}
