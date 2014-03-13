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

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.log4j.Logger;


import com.comcast.cmb.common.util.CMBProperties;
import java.util.regex.*;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


import java.util.concurrent.ConcurrentHashMap;


/**
 * Endpoint publisher for Redis pub/sub endpoints
 * @author whatha200
 *
 */
public class RedisPubSubEndpointPublisher extends AbstractEndpointPublisher {
	protected static Logger logger = Logger.getLogger(RedisPubSubEndpointPublisher.class);
	private static Map<String,JedisPool> jedisPoolMap = new ConcurrentHashMap<String,JedisPool>();
	protected static int timeoutMS = CMBProperties.getInstance().getRedisPubSubEndpointConnectionTimeoutMS();
	
	@Override
	public void send() throws Exception {
		if (jedisPoolMap.containsKey(endpoint)) {
			long startTime = System.currentTimeMillis();
			JedisPool pool = jedisPoolMap.get(endpoint);
			Jedis connection = pool.getResource();
			String []tokens = endpoint.split("/");
			String channel = tokens[tokens.length-1];
			long numClients = connection.publish(channel, message.getMessage());
			long timeTaken = System.currentTimeMillis() - startTime;
			pool.returnResource(connection);
			logger.debug("event=send_redis endpoint="+ endpoint + " num_clients=" + numClients + " conn=reuse resp_ms=" + timeTaken);
			return;
		} else {
			long startTime = System.currentTimeMillis();
			Matcher m = com.comcast.cns.util.Util.redisPubSubPattern.matcher(endpoint);
			if (m.matches()) {
				String password = m.group(2);
				String host = m.group(3);
				String port = m.group(4);
				String channel = m.group(5);
				JedisPool pool = new JedisPool( new JedisPoolConfig(), host, Integer.parseInt(port), timeoutMS, password);
				Jedis connection = pool.getResource();
				long numClients = connection.publish(channel, message.getMessage());
				long timeTaken = System.currentTimeMillis() - startTime;
				logger.debug("event=send_redis endpoint=" + endpoint + " num_clients=" + numClients + " conn=new resp_ms=" + timeTaken);
				pool.returnResource(connection);
				jedisPoolMap.put(endpoint, pool);
				return;
			} else {
				logger.error("event=send_redis error=badEndpoint endpoint="+endpoint + " topicArn=" + message.getTopicArn());
				return;
			}
		} 
	}

}
