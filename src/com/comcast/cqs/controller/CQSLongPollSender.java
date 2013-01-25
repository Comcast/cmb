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
package com.comcast.cqs.controller;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.beans.Row;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;

public class CQSLongPollSender {
	
    private static Logger logger = Logger.getLogger(CQSLongPollSender.class);
    private static ClientBootstrap clientBootstrap;
    private static ChannelFactory clientSocketChannelFactory;
	private static volatile ConcurrentHashMap<String, Long> activelyLongPollingCQSApiServers;
    private static volatile boolean initialized = false;
    public static volatile AtomicLong lastLPPingMinute = new AtomicLong(0);

	private static class CQSLongPollClientHandler extends SimpleChannelHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			logger.error("event=long_poll_sender_error msg=" + e);
			e.getChannel().close();
		}
	}

	public static void init() {
	
        activelyLongPollingCQSApiServers = new ConcurrentHashMap<String, Long>();
		
		// launch client side
		
		clientSocketChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),	Executors.newCachedThreadPool());
	
		clientBootstrap = new ClientBootstrap(clientSocketChannelFactory);
	
		clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(new CQSLongPollClientHandler());
			}
		});
	
		clientBootstrap.setOption("connectTimeoutMillis", 2000);
		clientBootstrap.setOption("tcpNoDelay", true);
		clientBootstrap.setOption("keepAlive", true);
		
		initialized = true;
	}
	
	public static void shutdown() {
		if (clientSocketChannelFactory != null) {
			clientSocketChannelFactory.releaseExternalResources();
		}
	}

	public static void send(String queueArn) {
		
		if (!initialized) {
			return;
		}
		
        long now = System.currentTimeMillis();
        
        if (lastLPPingMinute.getAndSet(now/(1000*60)) != now/(1000*60)) {

        	try {

        		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCMBCQSKeyspace());

                // read all other pings but ensure we are data-center local and lookign at a cqs service
                
        		List<Row<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows("CQSAPIServers", null, 1000, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), HConsistencyLevel.QUORUM);
        		
        		if (rows != null) {
        			
        			for (Row<String, String, String> row : rows) {
        				
        				String host = row.getKey();
        				String dataCenter = CMBProperties.getInstance().getCmbDataCenter();
        				long timestamp = 0, port = 0;
        				
        				if (row.getColumnSlice().getColumnByName("timestamp") != null) {
        					timestamp = (Long.parseLong(row.getColumnSlice().getColumnByName("timestamp").getValue()));
        				}
        				
        				if (row.getColumnSlice().getColumnByName("port") != null) {
        					port = Long.parseLong(row.getColumnSlice().getColumnByName("port").getValue());
        				}
        				
        				if (row.getColumnSlice().getColumnByName("dataCenter") != null) {
        					dataCenter = row.getColumnSlice().getColumnByName("dataCenter").getValue();
        				}
        				
        				if (now-timestamp < 5*60*1000 && dataCenter.equals(CMBProperties.getInstance().getCmbDataCenter())) {
        					activelyLongPollingCQSApiServers.put(host, port);
        					logger.info("event=found_active_api_server host=" + host + " port=" + port + " data_center=" + dataCenter);
        				}
        			}
        		}                
                
        	} catch (Exception ex) {
        		logger.warn("event=ping_glitch", ex);
        	}
        }
	
		final String msg = queueArn;

		for (String host : activelyLongPollingCQSApiServers.keySet()) {
		
			ChannelFuture channelFuture = clientBootstrap.connect(new InetSocketAddress(host, activelyLongPollingCQSApiServers.get(host).intValue()));
			
			final String h = host;
			final int p = activelyLongPollingCQSApiServers.get(host).intValue();
			
			channelFuture.addListener(new ChannelFutureListener() {
	
				@Override
				public void operationComplete(ChannelFuture cf) throws Exception {
					
					if (cf.isSuccess()) {
	
						final Channel clientChannel = cf.getChannel();
	
						logger.info("event=notification_sent host=" + h + " port=" + p + " notification=" + msg);
						
						if (clientChannel.isWritable()) {
							
							ChannelBuffer buf = ChannelBuffers.copiedBuffer(msg + ";", Charset.forName("UTF-8"));
							cf = clientChannel.write(buf);
							
							cf.addListener(new ChannelFutureListener() {
								
								@Override
								public void operationComplete(ChannelFuture cf) throws Exception {
									clientChannel.disconnect();
								}
							});
						}
					}
				}
			});
		}
	}
}
