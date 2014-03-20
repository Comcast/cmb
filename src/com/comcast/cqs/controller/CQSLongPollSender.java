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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import com.comcast.cqs.model.CQSAPIStats;

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

import com.comcast.cmb.common.persistence.AbstractCassandraPersistence;
import com.comcast.cmb.common.persistence.AbstractCassandraPersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.AbstractCassandraPersistence.CmbRow;
import com.comcast.cmb.common.persistence.CassandraPersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;

public class CQSLongPollSender {
	
    private static Logger logger = Logger.getLogger(CQSLongPollSender.class);
    private static LongPollSenderThread senderThread;
    private static LongPollConnectionMaintainerThread connectionMaintainerThread;
    private static volatile LinkedBlockingQueue<String> pendingNotifications;
    private static volatile boolean initialized = false;
    private static volatile String localhost;
    
    public static final String CQS_API_SERVERS = "CQSAPIServers";
    public static final String KEYSPACE = CMBProperties.getInstance().getCQSKeyspace();
    
    // last minute long poll sender checked for api server heart beats
    
    public static volatile AtomicLong lastLPPingMinute = new AtomicLong(0);
    
    // list of recently active cqs api servers, could be reduced to only list those servers recently
    // doing long poll receives

    private static volatile ConcurrentHashMap<String, Channel> activeCQSApiServers;
    
    private static ClientBootstrap clientBootstrap;
    private static ChannelFactory clientSocketChannelFactory;

	private static class CQSLongPollClientHandler extends SimpleChannelHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			logger.error("event=longpoll_sender_error remote_address=" + e.getChannel().getRemoteAddress(), e.getCause());
			e.getChannel().close();
		}
	}

	public static void init() throws UnknownHostException {
		
		if (!initialized) {
	
	    	activeCQSApiServers = new ConcurrentHashMap<String, Channel>();
	
	    	pendingNotifications = new LinkedBlockingQueue<String>();
			
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
	
			senderThread = new LongPollSenderThread();
			senderThread.start();
	
			connectionMaintainerThread = new LongPollConnectionMaintainerThread();
			connectionMaintainerThread.start();
			
			localhost = InetAddress.getLocalHost().getHostAddress();
	
			initialized = true;

			logger.info("event=longpoll_sender_service_initialized");
		}
	}
	
	public static void shutdown() {

    	if (clientSocketChannelFactory != null) {
			clientSocketChannelFactory.releaseExternalResources();
		}
	}
	
	private static class LongPollConnectionMaintainerThread extends Thread {
		
	    private static Logger logger = Logger.getLogger(LongPollSenderThread.class);
	    
	    public LongPollConnectionMaintainerThread() {
	    }
	    
	    public void run() {
	    	
	    	while (true) {
	    		
	        	logger.info("event=reloading_active_cqs_api_server_list");
	        	
	        	try {

	        		long now = System.currentTimeMillis();

	                // read all other pings but ensure we are data-center local and looking at a cqs service
	        		
	        		AbstractCassandraPersistence cassandraHandler = CassandraPersistenceFactory.getInstance(CMBProperties.getInstance().getCQSKeyspace());
	                
	        		List<CmbRow<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows(KEYSPACE, CQS_API_SERVERS, null, 1000, 10, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
	        		
	        		Map<String, CQSAPIStats> cqsAPIServers = new HashMap<String, CQSAPIStats>();
	        		
	        		if (rows != null) {
	        			
	        			for (CmbRow<String, String, String> row : rows) {
	        				
	        				CQSAPIStats stats = new CQSAPIStats();
	        				
	        				String endpoint = row.getKey();
	        				
	    					stats.setIpAddress(endpoint);

	        				String dataCenter = CMBProperties.getInstance().getCMBDataCenter();
	        				long timestamp = 0;
	        				int longpollPort = 0;
	        				
	        				if (row.getColumnSlice().getColumnByName("timestamp") != null) {
	        					timestamp = (Long.parseLong(row.getColumnSlice().getColumnByName("timestamp").getValue()));
	        					stats.setTimestamp(timestamp);
	        				}
	        				
	        				if (row.getColumnSlice().getColumnByName("port") != null) {
	        					longpollPort = Integer.parseInt(row.getColumnSlice().getColumnByName("port").getValue());
	        					stats.setLongPollPort(longpollPort);
	        				}
	        				
	        				if (row.getColumnSlice().getColumnByName("dataCenter") != null) {
	        					dataCenter = row.getColumnSlice().getColumnByName("dataCenter").getValue();
	        					stats.setDataCenter(dataCenter);
	        				}
	        				
	        				if (now-timestamp < 5*60*1000 && dataCenter.equals(CMBProperties.getInstance().getCMBDataCenter()) && !endpoint.equals(localhost + ":" + (new URL(CMBProperties.getInstance().getCQSServiceUrl())).getPort())) {
	        					cqsAPIServers.put(endpoint.substring(0, endpoint.indexOf(":")) + ":" + longpollPort, stats);
		        				logger.info("event=found_active_cqs_endpoint endpoint=" + endpoint);
	        				}
	        			}
	        		}    
	        		
	        		// remove dead endpoints from list
	        		
	        		Iterator<String> iter = activeCQSApiServers.keySet().iterator();
	        		
	        		while (iter.hasNext()) {
	        			
	        			String endpoint = iter.next();
	        			
	        			if (!cqsAPIServers.containsKey(endpoint)) {
	        				activeCQSApiServers.remove(endpoint);
	        				logger.info("event=removed_dead_endpoint_from_list endpoint=" + endpoint);
	        			}
	        		}
	        		
	        		// establish or reestablish connections
	        		
					for (String endpoint : cqsAPIServers.keySet()) {
						
    					final String host = endpoint.substring(0, endpoint.indexOf(":"));
    					final int longpollPort = (int)cqsAPIServers.get(endpoint).getLongPollPort();
    					final String dataCenter = cqsAPIServers.get(endpoint).getDataCenter();
						final Channel oldClientChannel = activeCQSApiServers.get(endpoint);
    					
    					ChannelFuture channelFuture = clientBootstrap.connect(new InetSocketAddress(host, longpollPort));
    					
    					channelFuture.addListener(new ChannelFutureListener() {
    					
    						@Override
    						public void operationComplete(ChannelFuture cf) throws Exception {
    						
    							if (cf.isSuccess()) {
    							
    								final Channel newClientChannel = cf.getChannel();
    	        					activeCQSApiServers.put(host + ":" + longpollPort, newClientChannel);
    	        					
    	        					logger.info("event=established_new_connection host=" + host + " port=" + longpollPort + " data_center=" + dataCenter);

    	    						if (oldClientChannel != null && oldClientChannel.isConnected()) {
    	    						
    	    							oldClientChannel.close();
    	    							
    	    							logger.info("event=closing_old_connection endpoint=" + host + ":" + longpollPort);
    	    						}
    							}
    						}
    					});
					}
	                
	        	} catch (Exception ex) {
	        		logger.warn("event=ping_glitch", ex);
	        	}
	        	
	        	// sleep for 1 minute

	        	try { 
	    			Thread.sleep(60*1000); 
	    		} catch (InterruptedException ex) {	
	    			logger.error("event=thread_interrupted", ex);
	    		}
	    	}
	    }
	}
	
	private static class LongPollSenderThread extends Thread {
		
	    private static Logger logger = Logger.getLogger(LongPollSenderThread.class);
	    
	    public LongPollSenderThread() {
	    }
	    
		public void run() {
			
			while (true) {
				
				String queueArn = null;
				
				// blocking wait for next pending notification
				
				try {
					queueArn = pendingNotifications.take();
				} catch (InterruptedException ex) {
					logger.warn("event=taking_pending_notifcation_from_queue_failed");
				}
				
				if (queueArn == null) {
					
					try { 
						Thread.sleep(1000); 
					} catch (InterruptedException ex) { 
						logger.error("event=thread_interrupted", ex);
					}
					
					continue;
				}

				// don't go through tcp stack for loopback
				
				int messageCount = CQSLongPollReceiver.processNotification(queueArn, "localhost");
				logger.debug("event=longpoll_notification_sent endpoint=localhost queue_arn=" + queueArn + " num_msg_found=" + messageCount);
				
				if (messageCount > 0) {
					continue;
				}

				// if no messages found locally, send notification on all other established channels to remote cqs api servers
				
				for (String endpoint : activeCQSApiServers.keySet()) {
					
					Channel clientChannel = activeCQSApiServers.get(endpoint);
					
					if (clientChannel.isConnected() && clientChannel.isOpen() && clientChannel.isWritable()) {
						
						ChannelBuffer buf = ChannelBuffers.copiedBuffer(queueArn + ";", Charset.forName("UTF-8"));
						clientChannel.write(buf);
					
					} else {
						
						// if connection is dead attempt to reestablish
						
    					final String host = endpoint.substring(0, endpoint.indexOf(":"));
    					final int longpollPort = Integer.parseInt(endpoint.substring(endpoint.indexOf(":")+1));
    					
    					ChannelFuture channelFuture = clientBootstrap.connect(new InetSocketAddress(host, longpollPort));
    					
    					channelFuture.addListener(new ChannelFutureListener() {
    					
    						@Override
    						public void operationComplete(ChannelFuture cf) throws Exception {
    						
    							if (cf.isSuccess()) {
    							
    								final Channel newClientChannel = cf.getChannel();
    	        					activeCQSApiServers.put(host + ":" + longpollPort, newClientChannel);
    	        					logger.info("event=reestablished_bad_connection host=" + host + " port=" + longpollPort);
    	        					
    								ChannelBuffer buf = ChannelBuffers.copiedBuffer(newClientChannel + ";", Charset.forName("UTF-8"));
    								newClientChannel.write(buf);
    							}
    						}
    					});
					}

					logger.debug("event=longpoll_notification_sent endpoint=" + endpoint + " queue_arn=" + queueArn);
				}
			}
		}
	}

	public static void send(String queueArn) {
		
		if (!initialized) {
			return;
		}
		
		pendingNotifications.add(queueArn);
	}
}
