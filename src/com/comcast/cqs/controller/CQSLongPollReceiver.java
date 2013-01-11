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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.comcast.cmb.common.util.CMBProperties;

public class CQSLongPollReceiver {
	
    private static Logger logger = Logger.getLogger(CQSLongPollReceiver.class);
    
    private static ChannelFactory serverSocketChannelFactory;

    public static volatile ConcurrentHashMap<String,Object> queueMonitors; 

	//
	// long poll design:
	//
	// http request handlers are now asynchronous: they offload action execution to a separate pool of worker threads
	// there is one single long poll receiver thread per api server listening on a dedicated port for "message available" notifications using netty nio library (asynchronous i/o)
	// each long polling receive() api call does a wait(timeout) on a monitor (one monitor per queue)
	// when the long poll receiver thread receives a notification (which only consists of the queue arn that received a message) it will do a notify() on the monitor associated with the queue
	// this wakes up at most one waiting receive() api call, which will then try to read and return messages from redis/cassandra as usual
	// if no messages are found (race conditions etc.) and there is still long polling time left, receive will call wait() again
	// each send() api call will write the message to redis/cassandra as usual and send the target queue arn to all api servers using the netty nio library
	// each api server will write a heart beat (timestamp, ip, port) to cassandra, maybe once a minute or so
	// each api server will read the heart beat table once a minute or so to be aware of active api servers and their ip:port combinations
	//
	// known limitations:
	//
	// each long poll request occupies a waiting thread on the worker pool
	// no short cut if send and receive happens on same api server (could add that as performance improvement)
	// receive publishes to all api servers regardless of whether they are actually listening or not (to save management overhead)
	// long poll receiver thread is single point of failure on api server
	//
    
    //todo: sensible tcp settings for client and server
    //todo: spill over between dcs?
    //todo: multiple workers / api servers per host?
    //todo: migrate to a truly asynchronous implementation
	
	private static class LongPollServerHandler extends SimpleChannelHandler {

		StringBuffer queueArn = new StringBuffer("");
		
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			
			ChannelBuffer buf = (ChannelBuffer)e.getMessage();
			
			while (buf.readable()) {
				
				char c = ((char)buf.readByte());
				
				if (c == ';') {
					
					Object monitor = queueMonitors.get(queueArn.toString());
					logger.info("event=notification_received notification=" + queueArn + " source=" + e.getRemoteAddress());
					queueArn = new StringBuffer("");
					
					if (monitor != null) {
						synchronized (monitor) {
							logger.info("event=notifying_thread");
							monitor.notify();
						}
					}
					
				} else {
					queueArn.append(c);
				}
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			logger.error("event=long_poll_receiver_error msg=" + e);
			e.getChannel().close();
		}
	}

	public static void listen() {
		
        queueMonitors = new ConcurrentHashMap<String,Object>();

		serverSocketChannelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

		ServerBootstrap serverBootstrap = new ServerBootstrap(serverSocketChannelFactory);

		serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(new LongPollServerHandler());
			}
		});

		//todo: add other tcp options, e.g. timeout here

		serverBootstrap.setOption("child.tcpNoDelay", true);
		serverBootstrap.setOption("child.keepAlive", true);
		
		serverBootstrap.bind(new InetSocketAddress(CMBProperties.getInstance().getCqsLongPollPort()));

		logger.info("event=long_poll_server_listening port=" + CMBProperties.getInstance().getCqsLongPollPort());
	}
	
	public static void shutdown() {
		if (serverSocketChannelFactory != null) {
			serverSocketChannelFactory.releaseExternalResources();
		}
	}
}
