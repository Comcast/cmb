package com.comcast.cns.test.unit;


import static org.junit.Assert.fail;

import org.apache.log4j.Logger;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class CNSRedisPubSubTest extends CMBAWSBaseTest {
	// logic largely taken from https://gist.github.com/FredrikWendt/3343861
	private static Random rand = new Random();

	private CountDownLatch messageReceivedLatch = new CountDownLatch(1);
	private CountDownLatch publishLatch = new CountDownLatch(1);
	private String redisHost = "127.0.0.1";
	private String redisPort = "6379";
	String redisChannel = "unittest1";
	List<String> messageContainer = new ArrayList<String>();
	String message = "redisUnitTest";
		
	
	public void setup() throws Exception {
		super.setup();
		logger = Logger.getLogger(CNSRedisPubSubTest.class);
	}
	
	//@Test
	public void testRedisPubSub() {
		JedisPubSub jedisPubSub = setupSubscriber();
		try {
			setupCNSMessagePublisher();
			logger.info("Finished starting setupCNSMessagePublisher");
			publishLatch.countDown();
			logger.info("Finished publish latch countdown");
			messageReceivedLatch.await();
			logger.info("Waiting for message receive");
		if (! messageContainer.iterator().next().equals(message)) {
			logger.info("published message did not match received message");
			fail("published message did not match received message");
		} else {
			logger.info("message received and matched published message");
		}
		jedisPubSub.unsubscribe();
		} catch (InterruptedException ie) {
			fail("interrupted " + ie);
		}
	}
	

	
	private void setupCNSMessagePublisher() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
				
					logger.info("setting up cnsMessagePublisher");
					String redisEndPointUrl = "redis://" + redisHost + ":" + redisPort + "/" + redisChannel;
					String topicArn = getTopic(1, USR.USER1);
					
					SubscribeRequest redisEndPointSubscribeRequest = new SubscribeRequest();
					redisEndPointSubscribeRequest.setEndpoint(redisEndPointUrl);
					redisEndPointSubscribeRequest.setProtocol("redis");
					redisEndPointSubscribeRequest.setTopicArn(topicArn);
					SubscribeResult subscribeResult = cns1.subscribe(redisEndPointSubscribeRequest);
					logger.info("subscribeResult=" + subscribeResult);
					
					PublishRequest publishRequest = new PublishRequest();
					publishRequest.setMessage(message);
					publishRequest.setTopicArn(topicArn);
					cns1.publish(publishRequest);
					logger.info("published message with contents=" + message);
				} catch (Exception e) {
					e.printStackTrace();
					fail("Caught error in setupCNSMessagePublisher");
					logger.error(e);
				}
			}
		}, "publisherThread").start();
	}
	
	private JedisPubSub setupSubscriber() {
		
		final JedisPubSub jedisPubSub = new JedisPubSub() {
			@Override
			public void onUnsubscribe(String channel, int subscribedChannels) {
				logger.info("onUnsubscribe channel=" + channel);
			}
 
			@Override
			public void onSubscribe(String channel, int subscribedChannels) {
				logger.info("onSubscribe channel=" + channel);
			}
 
			@Override
			public void onPUnsubscribe(String pattern, int subscribedChannels) {
			}
 
			@Override
			public void onPSubscribe(String pattern, int subscribedChannels) {
			}
 
			@Override
			public void onPMessage(String pattern, String channel, String message) {
			}
 
			@Override
			public void onMessage(String channel, String receivedMessage) {
				
				messageContainer.add(receivedMessage);
				logger.info("Redis message received = " + message);
				messageReceivedLatch.countDown();
			}
		};
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					logger.info("Connecting to redis host=" + redisHost);
					Jedis jedis = new Jedis(redisHost);

					logger.info("Subscribing to redis channel " + redisChannel);
					jedis.subscribe(jedisPubSub, redisChannel);
					logger.info("Subscribe returned, closing down");
					jedis.quit();
				} catch (Exception e) {
					logger.error("Redis subscription had an error " + e.getMessage());
					fail("redis subscribe hit an error " + e.getMessage());
				}
			}
		}, "subscriberThread").start();
		return jedisPubSub;
	}
}
