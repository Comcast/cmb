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
package com.comcast.cns.test.stress;

import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;

public class CNSStressTest {
	
    private static Logger logger = Logger.getLogger(CNSStressTest.class);
    
    // test settings
    
	private static int NUM_TOPICS = 1;
	private static int NUM_SUBSCRIBERS_PER_TOPIC = 2;
	private static int NUM_MESSAGES_PER_SEC = 30;
	private static int TEST_DURATION_SECS = 20;
	private static int NUM_THREADS_PER_TOPIC = 30;
	
	private static boolean DELETE_TOPIC = false; 
	
	//TODO: provide list of endpoint settings
	
	// list of endpoints conforming with com.comcast.cmb.test.tools.EndpointServlet
	// (minimum one, if multiple subscriptions will be randomly distributed over endpoints)
	
	private static String endpointUrls[] = new String[]{
		CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/"
	};
	
	private static final String USER_NAME = "stressuser";
	private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final String TOPIC_NAME_PREFIX = "StressTestTopic";
	
	private AtomicInteger messageCount = new AtomicInteger(0);
	private AtomicInteger apiResponseTime = new AtomicInteger(0);
	private List<String> topics = new ArrayList<String>();
	private static final Random rand = new Random();
	
	private static AbstractDurablePersistence cassandraHandler = DurablePersistenceFactory.getInstance();
	
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();

    }
    
    //
    // Splunk Queries
    //
    // sourcetype=cmb action=Publish | chart avg(responseTimeMS) avg(CassandraTimeMS) avg(CNSCQSTimeMS)
    // sourcetype=cns host=ccpplb-dt-c301-i.dt.ccp.cable.comcast.com action=Publish | chart avg(responseTimeMS) p95(responseTimeMS) avg(CassandraTimeMS) p95(CassandraTimeMS) avg(CNSCQSTimeMS) p95(CNSCQSTimeMS)
    //
    // sourcetype=cnsWorker CNSEndpointPublisherJobProducer  event=processed_publish_job | chart p95(responseTimeMS) avg(responseTimeMS) avg(CassandraTime) avg(CNSCQSTimeMS)
    // sourcetype=cnsWorker source=*cnsworker.log  event=processed_publish_job | chart avg(responseTimeMS) p95(responseTimeMS) avg(CassandraTime) p95(CassandraTime) avg(CNSCQSTimeMS) p95(CNSCQSTimeMS)
    //
    // sourcetype=cnsWorker CNSEndpointPublisherJobConsumer  event=run_pass_done | chart p95(responseTimeMS) avg(responseTimeMS) avg(CNSCQSTimeMS)
    // sourcetype=cnsWorker source=*cnsworker.log  CNSEndpointPublisherJobConsumer  event=run_pass_done | chart avg(responseTimeMS) p95(responseTimeMS) avg(CassandraTime) p95(CassandraTime) avg(CNSCQSTimeMS) p95(CNSCQSTimeMS)
    //
    // sourcetype=cnsWorker CNSEndpointPublisherJobConsumer  event=notifying_subscriber | chart p95(responseTimeMS) avg(responseTimeMS) avg(CassandraTimeMS) avg(CNSCQSTimeMS) avg(publishTimeMS)
    // sourcetype=cnsWorker source=*cnsworker.log CNSEndpointPublisherJobConsumer  event=notifying_subscriber | chart avg(responseTimeMS) p95(responseTimeMS) avg(CassandraTimeMS) p95(CassandraTimeMS) avg(CNSCQSTimeMS) p95(CNSCQSTimeMS)
    //

	@Test	
	public void stressTestCMB() {
		
		String report = "";
		
		NUM_TOPICS = 1;
		NUM_SUBSCRIBERS_PER_TOPIC = 2;
		NUM_MESSAGES_PER_SEC = 10;
		TEST_DURATION_SECS = 20;
		NUM_THREADS_PER_TOPIC = 10;

		report += stressTest(true) + "\n";
		
		logger.warn(report);
	}

    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
	
	private String generateRandomMessage(int length) {

		StringBuilder sb = new StringBuilder(length);
		
		
		Date now = new Date();

		sb.append(now).append(";");
		sb.append(now.getTime()).append(";");

		sb.append(cassandraHandler.getUniqueTimeUUID(now.getTime())).append(";");
		
		try {
			sb.append(cassandraHandler.getTimeLong(now.getTime())).append(";");
		} catch (InterruptedException e1) {
		}
		
		try {
			sb.append(InetAddress.getLocalHost().getHostAddress()).append(";");
		} catch (UnknownHostException e) {
		}

		for (int i=sb.length(); i<length; i++) {
			sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
		}

		return sb.toString();
	}
	
    private class MessageSender implements Runnable {
    	
    	private AmazonSNSClient sns;
    	private String topicArn;
    	
    	public MessageSender(AmazonSNSClient sns, String topicArn) {
    		this.sns = sns;
    		this.topicArn = topicArn;
    	}

		@Override
		public void run() {

			PublishRequest publishRequest = new PublishRequest();
			
			publishRequest.setMessage(generateRandomMessage(2048));
			publishRequest.setSubject("stress test message");
			publishRequest.setTopicArn(topicArn);
			
			long now = System.currentTimeMillis();
			sns.publish(publishRequest);
			long later = System.currentTimeMillis();
			apiResponseTime.addAndGet((int)(later-now));
			
			int count = messageCount.addAndGet(1);
			
			if (count % 100 == 0) {
				logger.info("event=publish topic_arn=" + topicArn + " total_count=" + count);
			}
		}
    }
	
	private String stressTest(boolean useLocalSns) {
		
		String report = null;

		try {
			
			AWSCredentials awsCredentials = null;
			User user = null;
			
			if (useLocalSns) {
				
				IUserPersistence dao = new UserCassandraPersistence();
				
				try {
					user = dao.getUserByName(USER_NAME);
				} catch (Exception ex) {
					user =  dao.createUser(USER_NAME, USER_NAME);
				}
				
				if (user == null) { 
					user =  dao.createUser(USER_NAME, USER_NAME);
				}
				
				awsCredentials = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
			
			} else {
				awsCredentials = new BasicAWSCredentials(CMBProperties.getInstance().getAwsAccessKey(), CMBProperties.getInstance().getAwsAccessSecret());
			}
			
			ClientConfiguration clientConfiguration = new ClientConfiguration();

			AmazonSNSClient sns = new AmazonSNSClient(awsCredentials, clientConfiguration);

			if (useLocalSns) {
				sns.setEndpoint(CMBProperties.getInstance().getCNSServiceUrl());
			}
			
			for (int k=0; k<NUM_TOPICS; k++) {
			
				// set up topics
				
				CreateTopicRequest createTopicRequest = new CreateTopicRequest(TOPIC_NAME_PREFIX + k);
	
				CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
	
				String topicArn = createTopicResult.getTopicArn();
				
				topics.add(topicArn);
				
				// set up subscriptions
				
				for (int i=0; i<NUM_SUBSCRIBERS_PER_TOPIC;i++) {
	
					String subscriptionId = UUID.randomUUID().toString();
					
					/*String subscriptionUrl = null;
					
					int idx = rand.nextInt(100);

					if (idx < 50) {
						subscriptionUrl = endpointUrls[2] + subscriptionId;
					} else if (idx < 75) {
						subscriptionUrl = endpointUrls[1] + subscriptionId;
					} else {
						subscriptionUrl = endpointUrls[0] + subscriptionId;
					}*/
					
					String subscriptionUrl = endpointUrls[rand.nextInt(endpointUrls.length)] + subscriptionId;
					
					//subscriptionUrl += "?delayMS=10000";
					//subscriptionUrl += "?errorCode=404";
					
					SubscribeRequest subscribeRequest = new SubscribeRequest();
					subscribeRequest.setEndpoint(subscriptionUrl);
					subscribeRequest.setProtocol("http");
					subscribeRequest.setTopicArn(topicArn);
					
					sns.subscribe(subscribeRequest);
					
					//String subscriptionArn = subscribeResult.getSubscriptionArn();
				}

				List<CNSSubscription> confirmedSubscriptions = CNSTestingUtils.confirmPendingSubscriptionsByTopic(topicArn, user.getUserId(), CnsSubscriptionProtocol.http);
				
				for (CNSSubscription s : confirmedSubscriptions) {
					logger.info("event=subscribe endpoint=" +  s.getEndpoint() + " subscription_arn=" + s.getArn() + " topic_arn=" + s.getTopicArn());
				}
			}
			
			// publish messages
			
			ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(NUM_TOPICS*NUM_THREADS_PER_TOPIC);
    		
			for (String arn : topics) {
				for (int i=0; i<NUM_THREADS_PER_TOPIC; i ++) {
					scheduledExecutorService.scheduleWithFixedDelay(new MessageSender(sns, arn), rand.nextInt(100), 1000*NUM_THREADS_PER_TOPIC/NUM_MESSAGES_PER_SEC, TimeUnit.MILLISECONDS);
				}
			}
	    	
	    	Thread.sleep(TEST_DURATION_SECS*1000);

	    	scheduledExecutorService.shutdown();

			// unsubscribe

			/*UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest();
			unsubscribeRequest.setSubscriptionArn(subscriptionArn);
			
			sns.unsubscribe(unsubscribeRequest);*/
			
			// delete topics 
	    	
	    	if (DELETE_TOPIC) {
		    	
		    	Thread.sleep(10000);
	
				for (String arn : topics) {
				
					DeleteTopicRequest  deleteTopicRequest = new DeleteTopicRequest(arn);
					sns.deleteTopic(deleteTopicRequest);
					
					logger.info("event=delete_topic arn=" + arn);
				}			
	    	}

			report = "event=stress_test_complete messages_count_per_topic=" + (messageCount.get() / NUM_TOPICS) + " avg_api_response_time_millis=" + apiResponseTime.get()/messageCount.get(); 
				
	    	logger.warn(report);
			
		} catch (Exception ex) {
            logger.error("exception=" + ex, ex);
			fail("Test failed: " + ex.toString());
		}
		
		return report;
	}
	
	public static void main(String [ ] args) throws Exception {
		
		// NUM_TOPICS = 1;
		// NUM_SUBSCRIBERS_PER_TOPIC = 1;
		// NUM_MESSAGES_PER_SEC = 10;
		// TEST_DURATION_SECS = 2;
		// NUM_THREADS_PER_TOPIC = 2;

		System.out.println("CNSStressTest V" + CMBControllerServlet.VERSION);
		System.out.println("Usage: CNSStressTest -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties -nt=<number_topics> -ns=<number_subscribers_per_topic> -mps=<number_messages_per_sec> -td=<test_duration_secs> -tpt=<number_threads_per_topic> <endpoint_url>");
		System.out.println("Example: java CNSStressTest -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties -nt=1 -ns=1 -mps=10 -td=10 -tpt=10 <endpoint_url>");
		
		for (String arg : args) {
			
			if (arg.startsWith("-nt")) {
				NUM_TOPICS = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-ns")) {
				NUM_SUBSCRIBERS_PER_TOPIC = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-mps")) {
				NUM_MESSAGES_PER_SEC = Integer.parseInt(arg.substring(5));
			} else if (arg.startsWith("-td")) {
				TEST_DURATION_SECS = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-tpt")) {
				NUM_THREADS_PER_TOPIC = Integer.parseInt(arg.substring(5));
			} else if (arg.startsWith("-")) {
				System.out.println("Unknown option: " + arg);
				System.exit(1);
			} else {
				endpointUrls = arg.split(",");
			}
		}
		
		System.out.println("Params for this test run:");
		System.out.println("Number of topics: " + NUM_TOPICS);
		System.out.println("Number of subscribers per topic: " + NUM_SUBSCRIBERS_PER_TOPIC);
		System.out.println("Number of messages per second: " + NUM_MESSAGES_PER_SEC);
		System.out.println("Test duration seconds: " + TEST_DURATION_SECS);
		System.out.println("Number of threads per topic: " + NUM_THREADS_PER_TOPIC);
		System.out.println("Endpoint: " + endpointUrls);

		CNSStressTest cns = new CNSStressTest();
		cns.setup();
		cns.stressTest(true);
	}
}