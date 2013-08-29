package com.comcast.cqs.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;

public class CQSMessageVisibilityTest {
	
	private static Logger logger = Logger.getLogger(CQSScaleQueuesTest.class);

	private static AmazonSQS sqs = null;

	private HashMap<String, String> attributeParams = new HashMap<String, String>();
	private User user = null;
	private Random randomGenerator = new Random();
	private final static String QUEUE_PREFIX = "TSTQ_"; 
	
	private static String accessKey = null;
	private static String accessSecret = null;

	private static int messageLength = 0;
	private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static Random rand = new Random();
    
    private static String queueName = null;
    private static String queueUrl = null;
    
    private static AtomicLong totalMessagesFound = new AtomicLong(0);
    private static boolean deleteQueues = false;
    
    private static String message = null;
    
	private static ScheduledThreadPoolExecutor ep = new ScheduledThreadPoolExecutor(10);

	@Before
	public void setup() throws Exception {

		Util.initLog4jTest();
		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		PersistenceFactory.reset();
		
		try {
			
			BasicAWSCredentials credentialsUser = null;
			
			if (accessKey != null && accessSecret != null) {

				credentialsUser = new BasicAWSCredentials(accessKey, accessSecret);
			
			} else {

				IUserPersistence userPersistence = new UserCassandraPersistence();
	
				user = userPersistence.getUserByName("cqs_unit_test");
	
				if (user == null) {
					user = userPersistence.createUser("cqs_unit_test", "cqs_unit_test");
				}
	
				credentialsUser = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
			}

			sqs = new AmazonSQSClient(credentialsUser);

			sqs.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());

		} catch (Exception ex) {
			logger.error("setup failed", ex);
			fail("setup failed: "+ex);
			return;
		}
		
		message = generateRandomMessage(1024);
		
		queueName = QUEUE_PREFIX + randomGenerator.nextLong();
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);

		//attributeParams.put("MessageRetentionPeriod", "600");
		//attributeParams.put("VisibilityTimeout", "30");
        //createQueueRequest.setAttributes(attributeParams);
        
        queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        
        //Thread.sleep(2500);
	}
	
	@After
	public void Teardown() {
		if (queueUrl != null && deleteQueues) {
			DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
			deleteQueueRequest.setQueueUrl(queueUrl);
			sqs.deleteQueue(deleteQueueRequest);
		}
	}
	
	private String generateRandomMessage(int length) {
		StringBuilder sb = new StringBuilder(length);
		for (int i=0; i<length; i++) {
			sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
		}
		return sb.toString();
	}
	
	@Test
	public void messageVisibilityTest() {
		try {
			ep.submit(new MessageSender());
			//give initial thread time to launch more thread
			Thread.sleep(5*1000);
			ep.shutdown();
			ep.awaitTermination(60, TimeUnit.MINUTES);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		}
	}
	
	private static class MessageSender extends Thread {
		
		@Override
		public void run() {
			
			logger.info("starting sender");
			
			try {

	    		SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, message);
	    		sqs.sendMessage(sendMessageRequest);

	    		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				receiveMessageRequest.setWaitTimeSeconds(20);
				
				ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);

				if (receiveMessageResult.getMessages().size() == 1) {
					
					assertTrue("wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals(message));
					logger.info("message found for first time");
					
					ep.submit(new MessageReceiver(60));
					
					// push message out 11 sec
					
					logger.info("sleeping for 29 sec");
					Thread.sleep(29*1000);
					ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
					changeMessageVisibilityRequest.setVisibilityTimeout(11);
					changeMessageVisibilityRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
					changeMessageVisibilityRequest.setQueueUrl(queueUrl);
					sqs.changeMessageVisibility(changeMessageVisibilityRequest);
					logger.info("changed visibility timeout to 11 sec");
					
					// push message out 10 sec
					
					logger.info("sleeping for 10 sec");
					Thread.sleep(10*1000);
					changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
					changeMessageVisibilityRequest.setVisibilityTimeout(10);
					changeMessageVisibilityRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
					changeMessageVisibilityRequest.setQueueUrl(queueUrl);
					sqs.changeMessageVisibility(changeMessageVisibilityRequest);
					logger.info("changed visibility timeout to 10 sec");
					
					// push message out 11 sec
					
					logger.info("sleeping for 9 sec");
					Thread.sleep(9*1000);
					changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
					changeMessageVisibilityRequest.setVisibilityTimeout(11);
					changeMessageVisibilityRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
					changeMessageVisibilityRequest.setQueueUrl(queueUrl);
					sqs.changeMessageVisibility(changeMessageVisibilityRequest);
					logger.info("changed visibility timeout to 11 sec");
					
				} else {
					fail ("no message found");
				}
			
			} catch (Exception ex) {
				ex.printStackTrace();
				fail(ex.getMessage());
			}
		}
	}

	private static class MessageReceiver extends Thread {
		
		public int expectedDelaySeconds = 30;
		
		public MessageReceiver(int expectedDelaySeconds) {
			this.expectedDelaySeconds = expectedDelaySeconds;
		}
		
		@Override
		public void run() {
			
			logger.info("starting receiver");

			try {

				long start = System.currentTimeMillis();
				int waitTime = 1;
	            
	            while (true) {
	            
					logger.info("checking for messages");

					ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
					receiveMessageRequest.setQueueUrl(queueUrl);
					receiveMessageRequest.setMaxNumberOfMessages(1);
					receiveMessageRequest.setWaitTimeSeconds(waitTime);
					
					ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);

					if (receiveMessageResult.getMessages().size() == 1) {
						assertTrue("wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals(message));
						long durationSec = (System.currentTimeMillis() - start)/1000;
						logger.info("message found again after " + durationSec + " sec");
						DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
						deleteMessageRequest.setQueueUrl(queueUrl);
						deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
						sqs.deleteMessage(deleteMessageRequest);
						logger.info("deleted message");
						assertTrue("message became visible already after " + durationSec + " sec", durationSec >= expectedDelaySeconds);
						break;
					}
					
					// long polling calls will not react to messages becoming revisible, so we need to wait for the next call plus some grace period
					
					if (System.currentTimeMillis() - start > expectedDelaySeconds*1000 + 2*waitTime*1000) {
						fail("no message found in " + (expectedDelaySeconds+waitTime*2) + " sec, expected to find one in " + expectedDelaySeconds + " sec");
					}
					
					//Thread.sleep(500);
	            }
				
			} catch (Exception ex) {
				ex.printStackTrace();
				fail(ex.getMessage());
			}
		}
	}
}
