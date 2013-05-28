package com.comcast.cqs.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;

public class CQSScaleQueuesTest {

	private static Logger logger = Logger.getLogger(CQSScaleQueuesTest.class);

	private AmazonSQS sqs = null;

	private HashMap<String, String> attributeParams = new HashMap<String, String>();
	private User user = null;
	private Random randomGenerator = new Random();
	private final static String QUEUE_PREFIX = "TSTQ_"; 

	private Vector<String> report = new Vector<String>();

	public static void main(String [ ] args) throws Exception {

		System.out.println("CQSScaleQueuesTest V" + CMBControllerServlet.VERSION);

		long numQueuesPerThread = 10;
		long numMessagesPerQueue = 10;
		int numThreads = 10;
		int numShards = 100;

		for (String arg : args) {

			if (arg.startsWith("-nq")) {
				numQueuesPerThread = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-nm")) {
				numMessagesPerQueue = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-nt")) {
				numThreads = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-ns")) {
				numShards = Integer.parseInt(arg.substring(4));
			} else {
				System.out.println("Usage: CQSScaleQueuesTest -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties -nq=<number_queues_per_thread> -nm=<number_messages_per_queue> -nt=<number_threads> -ns=<number_shards>");
				System.out.println("Example: java CQSScaleQueuesTest -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties -nq=10 -nm=10 -nt=10 -ns=100");
				System.exit(1);
			}
		}

		System.out.println("Params for this test run:");
		System.out.println("Number of queues per thread: " + numQueuesPerThread);
		System.out.println("Number of messages per queue: " + numMessagesPerQueue);
		System.out.println("Number of threads: " + numThreads);
		System.out.println("Number of shards: " + numShards);

		CQSScaleQueuesTest cqsScaleTest = new CQSScaleQueuesTest();
		cqsScaleTest.setup();
		cqsScaleTest.CreateQueuesConcurrent(numQueuesPerThread, numMessagesPerQueue, numThreads, numShards);
	}

	@Before
	public void setup() throws Exception {

		Util.initLog4jTest();
		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		PersistenceFactory.reset();

		try {

			IUserPersistence userPersistence = new UserCassandraPersistence();

			user = userPersistence.getUserByName("cqs_unit_test");

			if (user == null) {
				user = userPersistence.createUser("cqs_unit_test", "cqs_unit_test");
			}

			BasicAWSCredentials credentialsUser = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());

			//BasicAWSCredentials credentialsUser = new BasicAWSCredentials("UK19XNIS0512THPIEDMW", "vaPmtt9YDjM9H2xJvkwbl6zNVgb2Xinc9+Afuivm");

			sqs = new AmazonSQSClient(credentialsUser);

			sqs.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());
			//sqs.setEndpoint("http://localhost:6059/");
			//sqs.setEndpoint("http://162.150.10.72:10159/");
			//sqs.setEndpoint("http://sdev44:6059/");
			//sqs.setEndpoint("http://ccpsvb-po-v603-p.po.ccp.cable.comcast.com:10159/");

		} catch (Exception ex) {
			logger.error("setup failed", ex);
			fail("setup failed: "+ex);
			return;
		}

		//attributeParams.put("MessageRetentionPeriod", "600");
		attributeParams.put("VisibilityTimeout", "30");
	}

	@Test
	public void Create1Queues() {
		CreateNQueues(1, true);
	}

	@Test
	public void Create10Queues() {
		CreateNQueues(10, true);
	}

	@Test
	public void Create10Queues10Shards() {
		CreateNQueues(10, 100, 10, true);
	}

	@Test
	public void Create10Queues1Shards() {
		CreateNQueues(10, 100, 1, true);
	}

	@Test
	public void Create100Queues() {
		CreateNQueues(100, true);
	}

	@Test
	public void Create1000Queues() {
		CreateNQueues(1000, true);
	}

	@Test
	public void Create10000Queues() {
		CreateNQueues(10000, true);
	}

	@Test
	public void Create100000Queues() {
		CreateNQueues(100000, true);
	}

	@Test
	public void CreateQueuesConcurrent() {
		CreateQueuesConcurrent(10, 1000, 100, 100);
	}
	
	private void CreateQueuesConcurrent(long numQueuesPerThread, long numMessagesPerQueue, int numThreads, int numShards) {

		ScheduledThreadPoolExecutor ep = new ScheduledThreadPoolExecutor(numThreads + 10);
		
		final long nqpt = numQueuesPerThread;
		final long nmpq = numMessagesPerQueue;
		final int ns = numShards;

		for (int i=0; i<numThreads; i++) {
			ep.submit((new Runnable() { public void run() { CreateNQueues(nqpt, nmpq, ns, false); }}));
		}

		logger.warn("ALL TEST LAUNCHED");

		try {
			ep.shutdown();
			ep.awaitTermination(60, TimeUnit.MINUTES);
		} catch (InterruptedException ex) {
			logger.error("fail", ex);
			fail(ex.getMessage());
		}

		logger.warn("ALL TEST FINISHED");

		for (String message : report) {
			logger.warn(message);
		}
	}

	private String receiveMessage(String queueUrl) {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();

		receiveMessageRequest.setQueueUrl(queueUrl);
		receiveMessageRequest.setMaxNumberOfMessages(1);

		// use this to test with long poll

		//receiveMessageRequest.setWaitTimeSeconds(1);

		ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);

		if (receiveMessageResult.getMessages().size() == 1) {

			String message = receiveMessageResult.getMessages().get(0).getBody();

			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
			sqs.deleteMessage(deleteMessageRequest);

			return message;
		}

		return null;
	}

	private void CreateNQueues(long numQueues, boolean logReport) {
		CreateNQueues(numQueues, 100, 100, logReport);
	}

	private void CreateNQueues(long numQueues, long numMessages, int numShards, boolean logReport) {

		long testStart = System.currentTimeMillis();
		
		List<String> queueUrls = new ArrayList<String>();
		Map<String, String> messageMap = new HashMap<String, String>();

		if (numShards != 100) {
			attributeParams.put("NumberOfPartitions", numShards + "");
		}

		try {

			long counter = 0;
			long createFailures = 0;
			long totalTime = 0;

			for (int i=0; i<numQueues; i++) {

				try {

					String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
					CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
					createQueueRequest.setAttributes(attributeParams);
					long start = System.currentTimeMillis();
					String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
					long end = System.currentTimeMillis();
					totalTime += end-start;
					logger.info("average creation millis: " + (totalTime/(i+1)) + " last: " + (end-start));
					queueUrls.add(queueUrl);
					logger.info("created queue " + counter + ": " + queueUrl);
					counter++;

				} catch (Exception ex) {
					logger.error("create failure", ex);
					createFailures++;
				}
			}

			Thread.sleep(1000);

			long sendFailures = 0;
			totalTime = 0;
			long totalCounter = 0;
			long messagesSent = 0;

			for (int i=0; i<numMessages; i++) {

				counter = 0;

				for (String queueUrl : queueUrls) {

					try {

						long start = System.currentTimeMillis();
						SendMessageResult result = sqs.sendMessage(new SendMessageRequest(queueUrl, "" + messagesSent));
						long end = System.currentTimeMillis();
						totalTime += end-start;
						logger.info("average send millis: " + (totalTime/(totalCounter+1)) + " last: " + (end-start));
						logger.info("sent message on queue " + i + " - " + counter + ": " + queueUrl);
						counter++;
						totalCounter++;
						messagesSent++;

						/*if (counter > queueUrls.size() / 2) {
			            	logger.info("halfway through");
			            }*/

					} catch (Exception ex) {
						logger.error("send failure", ex);
						sendFailures++;
					}
				}
			}

			Thread.sleep(1000);

			long readFailures = 0;
			long emptyResponses = 0;
			long messagesFound = 0;
			long outOfOrder = 0;
			long duplicates = 0;
			totalTime = 0;
			totalCounter = 0;

			for (int i=0; i<1.1*numMessages; i++) {

				counter = 0;

				String lastMessage = null;

				for (String queueUrl : queueUrls) {

					try {

						long start = System.currentTimeMillis();
						String messageBody = receiveMessage(queueUrl);
						long end = System.currentTimeMillis();
						totalTime += end-start;
						logger.info("average receive millis: " + (totalTime/(totalCounter+1)) + " last: " + (end-start));

						if (messageBody != null) {
							logger.info("received message on queue " + i + " - " + counter + " : " + queueUrl + " : " + messageBody);
							if (lastMessage != null && messageBody != null && Long.parseLong(lastMessage) > Long.parseLong(messageBody)) {
								outOfOrder++;
							}
							if (messageMap.containsKey(messageBody)) {
								duplicates++;
							} else {
								messageMap.put(messageBody, null);
							}
							messagesFound++;
							lastMessage = messageBody;
						} else {
							logger.info("no message found on queue " + i + " - " + counter + " : "  + queueUrl);
							emptyResponses++;
						}

						counter++;
						totalCounter++;

					} catch (Exception ex) {

						logger.error("read failure, will retry: " + queueUrl, ex);
						readFailures++;
					}
				}
			}

			Thread.sleep(1000);

			long deleteFailures = 0;
			counter = 0;
			totalTime = 0;

			for (String queueUrl : queueUrls) {
				try {
					long start = System.currentTimeMillis();
					sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
					long end = System.currentTimeMillis();
					totalTime += end-start;
					logger.info("average delete millis: " + (totalTime/(counter+1)) + " last: " + (end-start));
					logger.info("deleted queue " + counter + ": " + queueUrl);
					counter++;
				} catch (Exception ex) {
					logger.error("delete failure", ex);
					deleteFailures++;
				}
			}
			
			long testEnd = System.currentTimeMillis();

			String message = "duration sec: " + ((testEnd-testStart)/1000) + " create failuers: " + createFailures +  " delete failures: " + deleteFailures + " send failures: " + sendFailures + " read failures: " + readFailures + " empty reads: " + emptyResponses + " messages found: " + messagesFound + " messages sent: " + messagesSent + " out of order: " + outOfOrder + " duplicates: " + duplicates + " distinct messages: " + messageMap.size(); 

			report.add(new Date() + ": " + message);

			if (logReport) {
			
				logger.info(message);
	
				assertTrue("Create failures: " + createFailures, createFailures == 0);
				assertTrue("Delete failures: " + deleteFailures, deleteFailures == 0);
				assertTrue("Send failures: " + sendFailures, sendFailures == 0);
				assertTrue("Read failures: " + readFailures, readFailures == 0);
				//assertTrue("Empty reads: " + emptyResponses, emptyResponses == 0);
				assertTrue("Wrong number of messages found!", messagesFound == messagesSent);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		}
	}
}
