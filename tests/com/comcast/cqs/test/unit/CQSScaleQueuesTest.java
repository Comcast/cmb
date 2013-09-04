package com.comcast.cqs.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
	private final static AtomicLong[] responseTimeDistribution100MS = new AtomicLong[100];
	private final static AtomicLong[] responseTimeDistribution10MS = new AtomicLong[10];
	
	private static String accessKey = null;
	private static String accessSecret = null;

	private Vector<String> report = new Vector<String>();
	
	private static boolean stopHealthCheck = false;
	
	private static int messageLength = 0;
	private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static Random rand = new Random();
    
    private static String queueName = null;
    
    private static AtomicLong totalMessagesFound = new AtomicLong(0);
    private static boolean deleteQueues = true;

	public static void main(String [ ] args) throws Exception {

		System.out.println("CQSScaleQueuesTest V" + CMBControllerServlet.VERSION);

		long numQueuesPerThread = 10;
		long numMessagesPerQueue = 10;
		int numThreads = 10;
		int numPartitions = 100;
		int numShards = 1;

		for (String arg : args) {

			if (arg.startsWith("-nq")) {
				numQueuesPerThread = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-nm")) {
				numMessagesPerQueue = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-nt")) {
				numThreads = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-np")) {
				numPartitions = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-ns")) {
				numShards = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-ml")) {
				messageLength = Integer.parseInt(arg.substring(4));
			} else if (arg.startsWith("-qn")) {
				queueName = arg.substring(4);
			} else if (arg.startsWith("-ak")) {
				accessKey = arg.substring(4);
			} else if (arg.startsWith("-as")) {
				accessSecret = arg.substring(4);
			} else if (arg.startsWith("-dq")) {
				deleteQueues = Boolean.parseBoolean(arg.substring(4));
			} else {
				System.out.println("Usage: CQSScaleQueuesTest -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties -nq=<number_queues_per_thread> -nm=<number_messages_per_queue> -nt=<number_threads> -np=<number_partitions> -ns=<number_shards> -ml=<message_length> -dq=<delete_queues_at_end_true_or_false> -qn=<queue_name_for_single_queue_tests> -ak=<access_key> -as=<access_secret>");
				System.out.println("Example: java CQSScaleQueuesTest -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties -nq=10 -nm=10 -nt=10 -np=100 -ns=1 -dq=true");
				System.exit(1);
			}
		}

		System.out.println("Params for this test run:");
		System.out.println("Number of queues per thread: " + numQueuesPerThread);
		System.out.println("Number of messages per queue: " + numMessagesPerQueue);
		System.out.println("Message length: " + messageLength);
		System.out.println("Number of threads: " + numThreads);
		System.out.println("Number of partitions: " + numPartitions);
		System.out.println("Number of shards: " + numShards);
		System.out.println("Delete queues: " + deleteQueues);
		
		if (queueName != null) {
			System.out.println("Queue name: " + queueName);
			System.out.println("Ignoring -nq setting above!");
			numQueuesPerThread = 1;
		}

		if (accessKey != null) {
			System.out.println("Access Key: " + accessKey);
		}
		
		if (accessSecret != null) {
			System.out.println("Access Secret: " + accessSecret);
		}

		CQSScaleQueuesTest cqsScaleTest = new CQSScaleQueuesTest();
		cqsScaleTest.setup();
		cqsScaleTest.CreateQueuesConcurrent(numQueuesPerThread, numMessagesPerQueue, numThreads, numPartitions, numShards);
	}

	@Before
	public void setup() throws Exception {

		Util.initLog4jTest();
		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		PersistenceFactory.reset();
		
		for (int i=0; i<responseTimeDistribution100MS.length; i++) {
			responseTimeDistribution100MS[i] = new AtomicLong(0);
		}

		for (int i=0; i<responseTimeDistribution10MS.length; i++) {
			responseTimeDistribution10MS[i] = new AtomicLong(0);
		}

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

		//attributeParams.put("MessageRetentionPeriod", "600");
		attributeParams.put("VisibilityTimeout", "30");
	}
	
	private String generateRandomMessage(int length) {
		StringBuilder sb = new StringBuilder(length);
		for (int i=0; i<length; i++) {
			sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
		}
		return sb.toString();
	}
	
	private void recordResponseTime(String api, long responseTimeMS) {
		
		// ignore api for now
		
		long rt100 = responseTimeMS/100;
		
		if (rt100<responseTimeDistribution100MS.length) {
			responseTimeDistribution100MS[(int)rt100].incrementAndGet();
		} else {
			logger.warn("event=RT_OFF_THE_CHART rt=" + responseTimeMS + " api=" + api);
		}
		
		long rt10 = responseTimeMS/10;
		
		if (rt10<responseTimeDistribution10MS.length) {
			responseTimeDistribution10MS[(int)rt10].incrementAndGet();
		}	
	}
	
	private void printResponseTimeDistribution() {
		long callCount = 0;
		logger.warn("RT100");
		for (int i=0; i<responseTimeDistribution100MS.length; i++) {
			logger.warn("RT=" + (i*100) + " CT=" + responseTimeDistribution100MS[i]);
			callCount += responseTimeDistribution100MS[i].longValue();
		}
		logger.warn("RT10");
		for (int i=0; i<responseTimeDistribution10MS.length; i++) {
			logger.warn("RT=" + (i*10) + " CT=" + responseTimeDistribution10MS[i]);
		}
		logger.warn("CALL_COUNT=" + callCount);
		logger.warn("PCT_100MS=" + 1.0*responseTimeDistribution100MS[0].longValue()/callCount);
		logger.warn("PCT_10MS=" + 1.0*responseTimeDistribution10MS[0].longValue()/callCount);
	}
	
	private void launchPeriodicHealthCheck(long delayMillis) {
		final long waitPeriod = delayMillis;
		new Thread (new Runnable() { 
			public void run() { 
				while (!stopHealthCheck) {
					long start = System.currentTimeMillis();
					String result = httpGet(CMBProperties.getInstance().getCQSServiceUrl() + "?Action=HealthCheck");
					long end = System.currentTimeMillis();
					logger.warn("HEALTH_CHECK_RT=" + (end-start));
					recordResponseTime(null, end-start);
					try {
						Thread.sleep(waitPeriod);
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				}
				logger.warn("HEALTH_CHECK_STOPPED");
			}
			private String httpGet(String api) {
			      URL url;
			      HttpURLConnection conn;
			      BufferedReader rd;
			      String line;
			      String result = "";
			      try {
			         url = new URL(api);
			         conn = (HttpURLConnection) url.openConnection();
			         conn.setRequestMethod("GET");
			         rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			         while ((line = rd.readLine()) != null) {
			            result += line;
			         }
			         rd.close();
			      } catch (Exception ex) {
			         ex.printStackTrace();
			      }
			      return result;
			   }
		}).start();
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
	public void Create10Queues10Partitions() {
		CreateNQueues(10, 100, 10, 1, true);
	}

	@Test
	public void Create10Queues1Partition() {
		CreateNQueues(10, 100, 1, 1, true);
	}

	@Test
	public void Create100Queues() {
		CreateNQueues(100, true);
	}

	//@Test
	public void Create1000Queues() {
		CreateNQueues(1000, true);
	}

	//@Test
	public void Create10000Queues() {
		CreateNQueues(10000, true);
	}

	//@Test
	public void Create100000Queues() {
		CreateNQueues(100000, true);
	}

	@Test
	public void CreateQueuesConcurrent() {
		//queueName = "myqueue";
		//messageLength = 100;
		CreateQueuesConcurrent(2, 100, 20, 100, 1);
	}
	
	private void CreateQueuesConcurrent(long numQueuesPerThread, long numMessagesPerQueue, int numThreads, int numPartitions, int numShards) {

		launchPeriodicHealthCheck(5000);
		
		ScheduledThreadPoolExecutor ep = new ScheduledThreadPoolExecutor(numThreads + 10);
		
		final long nqpt = numQueuesPerThread;
		final long nmpq = numMessagesPerQueue;
		final int np = numPartitions;
		final int ns = numShards;

		long start = System.currentTimeMillis();
		
		for (int i=0; i<numThreads; i++) {
			ep.submit((new Runnable() { public void run() { CreateNQueues(nqpt, nmpq, np, ns, false); }}));
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
		
		long end = System.currentTimeMillis();
		
		stopHealthCheck = true;

		for (String message : report) {
			logger.warn(message);
		}
		
		printResponseTimeDistribution();
		
		long apisPerSec = (numThreads*numQueuesPerThread*numMessagesPerQueue*3)/((end-start)/1000);
		
		logger.warn("APIS_PER_SEC=" + apisPerSec);
		logger.warn("TOTAL_MESSAGES_FOUND=" + totalMessagesFound.longValue());
	}

	private String receiveMessage(String queueUrl) {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();

		receiveMessageRequest.setQueueUrl(queueUrl);
		receiveMessageRequest.setMaxNumberOfMessages(1);

		// use this to test with long poll

		//receiveMessageRequest.setWaitTimeSeconds(1);

		long start = System.currentTimeMillis();
		ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
		long end = System.currentTimeMillis();
		recordResponseTime(null, end-start);
		
		if (end-start>=500) {
			logger.warn("RECEIVE_RT=" + (end-start));
		}

		if (receiveMessageResult.getMessages().size() == 1) {

			String message = receiveMessageResult.getMessages().get(0).getBody();

			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());

			start = System.currentTimeMillis();
			sqs.deleteMessage(deleteMessageRequest);
			end = System.currentTimeMillis();
			recordResponseTime(null, end-start);
			
			if (end-start>=500) {
				logger.warn("DELETE_RT=" + (end-start));
			}
			
			return message;
		}

		return null;
	}

	private void CreateNQueues(long numQueues, boolean logReport) {
		CreateNQueues(numQueues, 100, 100, 1, logReport);
	}

	private void CreateNQueues(long numQueues, long numMessages, int numPartitions, int numShards, boolean logReport) {

		long testStart = System.currentTimeMillis();
		
		Set<String> queueUrls = new HashSet<String>();
		Map<String, String> messageMap = new HashMap<String, String>();

		if (numPartitions != 100) {
			attributeParams.put("NumberOfPartitions", numPartitions + "");
		}
		
		if (numShards != 1) {
			attributeParams.put("NumberOfShards", numShards + "");
		}

		try {

			long counter = 0;
			long createFailures = 0;
			long totalTime = 0;

			for (int i=0; i<numQueues; i++) {

				try {

					String name = null;
					if (queueName != null) {
						name = queueName;
					} else {
						name = QUEUE_PREFIX + randomGenerator.nextLong();
					}
					CreateQueueRequest createQueueRequest = new CreateQueueRequest(name);
					createQueueRequest.setAttributes(attributeParams);
					long start = System.currentTimeMillis();
					String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
					long end = System.currentTimeMillis();
					recordResponseTime(null, end-start);
					totalTime += end-start;
					logger.info("average creation millis: " + (totalTime/(i+1)) + " last: " + (end-start));
					queueUrls.add(queueUrl);
					logger.info("created queue " + counter + ": " + queueUrl + " " + numPartitions + " partitions, " + numShards + " shards");
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
						String msg = "" + messagesSent;
						if (messageLength > 0) {
							msg += "-" + generateRandomMessage(messageLength);
						}
						SendMessageResult result = sqs.sendMessage(new SendMessageRequest(queueUrl, msg));
						long end = System.currentTimeMillis();
						recordResponseTime(null, end-start);
						totalTime += end-start;
						logger.info("average send millis: " + (totalTime/(totalCounter+1)) + " last: " + (end-start));
						logger.info("sent message on queue " + i + " - " + counter + ": " + queueUrl);
						if (end-start>=500) {
							logger.warn("SEND_RT=" + (end-start));
						}
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

			for (int i=0; i<1.5*numMessages; i++) {

				counter = 0;

				String lastNumericContent = null;

				for (String queueUrl : queueUrls) {

					try {

						long start = System.currentTimeMillis();
						String messageBody = receiveMessage(queueUrl);
						long end = System.currentTimeMillis();
						totalTime += end-start;
						logger.info("average receive millis: " + (totalTime/(totalCounter+1)) + " last: " + (end-start));
						
						if (messageBody != null) {
							String numericContent = messageBody;
							if (messageBody.length()>0 && messageBody.contains("-")) {
								numericContent = messageBody.substring(0, messageBody.indexOf("-"));
							}
							logger.info("received message on queue " + i + " - " + counter + " : " + queueUrl + " : " + messageBody);
							if (lastNumericContent != null && numericContent != null && Long.parseLong(lastNumericContent) > Long.parseLong(numericContent)) {
								outOfOrder++;
							}
							if (messageMap.containsKey(messageBody)) {
								logger.warn("DUPLICATE=" + messageBody);
								duplicates++;
							} else {
								messageMap.put(messageBody, null);
							}
							messagesFound++;
							lastNumericContent = numericContent;
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

			if (deleteQueues) {
				for (String queueUrl : queueUrls) {
					try {
						long start = System.currentTimeMillis();
						sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
						long end = System.currentTimeMillis();
						recordResponseTime(null, end-start);
						totalTime += end-start;
						logger.info("average delete millis: " + (totalTime/(counter+1)) + " last: " + (end-start));
						logger.info("deleted queue " + counter + ": " + queueUrl);
						counter++;
					} catch (Exception ex) {
						logger.error("delete failure", ex);
						deleteFailures++;
					}
				}
			}
			
			long testEnd = System.currentTimeMillis();

			String message = "duration sec: " + ((testEnd-testStart)/1000) + " create failuers: " + createFailures +  " delete failures: " + deleteFailures + " send failures: " + sendFailures + " read failures: " + readFailures + " empty reads: " + emptyResponses + " messages found: " + messagesFound + " messages sent: " + messagesSent + " out of order: " + outOfOrder + " duplicates: " + duplicates + " distinct messages: " + messageMap.size(); 

			totalMessagesFound.addAndGet(messagesFound);
			
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
