package com.comcast.cqs.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CQSScaleQueuesTest extends CMBAWSBaseTest {

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
    private static int numBatchReceive = 1;
    
    private static AtomicLong totalMessagesFound = new AtomicLong(0);
    private static AtomicLong totalMessagesSent = new AtomicLong(0);
    private static AtomicLong totalMessageSendTime = new AtomicLong(0);
    private static AtomicLong totalMessageReceiveTime = new AtomicLong(0);
    private static AtomicLong totalMessageDeleteTime = new AtomicLong(0);
    private static AtomicLong totalNumReceiveCalls = new AtomicLong(0);
    
    private static boolean deleteQueues = true;
    
    private static HashMap<String, String> attributeParams = new HashMap<String, String>();
    
    static {
        attributeParams.put("MessageRetentionPeriod", "600");
        attributeParams.put("VisibilityTimeout", "30");
    }

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
			} else if (arg.startsWith("-br")) {
				numBatchReceive = Integer.parseInt(arg.substring(4));
			} else {
				System.out.println("Usage: CQSScaleQueuesTest -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties -nq=<number_queues_per_thread> -nm=<number_messages_per_queue> -nt=<number_threads> -np=<number_partitions> -ns=<number_shards> -ml=<message_length> -dq=<delete_queues_at_end_true_or_false> -qn=<queue_name_for_single_queue_tests> -ak=<access_key> -as=<access_secret> -br=<num_batch_receive>");
				System.out.println("Example: java CQSScaleQueuesTest -Dcmb.log4j.propertyFile=config/log4j.properties -Dcmb.propertyFile=config/cmb.properties -nq=10 -nm=10 -nt=10 -np=100 -ns=1 -dq=true -br=1");
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
		System.out.println("Number of messages received in batch: " + numBatchReceive);
		
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
		
		super.setup();

		for (int i=0; i<responseTimeDistribution100MS.length; i++) {
			responseTimeDistribution100MS[i] = new AtomicLong(0);
		}

		for (int i=0; i<responseTimeDistribution10MS.length; i++) {
			responseTimeDistribution10MS[i] = new AtomicLong(0);
		}
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
		/*logger.warn("RT100");
		for (int i=0; i<responseTimeDistribution100MS.length; i++) {
			logger.warn("RT=" + (i*100) + " CT=" + responseTimeDistribution100MS[i]);
			callCount += responseTimeDistribution100MS[i].longValue();
		}*/
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
			//Thread.sleep(5000);
		} catch (InterruptedException ex) {
			logger.error("fail", ex);
			fail(ex.getMessage());
		}

		logger.warn("ALL TEST FINISHED");
		
		long end = System.currentTimeMillis();
		
		stopHealthCheck = true;
		
		int c = 0;

		for (String message : report) {
			logger.warn(c + ": " + message);
			c++;
		}
		
		printResponseTimeDistribution();
		
		long apisPerSec = (numThreads*numQueuesPerThread*numMessagesPerQueue*3)/((end-start)/1000);
		
		logger.warn("APIS_PER_SEC=" + apisPerSec);
		logger.warn("TOTAL_MESSAGES_SENT=" + totalMessagesSent.longValue());
		logger.warn("TOTAL_MESSAGES_FOUND=" + totalMessagesFound.longValue());
		logger.warn("AVG_MSG_SEND_MS=" + totalMessageSendTime.longValue()/totalMessagesSent.longValue());
		logger.warn("AVG_MSG_RECEIVE_MS=" + totalMessageReceiveTime.longValue()/totalNumReceiveCalls.longValue());
		logger.warn("AVG_MSG_DELTE_MS=" + totalMessageDeleteTime.longValue()/totalMessagesFound.longValue());
	}

	private List<String> receiveMessage(String queueUrl) {

		List<String> messageBodies = new ArrayList<String>();
		
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();

		receiveMessageRequest.setQueueUrl(queueUrl);
		receiveMessageRequest.setMaxNumberOfMessages(numBatchReceive);

		// use this to test with long poll

		//receiveMessageRequest.setWaitTimeSeconds(1);

		long start = System.currentTimeMillis();
		//logger.info("event=REC_START");
		ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
		//logger.info("event=REC_END");
		long end = System.currentTimeMillis();
		recordResponseTime(null, end-start);
		totalMessageReceiveTime.addAndGet(end-start);
		totalNumReceiveCalls.incrementAndGet();
		
		if (end-start>=500) {
			logger.warn("RECEIVE_RT=" + (end-start));
		}

		for (Message msg : receiveMessageResult.getMessages()) {

			messageBodies.add(msg.getBody());

			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(msg.getReceiptHandle());

			start = System.currentTimeMillis();
			//logger.info("event=DEL_START");
			cqs1.deleteMessage(deleteMessageRequest);
			//logger.info("event=DEL_END");
			end = System.currentTimeMillis();
			recordResponseTime(null, end-start);
			totalMessageDeleteTime.addAndGet(end-start);
			
			if (end-start>=500) {
				logger.warn("DELETE_RT=" + (end-start));
			}
		}

		return messageBodies;
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
					String queueUrl = cqs1.createQueue(createQueueRequest).getQueueUrl();
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

						String msg = "" + messagesSent;
						if (messageLength > 0) {
							msg += "-" + generateRandomMessage(messageLength);
						}
						long start = System.currentTimeMillis();
						SendMessageResult result = cqs1.sendMessage(new SendMessageRequest(queueUrl, msg));
						long end = System.currentTimeMillis();
						recordResponseTime(null, end-start);
						totalMessageSendTime.addAndGet(end-start);
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
			
			totalMessagesSent.addAndGet(totalCounter);

			Thread.sleep(1000);

			long readFailures = 0;
			long emptyResponses = 0;
			long messagesFound = 0;
			long outOfOrder = 0;
			long duplicates = 0;
			totalTime = 0;
			totalCounter = 0;

			for (int i=0; i<1.5*Math.max(numMessages/numBatchReceive,1); i++) {

				String lastNumericContent = null;

				for (String queueUrl : queueUrls) {

					try {

						long start = System.currentTimeMillis();
						List<String> messageBodies = receiveMessage(queueUrl);
						long end = System.currentTimeMillis();
						totalTime += end-start;
						logger.info("avg_receive_ms=" + (totalTime/(totalCounter+1)) + " last_receive_ms=" + (end-start));
						
						if (messageBodies.size() > 0) {
							for (String messageBody : messageBodies) {
								String numericContent = messageBody;
								if (messageBody.length()>0 && messageBody.contains("-")) {
									numericContent = messageBody.substring(0, messageBody.indexOf("-"));
								}
								if (lastNumericContent != null && numericContent != null && Long.parseLong(lastNumericContent) > Long.parseLong(numericContent)) {
									outOfOrder++;
								}
								if (messageMap.containsKey(messageBody)) {
									logger.warn("event=DUPLICATE body=" + messageBody.substring(0, 50));
									duplicates++;
								} else {
									messageMap.put(messageBody, null);
								}
								messagesFound++;
								totalCounter++;
								lastNumericContent = numericContent;
								totalMessagesFound.incrementAndGet();
							}
							String messagePrefix = messageBodies.get(0);
							if (messagePrefix.length() > 50) {
								messagePrefix = messagePrefix.substring(0, 50);
							}
							logger.info("event=received_message batch_size=" + messageBodies.size() + " msg_count=" + totalCounter + " queue_url=" + queueUrl + " body=" + messagePrefix);
						} else {
							logger.info("event=no_message_found msg_count=" + totalCounter + " queue_url"  + queueUrl);
							emptyResponses++;
						}


					} catch (Exception ex) {

						logger.error("read failure, will retry: " + queueUrl, ex);
						readFailures++;
					}
				}
			}

			long deleteFailures = 0;
			counter = 0;
			totalTime = 0;

			if (deleteQueues) {
				Thread.sleep(1000);
				for (String queueUrl : queueUrls) {
					try {
						long start = System.currentTimeMillis();
						cqs1.deleteQueue(new DeleteQueueRequest(queueUrl));
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

			String message = Thread.currentThread().getName() + " duration sec: " + ((testEnd-testStart)/1000) + " create failuers: " + createFailures +  " delete failures: " + deleteFailures + " send failures: " + sendFailures + " read failures: " + readFailures + " empty reads: " + emptyResponses + " messages found: " + messagesFound + " messages sent: " + messagesSent + " out of order: " + outOfOrder + " duplicates: " + duplicates + " distinct messages: " + messageMap.size(); 

			logger.info("event=thread_finished");
			
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
