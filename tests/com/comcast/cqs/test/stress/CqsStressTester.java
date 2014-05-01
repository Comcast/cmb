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
package com.comcast.cqs.test.stress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import com.amazonaws.AmazonServiceException;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.io.CommunicationUtils;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.util.CQSErrorCodes;

//
// some useful greps:
//
// grep -i responsetime /tmp/receiveMessage.log | grep ReceiveMessage | cut -f9- -d= > /tmp/redisTime.out
//
// grep -i responsetime /tmp/receiveMessage.log | grep ReceiveMessage | cut -f5-5 -d= > /tmp/responseTime.log
//
// grep -i responsetime /tmp/receiveMessage.log | grep ReceiveMessage |  cut -f6-6 -d= > /tmp/cassandraTime.log
//
// some useful splunks:
//
// response time for receive message operation:
//
// sourcetype=cqs action=ReceiveMessage | chart perc95(responseTimeMS) perc90(responseTimeMS) avg(responseTimeMS) median(responseTimeMS) max(responseTimeMS) perc95(redisTime) perc90(redisTime) avg(redisTime) median(redisTime) max(redisTime) perc95(CassandraTimeMS) perc90(CassandraTimeMS) avg(CassandraTimeMS) median(CassandraTimeMS) max(CassandraTimeMS)
//
// sourcetype=cqs action=ReceiveMessage | timechart avg(redisTime) avg(CassandraTimeMS) avg(responseTimeMS)
//
// manage changes in message visibility, performed once a second:
//
// sourcetype=cqs event=chechAndProcessRevisibleSet | chart avg(responseTimeMS) p95(responseTimeMS)
//
// manage hidden messages every 10 sec, look for hiddenSetSize parameter:
//
// sourcetype=cqs event=RevisibleProcessor | chart avg(responseTimeMS) p95(responseTimeMS)
//

// todo: check for message order

public class CqsStressTester {

    private static Logger logger = Logger.getLogger(CqsStressTester.class);

	static ConcurrentHashMap<Integer, AtomicInteger> timeReceiveMessageCountMap = new ConcurrentHashMap<Integer, AtomicInteger>();
	static ConcurrentHashMap<Integer, AtomicInteger> timeSendMessageCountMap = new ConcurrentHashMap<Integer, AtomicInteger>();
	static Set<String> sendMessageIdSet = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

    //static private ConcurrentLinkedQueue<Long> receiveLacencyMSList = new ConcurrentLinkedQueue<Long>();

    public long startTime = System.currentTimeMillis();
    final static SchemeRegistry schemeRegistry = new SchemeRegistry();
    public final static ThreadSafeClientConnManager cm;
    final static HttpClient httpClient;

    static {

       schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
       schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));

       cm = new ThreadSafeClientConnManager(schemeRegistry);
       // Increase max total connection to 200
       cm.setMaxTotal(CMBProperties.getInstance().getCNSPublisherHttpEndpointConnectionPoolSize());
       // Increase default max connection per route to 20
       cm.setDefaultMaxPerRoute(CMBProperties.getInstance().getCNSPublisherHttpEndpointConnectionsPerRouteSize());

       httpClient = new DefaultHttpClient(cm);
    }

    private List<String> queueUrls = new ArrayList<String>();
    private HashMap<String, List<Receiver>> receiverMap = new HashMap<String, List<Receiver>>();
	private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static HashMap<String, String> attributeParams = new HashMap<String, String>();
    private static AtomicInteger messageCount = new AtomicInteger(0);
    static Random rand = new Random();
    static User user = null;
    final static int revisiblePercentage = CQSStressTestProperties.getInstance().getRevisiblePercentage();

    private static void setup() throws Exception {

        attributeParams.put("MessageRetentionPeriod", "6000"); // the number of seconds when a message is available in the system - 10 hours
        attributeParams.put("VisibilityTimeout", "60"); // the number of seconds a message is hidden from the next user - 1 min

        try {
	        IUserPersistence userPersistence = new UserCassandraPersistence();
	        user = userPersistence.getUserByName("cqs_stress_user");

	        if (user == null) {
	            user = userPersistence.createUser("cqs_stress_user", "cqs_stress_user");
	        }

        } catch (Exception ex) {
            logger.error("Action=setup status=exception ", ex);
        }
    }

    // This creates the specified number of queues

    private ScheduledExecutorService createQueuesAndInitializePublishersAndReceivers() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, InterruptedException {

    	String fixedQueueName = "testQueue";
    	int totalNumberOfQueues = CQSStressTestProperties.getInstance().getNumberOfQueues();

        String[] queueNames = CQSStressTestProperties.getInstance().getQueueNames();

        if (queueNames != null) {
    		totalNumberOfQueues = queueNames.length;
    	}

        ICQSMessagePersistence messagePersistence = PersistenceFactory.getCQSMessagePersistence();

        for (int i=0; i<totalNumberOfQueues; i++) {

        	String queueName = fixedQueueName + rand.nextInt() + "_" + i;

        	if (queueNames != null) {
    			queueName = queueNames[i];
    		}

        	String myQueueUrl = createQueue(queueName);
			queueUrls.add(myQueueUrl);

			if (CQSStressTestProperties.getInstance().getNumberOfSendersPerQueue() > 0) {
				messagePersistence.clearQueue(myQueueUrl, 0);
			}

			logger.info("QueueUrl" + i + " = " + myQueueUrl);

			//first tickle the empty queue population by calling a receive when nothing is in the queue

			receiveMessage(myQueueUrl, 1, 100);

			Thread.sleep(500);
    	}

        for (String queueUrl : queueUrls) {
			createReceivers(queueUrl);
        }

        return createSenders(queueUrls);
   }

    private ScheduledExecutorService createSenders(List<String> queueUrls) {

    	int senderCount = CQSStressTestProperties.getInstance().getNumberOfSendersPerQueue();

    	if (senderCount == 0) {
    		return null;
    	}

    	int numberOfMessagesPerSec = CQSStressTestProperties.getInstance().getMessagesPerQueuePerSecond();
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(queueUrls.size()*senderCount);

		for (String queueUrl : queueUrls) {
			for (int i=0; i<senderCount; i ++) {
				scheduledExecutorService.scheduleWithFixedDelay(new MessageSender(queueUrl, i), rand.nextInt(100), 1000*senderCount/numberOfMessagesPerSec, TimeUnit.MILLISECONDS);
			}
		}

		return scheduledExecutorService;
    }

	private String generateRandomMessage(int length) {

		StringBuilder sb = new StringBuilder(length);

		Date now = new Date();
		sb.append("@").append(now.getTime()).append("*");
		sb.append(now).append("%");

		for (int i=sb.length(); i<length; i++) {
			sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
		}

		return sb.toString();
	}

	private void createReceivers(String queueUrl) {

    	AbstractDurablePersistence persistence = DurablePersistenceFactory.getInstance();
    	long receiverCount = CQSStressTestProperties.getInstance().getNumberOfReceiversPerQueue();
    	List<Receiver> receiverListForQueue = new ArrayList<Receiver>();

    	for (int i=0; i<receiverCount; i++) {
    		Receiver receiver = new Receiver(queueUrl, i, persistence);
    		receiver.start();
    		receiverListForQueue.add(receiver);
    	}

    	receiverMap.put(queueUrl, receiverListForQueue);
    }

    protected void doChart(int totalMessagesSent, int totalMessagesReceived) throws IOException {

    	generateChart("Send message count: Total Messages Sent=" + totalMessagesSent,  timeSendMessageCountMap, "SendMessage_Run_" + startTime, new Date(startTime), new Date());
        generateChart("Receive message count: Total Messages Received=" + totalMessagesReceived,  timeReceiveMessageCountMap, "ReceiveMessage_Run_" + startTime, new Date(startTime), new Date());
    }

    private byte[] generateChart(String title, ConcurrentHashMap<Integer, AtomicInteger> metric, String id, Date startTime, Date endTime) throws IOException {

    	XYSeries series = new XYSeries(title);

    	for (Entry<Integer, AtomicInteger> entry : metric.entrySet()) {
    		series.add(entry.getKey().intValue(), entry.getValue().intValue());
    	}

    	XYSeriesCollection dataset = new XYSeriesCollection(series);

    	JFreeChart chart = ChartFactory.createXYBarChart(
                "Start: " + startTime + " End: " + endTime + " Message Count: " + messageCount,
                "Test Second",
                false,
                "Number of Messages",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
            );

    	File file = new File("/tmp/" + id + ".jpeg");
    	//File file = new File(getServletContext().getRealPath("WEB-INF" + "/" + id + ".jpeg"));
    	ChartUtilities.saveChartAsJPEG(file, chart, 1600, 400);
    	//byte b[] = Files.toByteArray(file);
    	//return b;

    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
    	ChartUtilities.writeChartAsJPEG(bos, chart, 2400, 400);
    	return bos.toByteArray();
    }
    /**
	 * @param args
	 */
	public static void main(String[] args) {

		try {

			CMBControllerServlet.valueAccumulator.initializeAllCounters();
	        setup();
			Util.initLog4j();
			CqsStressTester tester = new CqsStressTester();
			ScheduledExecutorService scheduledExecutorService = tester.createQueuesAndInitializePublishersAndReceivers(); //will clear queues

			long totalMessagesReceived = 0;
			long totalMessagesDeleted = 0;
			long totalMessagesRevisibled = 0;
			long totalEmptyResponses = 0;
			long totalDuplicates = 0;
			long totalOutOfOrderMessages = 0;

			int testDurationSeconds = CQSStressTestProperties.getInstance().getTestDurationSeconds();
			Thread.sleep(testDurationSeconds*1000);

			if (scheduledExecutorService != null) {
				scheduledExecutorService.shutdown();
			}

			logger.info("===Sender Shutdown Triggered==");

			for (String queueUrl : tester.receiverMap.keySet()) {
		    	for (Receiver receiver : tester.receiverMap.get(queueUrl)) {
		    		receiver.setContinueThread(false);
		    	}
			}

			for (String queueUrl : tester.receiverMap.keySet()) {

				Set<String> messageIdMaster = new HashSet<String>();
		    	List<Integer> deleteTimesMaster = new ArrayList<Integer>();
		    	List<Long> flightTimesMaster = new ArrayList<Long>();
		    	List<Long> receiveTimesMaster = new ArrayList<Long>();

		    	for (Receiver receiver : tester.receiverMap.get(queueUrl)) {

					receiver.join();

					logger.warn("===================================================================================================================");
					logger.warn("TheadId=" + receiver.getThreadId() + " receiveMessageCount=" + receiver.getTotalMessagesReceived() + " deletedMessageCount=" + receiver.getTotalMessagesDeleted() + " revisibledMessageCount=" + receiver.getTotalMessagesRevisibled());
					logger.warn("===================================================================================================================");

					totalMessagesReceived += receiver.getTotalMessagesReceived();
					totalMessagesDeleted += receiver.getTotalMessagesDeleted();
					totalMessagesRevisibled += receiver.getTotalMessagesRevisibled();
					totalOutOfOrderMessages += receiver.getTotalOutOfOrderMessages();
					totalDuplicates += checkAndCombine(messageIdMaster, receiver.messageIds);
					//deleteTimesMaster.addAll(receiver.deleteLatencyMSList);
					//flightTimesMaster.addAll(receiver.flightTimeList);
					totalEmptyResponses += receiver.emptyResponseCount;
				}

				logger.warn("===================================================================================================================");

				Iterator<String> iter = sendMessageIdSet.iterator();

				while (iter.hasNext()) {
					logger.error("Missed message:" + iter.next());
				}

				Collections.sort(deleteTimesMaster);
				Collections.sort(flightTimesMaster);
                //receiveTimesMaster.addAll(receiveLacencyMSList);
				Collections.sort(receiveTimesMaster);

				/*logger.warn("Receive message latencies");

				if (receiveTimesMaster.size() > 0) {

					for (int i=5; i<=100; i+=5) {

						int percentileIndex = receiveTimesMaster.size()*i/100 - 1;

						if (percentileIndex < 0) {
							percentileIndex = 0;
						}

						logger.warn("" + i + "th percentile=" + receiveTimesMaster.get(percentileIndex));
					}
				}

				logger.warn("===================================================================================================================");
				logger.warn("Message flight time latencies");

				if (flightTimesMaster.size() > 0) {

					for (int i=5; i<=100; i+=5) {

						int percentileIndex = flightTimesMaster.size()*i/100 - 1;

						if (percentileIndex < 0) {
							percentileIndex = 0;
						}

						logger.warn("" + i + "th percentile=" + flightTimesMaster.get(percentileIndex));
					}
				}

				logger.warn("===================================================================================================================");
				logger.warn("Delete message latencies");

				if (deleteTimesMaster.size() > 0) {

					for (int i=5; i<=100; i+=5) {

						int percentileIndex = deleteTimesMaster.size()*i/100 - 1;

						if (percentileIndex < 0) {
							percentileIndex = 0;
						}

						logger.warn("" + i + "th percentile=" + deleteTimesMaster.get(percentileIndex));
					}
				}*/

			}

			logger.warn("===================================================================================================================");
			logger.warn("===================================================================================================================");
			logger.warn("totalMessagesSent=" + tester.messageCount.get() + " totalMessagesReceived=" + totalMessagesReceived + " totalMessagesDeleted=" + totalMessagesDeleted + " totalMessagesRevisibled=" + totalMessagesRevisibled);
			logger.warn("===================================================================================================================");
			logger.warn("totalEmptyResponses=" + totalEmptyResponses);
			logger.warn("totalDuplicates=" + totalDuplicates);
			logger.warn("totalOutOfOrderMessages=" + totalOutOfOrderMessages);
			logger.warn("totalMessagesLost=" + sendMessageIdSet.size());
			logger.warn("===================================================================================================================");
			logger.warn("totalRunTimeMillis=" + (System.currentTimeMillis()-tester.startTime) + " status=Exit");
			logger.warn("===================================================================================================================");
			logger.warn("===================================================================================================================");

		} catch (Exception e) {
			logger.error("Thread=main status=exception message=setup_failure ", e);
		} finally {
			CMBControllerServlet.valueAccumulator.deleteAllCounters();
		}
	}

	private static long checkAndCombine(Set<String> masterSet, Set<String> receivedMessageIds) {

		long totalDuplicates = 0;

		if (masterSet == null || receivedMessageIds == null) {
			return 0;
		}

		for (String messageId : receivedMessageIds) {

			if (masterSet.contains(messageId)) {
				logger.error("Action=checkAndCombine status=error message=duplicateId:" + messageId);
				totalDuplicates++;
			} else {
				masterSet.add(messageId);
			}
		}

		return totalDuplicates;
	}

    private class MessageSender implements Runnable {

		private String queueUrl;
		private String threadId;

    	public MessageSender(String queueUrl, int index) {
			setQueueUrl(queueUrl);
			setThreadId(queueUrl, index);
		}

		public String getQueueUrl() {
			return queueUrl;
		}

		public void setQueueUrl(String queueUrl) {
			this.queueUrl = queueUrl;
		}

		public void setThreadId(String queueUrl, int index) {

			this.queueUrl = queueUrl;

			if (queueUrl == null || queueUrl.length() == 0) {
				return;
			}

			String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);
			this.threadId = "Sender_" + queueName + "_" + index;
		}

		@Override
		public void run() {

			try {
		        CMBControllerServlet.valueAccumulator.initializeAllCounters();
			} catch (Exception ex) {
	            logger.error("Thread=" + threadId + " Action=setup status=exception ", ex);
			}

			int maxSendBatchSize = CQSStressTestProperties.getInstance().getSendMessageBatchSize();
			maxSendBatchSize = 1;  // Till we add support for send message batch

			try {

	    		String currentTime = "" + System.currentTimeMillis();

	    	    long startNanoTime = System.nanoTime();
	    	    int totalMessagesSuccessful = 0;

	            for (int i = 0; i < maxSendBatchSize; i++) {

	            	//TBD: Make this a configurable parameter
	            	String messageBodyRandom = generateRandomMessage(2000);
	            	long index = rand.nextLong();
	            	String messageIndex = Thread.currentThread().getId() + "_" + index;
	            	sendMessageIdSet.add(messageIndex);
		    		String messageBody = "currentTime=" + currentTime + " messageIndex=" + messageIndex + " messagebody=message_" + currentTime + "_" + messageBodyRandom + "_" + index;

		    		if (sendMessage(this.queueUrl, messageBody) != null) {
		    			totalMessagesSuccessful++;
		    		}

	                logger.debug("Thread=" + threadId + " Action=Sent MessageBody '" + messageBody + "'");
	            }

	            //addSendMessageCount((int)((System.currentTimeMillis() - startTime)/1000), totalMessagesSuccessful);
	            long endNanoTime = System.nanoTime();
	            logger.info("Thread=" + threadId + " Action=SendMessageBatch latencyNano=" + (endNanoTime-startNanoTime));
				int count = messageCount.addAndGet(totalMessagesSuccessful);

				if (count % 100 == 0) {
					logger.info("event=publish queueUrl=" + getQueueUrl() + " totalCount=" + count);
				}

			} catch (AmazonServiceException ase) {
				displayServiceException("" + Thread.currentThread().getId(), "SendMessageBatch", ase);
			} finally {
				CMBControllerServlet.valueAccumulator.deleteAllCounters();
			}
		}
    }

	class Receiver extends Thread {

		private String queueUrl;
		private String threadId;
		private long totalMessagesReceived = 0;
		private long totalMessagesDeleted = 0;
		private long totalMessagesRevisibled = 0;
		private long totalOutOfOrderMessages = 0;
		private long lastMessageReceivedTime = 0;
		private boolean continueThread = true;
		private AbstractDurablePersistence persistence;
		private static final int visibilityTimeout = 600;
		private Set<String> messageIds = new HashSet<String>();
		//private List<Integer> deleteLatencyMSList = new ArrayList<Integer>();
		//private Set<Long> flightTimeList = new HashSet<Long>();
		private long emptyResponseCount = 0;

		public Receiver(String queueUrl, int index, AbstractDurablePersistence persistence) {
			setQueueUrl(queueUrl);
			setThreadId(queueUrl, index);
			setPersistence(persistence);
		}

		public String getQueueUrl() {
			return queueUrl;
		}

		public void setQueueUrl(String queueUrl) {
			this.queueUrl = queueUrl;
		}

		public void run() {

			try {
		        CMBControllerServlet.valueAccumulator.initializeAllCounters();
			} catch (Exception ex) {
	            logger.info("Thread=" + threadId + " Action=setup status=exception ", ex);
			}

			int maxReceiveBatchSize = CQSStressTestProperties.getInstance().getReceiveMessageBatchSize();

			try {

				int emptyCount = 0;

				while (true) {

		    	    long startNanoTime = System.currentTimeMillis();

		    	    List<CQSMessage> messageList = receiveMessage(this.queueUrl, maxReceiveBatchSize, visibilityTimeout);

		    	    if (messageList == null) {
		    	    	Thread.sleep(300);
		    	    	continue;
		    	    }

		    	    long currentTime = System.currentTimeMillis();
		    		long messageFlightTime = 0;
		    		String messageIndex = "";

		    		if (messageList.size() == 0) {

                        emptyResponseCount++;
		    			logger.info("Thread=" + threadId + " Action=ReceiveMessage batchSize=" + maxReceiveBatchSize + " status=Empty count=" + emptyCount);

		    			if (!isContinueThread()) {

                        	// sleep an extra second if no messages are there to account for potentially hidden messages

							Thread.sleep(1000);

                        	emptyCount++;

                        	if (emptyCount > 10) {
			    				logger.info("Thread=" + threadId + "Action=ReceiveMessage status=Completed");
			    				return;
			    			}
		    			}

		    		} else {

		    			emptyCount = 0;
		    			totalMessagesReceived += messageList.size();
		    			logger.info("Thread=" + threadId + " Action=ReceiveMessage batchSize=" + maxReceiveBatchSize + " receivedCount=" + messageList.size());
		    			//addReceiveMessageCount((int)((System.currentTimeMillis() - startTime)/1000), messageList.size());
		    		}

		    		int delayBetweenReceiveAndDelete = CQSStressTestProperties.getInstance().getDelayBetweenReceiveAndDeleteMS();

		    		if (delayBetweenReceiveAndDelete > 0) {
		    			Thread.sleep(delayBetweenReceiveAndDelete + rand.nextInt(40));
		    		}

		    		for (CQSMessage message: messageList) {

		    			String[] bodyParts = message.getBody().split(" ");

		    			for (String bodyPart : bodyParts) {

		    				String[] subParts = bodyPart.split("=");

		    				if (subParts[0].equals("currentTime")) {

		            			long messageSendTime = Long.parseLong(subParts[1]);
		    					messageFlightTime = currentTime - messageSendTime;

		    					if (lastMessageReceivedTime != 0 && lastMessageReceivedTime > messageSendTime) {
		    						totalOutOfOrderMessages++;
		    						//logger.info("Event=MessageOutOfOrder messageId=" + messageIndex + " messageSendTime=" + messageSendTime + " lastMessageReceivedTime=" + lastMessageReceivedTime + " delta=" + Math.abs(lastMessageReceivedTime-messageSendTime));
		    					}

		    					lastMessageReceivedTime = messageSendTime;

		            		} else if (subParts[0].equals("messageIndex")) {
		            			messageIndex = subParts[1];
		            		}
		            	}

		            	if (messageIds.contains(messageIndex)) {
		            		logger.error("Action=receiveMessage status=error exception=Duplicate id: " + messageIndex);
		            	} else {
		            		messageIds.add(messageIndex);
		            	}

		            	//flightTimeList.add(messageFlightTime);

		            	logger.info("Thread=" + threadId + " Action=ReceivedMessage MessageIndex=" + messageIndex + " totalTimeInFlightMillis=" + messageFlightTime);

		            	startNanoTime = System.nanoTime();

		            	if (revisiblePercentage > 0 && rand.nextInt(100)+1 <= revisiblePercentage) {
		                    changeMessageVisibility(this.queueUrl, message.getReceiptHandle());
		                    totalMessagesRevisibled += 1;
		                    totalMessagesReceived -= 1;
		                    messageIds.remove(messageIndex);
			            	long endNanoTime = System.nanoTime();
			            	logger.info("Thread=" + threadId + " Action=RevisibleMessage MessageIndex=" + messageIndex + " latencyNano=" + (endNanoTime-startNanoTime));
		            	} else {
			            	sendMessageIdSet.remove(messageIndex);
			            	deleteMessage(this.queueUrl, message.getReceiptHandle());
			            	totalMessagesDeleted += 1;
			            	long endNanoTime = System.nanoTime();
			            	logger.info("Thread=" + threadId + " Action=DeleteMessage MessageIndex=" + messageIndex + " latencyNano=" + (endNanoTime-startNanoTime));
		            	}

		            	//deleteLatencyMSList.add(new Integer((int)(endNanoTime-startNanoTime)/1000000));
		            }

		    		int delayBetweenReceivesMS = CQSStressTestProperties.getInstance().getDelayBetweenReceivesMS();

		    		if (delayBetweenReceivesMS > 0) {
	            		try {
							Thread.sleep(delayBetweenReceivesMS);
						} catch (InterruptedException e) {
							logger.error("Action=ReceiveMessage status=Exception ", e);
						}
	            	}
				}

			} catch (AmazonServiceException ase) {
				displayServiceException(threadId, "ReceiveMessage/DeleteMessage/RevisibleMessage", ase);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				CMBControllerServlet.valueAccumulator.deleteAllCounters();
			}
		}

		public String getThreadId() {
			return threadId;
		}

		public void setThreadId(String queueUrl, int index) {

			this.queueUrl = queueUrl;

			if (queueUrl == null || queueUrl.length() == 0) {
				return;
			}

			String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);
			this.threadId = "Receiver_" + queueName + "_" + index;
		}

		public long getTotalMessagesReceived() {
			return totalMessagesReceived;
		}

		public void setTotalMessagesReceived(long totalMessagesReceived) {
			this.totalMessagesReceived = totalMessagesReceived;
		}

		public long getTotalMessagesDeleted() {
			return totalMessagesDeleted;
		}

		public void setTotalMessagesDeleted(long totalMessagesDeleted) {
			this.totalMessagesDeleted = totalMessagesDeleted;
		}

		public long getTotalMessagesRevisibled() {
			return totalMessagesRevisibled;
		}

		public void setTotalMessagesRevisibled(long totalMessagesRevisibled) {
			this.totalMessagesRevisibled = totalMessagesRevisibled;
		}

		public synchronized boolean isContinueThread() {
			return continueThread;
		}

		public synchronized void setContinueThread(boolean continueThread) {
			this.continueThread = continueThread;
		}

		public AbstractDurablePersistence getPersistence() {
			return persistence;
		}

		public void setPersistence(AbstractDurablePersistence persistence) {
			this.persistence = persistence;
		}

		public long getTotalOutOfOrderMessages() {
			return this.totalOutOfOrderMessages;
		}
	}

    private void displayServiceException(String threadId, String action, AmazonServiceException ase) {
    	logger.error("ThreadId=" + threadId + " Action=" + action);
        logger.error("Caught an AmazonServiceException, which means your request made it to Amazon SQS, but was rejected with an error response for some reason.");
        logger.error("Error Message=" + ase.getMessage());
        logger.error("HTTP Status Code=" + ase.getStatusCode());
        logger.error("AWS Error Code=" + ase.getErrorCode());
        logger.error("Error Type=" + ase.getErrorType());
        logger.error("Request ID=" + ase.getRequestId());
    }

    public void addReceiveMessageCount(int second, int count) {
    	AtomicInteger val = null;
		timeReceiveMessageCountMap.putIfAbsent(second, new AtomicInteger(0));
		val = timeReceiveMessageCountMap.get(second);
    	val.addAndGet(count);
    }

    public void addSendMessageCount(int second, int count) {
    	AtomicInteger val = null;
    	timeSendMessageCountMap.putIfAbsent(second, new AtomicInteger(0));
    	val = timeSendMessageCountMap.get(second);
    	val.addAndGet(count);
    }

    public String createQueue(String queueName) {
    	Map<String, String[]> params = new HashMap<String, String[]>();
		CommunicationUtils.addParam(params,"Action", "CreateQueue");
		CommunicationUtils.addParam(params, "QueueName", queueName);
		CommunicationUtils.addParam(params, "AWSAccessKeyId", user.getAccessKey());
		CommunicationUtils.addParam(params, "Version", "2009-02-01");
		try {
			String response = send(params, CMBProperties.getInstance().getCQSServiceUrl());
			return CqsStressTester.deserialize(response, "QueueUrl").trim();
		} catch (Exception e) {
			logger.error("Action=CreateQueue status=error exception=", e);
			return null;
		}
    }

    public String sendMessage(String queueUrl, String messageBody) {
		Map<String, String[]> params = new HashMap<String, String[]>();
		CommunicationUtils.addParam(params,"Action", "SendMessage");
		CommunicationUtils.addParam(params, "MessageBody", messageBody);
		CommunicationUtils.addParam(params, "AWSAccessKeyId", user.getAccessKey());
		CommunicationUtils.addParam(params, "Version", "2009-02-01");
		try {
			String response = send(params, queueUrl);
			String messageId = CqsStressTester.deserialize(response, "MessageId");
			return (messageId!=null)?messageId.trim() : null;
		} catch (Exception e) {
			logger.error("Action=sendMessage status=error exception=", e);
			return null;
		}
    }

    public List<CQSMessage> receiveMessage(String queueUrl, int maxNoOfMessages, int visibilityTimeout) {

    	// Max number of messages will be set to 1 for now
    	maxNoOfMessages = 1;
		Map<String, String[]> params = new HashMap<String, String[]>();
		CommunicationUtils.addParam(params,"Action", "ReceiveMessage");
		CommunicationUtils.addParam(params, "MaxNumberOfMessages", "" + maxNoOfMessages);
		CommunicationUtils.addParam(params, "VisibilityTimeout", "" + visibilityTimeout);
		CommunicationUtils.addParam(params, "AWSAccessKeyId", user.getAccessKey());
		CommunicationUtils.addParam(params, "Version", "2009-02-01");

		try {
			long ts1 = System.currentTimeMillis();
			String response = send(params, queueUrl);
			long elapsedTime = System.currentTimeMillis() - ts1;
			logger.info("Total time spent on receiveMessage=" + elapsedTime);

			if (response.indexOf("<Body>") > 0) {
				//receiveLacencyMSList.add(elapsedTime);
			}

			return CqsStressTester.deserializeMessage(response);

		} catch (Exception e) {
			logger.error("Action=receiveMessage status=error exception=", e);
			return null;
		}
    }

    public void deleteMessage(String queueUrl, String receiptHandle) {

    	Map<String, String[]> params = new HashMap<String, String[]>();
		CommunicationUtils.addParam(params,"Action", "DeleteMessage");
		CommunicationUtils.addParam(params, "ReceiptHandle", receiptHandle);
		CommunicationUtils.addParam(params, "AWSAccessKeyId", user.getAccessKey());
		CommunicationUtils.addParam(params, "Version", "2009-02-01");

		try {
			send(params, queueUrl);
		} catch (Exception e) {
			logger.error("Action=deleteMessage status=error exception=", e);
		}
    }

    public void changeMessageVisibility(String queueUrl, String receiptHandle) {

    	Map<String, String[]> params = new HashMap<String, String[]>();

    	CommunicationUtils.addParam(params,"Action", "ChangeMessageVisibility");
		CommunicationUtils.addParam(params, "ReceiptHandle", receiptHandle);
		int randomVisibilityTimeoutSecs = rand.nextInt(5);
		CommunicationUtils.addParam(params, "VisibilityTimeout", randomVisibilityTimeoutSecs+"");
		CommunicationUtils.addParam(params, "AWSAccessKeyId", user.getAccessKey());
		CommunicationUtils.addParam(params, "Version", "2009-02-01");

		try {
			send(params, queueUrl);
		} catch (Exception e) {
			logger.error("Action=changeMessageVisibilityTimeout status=error exception=", e);
		}
    }

    public String send(Map<String, String[]> params, String endPoint) throws Exception {

		logger.debug("event=send_cqs_message endpoint=" + endPoint);

		String url = endPoint;
		logger.debug("event=send_cqs_message url=" + url + " endpoint=" + endPoint+ "\"");

		Set<String> parameters = params.keySet();
		boolean first = true;

		for (String param: parameters) {
			if(first) {url += "?"; first=false;}
			else url += "&";
			url += URLEncoder.encode(param,"UTF-8") + "=" + URLEncoder.encode(params.get(param)[0], "UTF-8");
		}

		String resp = "";

		try {
			logger.debug("Sending request to url:" + url);
			 //resp = sendHttpMessage(url);
			resp = send(url, "");
		} catch(Exception e) {
			logger.error("event=send_cqs_message endpoint=" + endPoint + " exception=" + e.toString(), e);
			throw new CMBException(CQSErrorCodes.InternalError, "internal service error");
		}
		return resp;
	}

    public String send(String endpoint, String message) throws Exception {

        logger.debug("event=send_http_request endpoint=" + endpoint + "\" message=\"" + message + "\"");

        if ((message == null) || (endpoint == null)) {
            logger.debug("event=send_http_request error_code=MissingParameters endpoint=" + endpoint + "\" message=\"" + message + "\"");
            throw new Exception("Message and Endpoint must both be set");
        }

        HttpPost httpPost = new HttpPost(endpoint);
        StringEntity stringEntity = new StringEntity(message);
        httpPost.setEntity(stringEntity);

        HttpResponse response = httpClient.execute(httpPost);
        response.getStatusLine().getStatusCode();

        HttpEntity entity = response.getEntity();

        if (entity != null) {
            InputStream instream = entity.getContent();
            InputStreamReader responseReader = new InputStreamReader(instream);
            StringBuffer responseB = new StringBuffer();

            char []arr = new char[1024];
            int size = 0;

            while ((size = responseReader.read(arr, 0, arr.length)) != -1) {
                responseB.append(arr, 0, size);
            }

            instream.close();
            return responseB.toString();
        }

        logger.error("Could not get response entity");
        System.exit(1);
        return null;
    }

	public static List<CQSMessage> deserializeMessage(String serializedMessage) {
		javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		javax.xml.parsers.SAXParser saxParser;
		SaxHandler p = new SaxHandler();
		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(serializedMessage.getBytes()), p);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (Exception e) {
		    e.printStackTrace();
		}

		CQSMessage cqsMessage = null;

		try {
			if (p.getValueByKey("Body") == null) {
				return new ArrayList<CQSMessage>();
			}
			cqsMessage = new CQSMessage(p.getValueByKey("Body"), p.getAttributes());
		    cqsMessage.setMessageId(p.getValueByKey("MessageId"));
			cqsMessage.setReceiptHandle(p.getValueByKey("ReceiptHandle"));
		    cqsMessage.setMD5OfBody(p.getValueByKey("MD5OfBody"));
		} catch (NoSuchAlgorithmException e) {
		    e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return java.util.Arrays.asList(cqsMessage) ;
	}

	public static String deserialize(String serialized, String key) {
		javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		javax.xml.parsers.SAXParser saxParser;
		SaxHandler p = new SaxHandler();
		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(serialized.getBytes()), p);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (Exception e) {
		    e.printStackTrace();
		}

		return p.getValueByKey(key);
	}

    private static class SaxHandler extends org.xml.sax.helpers.DefaultHandler {
        private CharArrayWriter contents = new CharArrayWriter();
        private HashMap<String, String> messageMap = new HashMap<String, String>();
        private String _name;
        private String _value;
        private HashMap<String, String> messageAttributes = new HashMap<String, String>();

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        	// clear the contents since we are in a new tag

        	if (qName.equals("Message")) {
        		messageMap.clear();
        		messageAttributes.clear();
        	}
        	contents.reset();
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
        	if (qName.equals("Name")) {
        		_name = contents.toString();
        	} else if (qName.equals("Value")) {
        		_value = contents.toString();
        	} else if (qName.equals("Attribute")) {
        		messageAttributes.put(_name, _value);
        	} else if (!qName.equals("Message") && !qName.contains("Response") && !qName.contains("Result") && !qName.equals("RequestId")){
        		messageMap.put(qName, contents.toString());
        	}
        }

        @Override
    	public void characters(char[] ch, int start, int length) {
    		contents.write(ch, start, length);
        }

        String getValueByKey(String k) {
        	return messageMap.get(k);
        }

        HashMap<String, String> getAttributes() {
        	return messageAttributes;
        }
    }
}
