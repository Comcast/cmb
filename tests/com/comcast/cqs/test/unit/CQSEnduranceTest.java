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
 */package com.comcast.cqs.test.unit;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CQSEnduranceTest extends CMBAWSBaseTest {
	
	private static int sendDelay = 10;
	private static int receiveDelay = 0;
	private static int monitorDelay = 5000;
	private static int numSenders = 1;
	private static int numReceivers = 1;
	private static int maxQueueDepth = 1000;

	private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static String queueUrl;
	private static AtomicLong sendSuccessCount = new AtomicLong(0);
	private static AtomicLong sendFailureCount = new AtomicLong(0);
	private static AtomicLong receiveSuccessCount = new AtomicLong(0);
	private static AtomicLong receiveFailureCount = new AtomicLong(0);
	private static AtomicLong actualMessagesReceivedCount = new AtomicLong(0);
	private static AtomicLong deleteSuccessCount = new AtomicLong(0);
	private static AtomicLong deleteFailureCount = new AtomicLong(0);
	private static AtomicLong attribSuccessCount = new AtomicLong(0);
	private static AtomicLong attribFailureCount = new AtomicLong(0);

	private static String generateRandomMessage(int length) {
		StringBuilder sb = new StringBuilder(length);
		for (int i=0; i<length; i++) {
			sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
		}
		return sb.toString();
	}
	
	public void start() throws Exception {
		setup();
		queueUrl = this.getQueueUrl(1, USR.USER1);
		ScheduledThreadPoolExecutor ep = new ScheduledThreadPoolExecutor(100);
		logger.info("event=launching_monitor");
		ep.submit(new MonitorDaemon());
		for (int i=0; i<numReceivers; i++) {
			logger.info("event=launching_receiver");
			ep.submit(new MessageReceiver());
		}
		for (int i=0; i<numSenders; i++) {
			logger.info("event=launching_sender");
			ep.submit(new MessageSender());
		}
		try {
			ep.shutdown();
			ep.awaitTermination(100, TimeUnit.DAYS);
		} catch (InterruptedException ex) {
			logger.error("event=endurance_failure queue_url=" + queueUrl, ex);
		}
		logger.info("event=done");
	}

	public static void main(String [ ] args) throws Exception {
		new CQSEnduranceTest().start();
	}
	
    private static class MonitorDaemon implements Runnable {

		@Override
		public void run() {
			
			Thread.currentThread().setName("MONITOR_" + rand.nextInt(100));
			
			while (true) {
				try {
					Thread.currentThread().sleep(monitorDelay);
					long start = System.currentTimeMillis();
					GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(queueUrl);
					Set<String> attribs = new TreeSet<String>();
					attribs.add("All");
					getQueueAttributesRequest.setAttributeNames(attribs);
					GetQueueAttributesResult result = cqs1.getQueueAttributes(getQueueAttributesRequest);
					int numMsg = Integer.valueOf(result.getAttributes().get("ApproximateNumberOfMessages"));
					int numInvisible = Integer.valueOf(result.getAttributes().get("ApproximateNumberOfMessagesNotVisible"));
					if (numMsg > maxQueueDepth || numInvisible > maxQueueDepth) {
						if (sendDelay == 10) {
							sendDelay = 1000;
							logger.warn("event=slowing_send queueUrl=" + queueUrl + " queue_depth=" + numMsg + " num_invisible=" + numInvisible);
						}
					} else {
						sendDelay = 10;
					}
					long end = System.currentTimeMillis();
					attribSuccessCount.incrementAndGet();
					logger.info("event=get_attributes rt="+(end-start)+" queue_url=" + queueUrl + " success_count=" + attribSuccessCount.get() + " failure_count=" + attribFailureCount.get() + " queue_depth=" + numMsg + " num_invisible=" + numInvisible);
				} catch (Exception ex) {
					attribFailureCount.incrementAndGet();
					logger.error("event=get_attributes queue_url=" + queueUrl + " success_count=" + attribSuccessCount.get() + " failure_count=" + attribFailureCount.get(), ex);
				}
			}
		}
    }
    
    private static class MessageSender implements Runnable {

		@Override
		public void run() {
			
			Thread.currentThread().setName("MESSAGE_SENDER_" + rand.nextInt(100));
			
			while (true) {
				try {
					Thread.currentThread().sleep(sendDelay);
					long start = System.currentTimeMillis();
					String body = generateRandomMessage(20000);
					SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, body);
					SendMessageResult result = cqs1.sendMessage(sendMessageRequest);
					long end = System.currentTimeMillis();
					sendSuccessCount.incrementAndGet();
					logger.info("event=message_sent rt="+(end-start)+" queue_url=" + queueUrl + " success_count=" + sendSuccessCount.get() + " failure_count=" + sendFailureCount.get());
				} catch (Exception ex) {
					sendFailureCount.incrementAndGet();
					logger.error("event=send_error queue_url=" + queueUrl + " success_count=" + sendSuccessCount.get() + " failure_count=" + sendFailureCount.get(), ex);
				}
			}
		}
    }
	
    private static class MessageReceiver implements Runnable {
    	
    	private void deleteMessage(String receiptHandle) {
			try {
				long start = System.currentTimeMillis();
				DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
				deleteMessageRequest.setQueueUrl(queueUrl);
				deleteMessageRequest.setReceiptHandle(receiptHandle);
				cqs1.deleteMessage(deleteMessageRequest);
				long end = System.currentTimeMillis();
				deleteSuccessCount.incrementAndGet();
				logger.info("event=delete rt="+(end-start)+" queue_url=" + queueUrl + " receipt_handle=" + receiptHandle + " success_count=" + deleteSuccessCount.get() + " failure_count=" + deleteFailureCount);
			} catch (Exception ex) {
				deleteFailureCount.incrementAndGet();
				logger.error("event=send_error queue_url=" + queueUrl + " receipt_handle=" + receiptHandle + " success_count=" + deleteSuccessCount.get() + " failure_count=" + deleteFailureCount, ex);
			}
    	}

		@Override
		public void run() {
			
			Thread.currentThread().setName("MESSAGE_RECEIVER_" + rand.nextInt(100));
			
			while (true) {
				try {
					Thread.currentThread().sleep(receiveDelay);
					long start = System.currentTimeMillis();
					ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
					receiveMessageRequest.setQueueUrl(queueUrl);
					receiveMessageRequest.setMaxNumberOfMessages(10);
					receiveMessageRequest.setWaitTimeSeconds(20);
					ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
					long end = System.currentTimeMillis();
					receiveSuccessCount.incrementAndGet();
					actualMessagesReceivedCount.addAndGet(receiveMessageResult.getMessages().size());
					if (receiveMessageResult.getMessages().size() > 0) {
						logger.info("event=messages_received rt="+(end-start)+" queue_url=" + queueUrl + " batch_count=" + receiveMessageResult.getMessages().size() + " success_count" + receiveSuccessCount.get() + " failure_count=" + receiveFailureCount.get() + " total_count=" + actualMessagesReceivedCount.get());
						for (Message msg : receiveMessageResult.getMessages()) {
							deleteMessage(msg.getReceiptHandle());
						}
					} else {
						logger.info("event=empty_receive rt="+(end-start)+" queue_url=" + queueUrl + " batch_count=" + receiveMessageResult.getMessages().size());
					}
				} catch (Exception ex) {
					receiveFailureCount.incrementAndGet();
					logger.error("event=send_error queue_url=" + queueUrl + " success_count" + receiveSuccessCount.get() + " failure_count=" + receiveFailureCount.get() + " total_count=" + actualMessagesReceivedCount.get(), ex);
				}
			}
		}
    }
}
