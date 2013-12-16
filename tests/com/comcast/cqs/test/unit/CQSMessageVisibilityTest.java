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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CQSMessageVisibilityTest extends CMBAWSBaseTest {
											  	
	private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static Random rand = new Random();
    private static String message = generateRandomMessage(1024);
    private static String queueUrl;
    
	private static ScheduledThreadPoolExecutor ep = new ScheduledThreadPoolExecutor(10);
	
	private static String generateRandomMessage(int length) {
		StringBuilder sb = new StringBuilder(length);
		for (int i=0; i<length; i++) {
			sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
		}
		return sb.toString();
	}
	
	@Test
	public void messageVisibilityTest() {
		try {
    		queueUrl = getQueueUrl(1, USR.USER1);
    		Thread.sleep(1000);
			ep.submit(new MessageSender());
			//give initial thread time to launch more thread
			Thread.sleep(5*1000);
			ep.shutdown();
			ep.awaitTermination(60, TimeUnit.MINUTES);
		} catch (InterruptedException ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}
	
	private static class MessageSender extends Thread {
		
		@Override
		public void run() {
			
			logger.info("starting sender");
			
			try {

	    		SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, message);
	    		cqs1.sendMessage(sendMessageRequest);

	    		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				// use long poll style requests
				receiveMessageRequest.setWaitTimeSeconds(20);
				// set initial vto to 20 sec
				receiveMessageRequest.setVisibilityTimeout(20);
				
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);

				if (receiveMessageResult.getMessages().size() == 1) {
					
					assertTrue("wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals(message));
					logger.info("message found for first time");
					
					ep.submit(new MessageReceiver(50));
					
					// push message out 11 sec
					
					logger.info("sleeping for 19 sec");
					Thread.sleep(19*1000);
					ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
					changeMessageVisibilityRequest.setVisibilityTimeout(11);
					changeMessageVisibilityRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
					changeMessageVisibilityRequest.setQueueUrl(queueUrl);
					cqs1.changeMessageVisibility(changeMessageVisibilityRequest);
					logger.info("changed visibility timeout to 11 sec");
					
					// push message out 10 sec
					
					logger.info("sleeping for 10 sec");
					Thread.sleep(10*1000);
					changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
					changeMessageVisibilityRequest.setVisibilityTimeout(10);
					changeMessageVisibilityRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
					changeMessageVisibilityRequest.setQueueUrl(queueUrl);
					cqs1.changeMessageVisibility(changeMessageVisibilityRequest);
					logger.info("changed visibility timeout to 10 sec");
					
					// push message out 11 sec
					
					logger.info("sleeping for 9 sec");
					Thread.sleep(9*1000);
					changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
					changeMessageVisibilityRequest.setVisibilityTimeout(11);
					changeMessageVisibilityRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
					changeMessageVisibilityRequest.setQueueUrl(queueUrl);
					cqs1.changeMessageVisibility(changeMessageVisibilityRequest);
					logger.info("changed visibility timeout to 11 sec");
					
				} else {
					fail ("no message found");
				}
			
			} catch (Exception ex) {
				logger.error("test failed", ex);
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
					
					ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);

					if (receiveMessageResult.getMessages().size() == 1) {
						assertTrue("wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals(message));
						long durationSec = (System.currentTimeMillis() - start)/1000;
						logger.info("message found again after " + durationSec + " sec");
						DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
						deleteMessageRequest.setQueueUrl(queueUrl);
						deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
						cqs1.deleteMessage(deleteMessageRequest);
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
				logger.error("test failed", ex);
				fail(ex.getMessage());
			}
		}
	}
}
