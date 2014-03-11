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
package com.comcast.cqs.test.unit;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CQSLongPollTest extends CMBAWSBaseTest {
	
    private static HashMap<String, String> attributeParams = new HashMap<String, String>();
    private static Map<String, String> messageMap;
    private static String queueUrl;
    
    static {
        attributeParams.put("MessageRetentionPeriod", "600");
        attributeParams.put("VisibilityTimeout", "30");
    }
    
    private class MessageSender extends Thread {
    	public void run() {
    		try {
				logger.info("sender sleeping for 5 sec");
    			sleep(5000);
	            cqs1.sendMessage(new SendMessageRequest(queueUrl, "test message"));
	            logger.info("test message sent");
			} catch (Exception ex) {
				ex.printStackTrace();
			}
    	}
    }
    
    /**
     * Simple functional test: Call receive() with 20 sec TO, then 5 sec later call send()
     * and check if message is received exactly once after around 5 sec.
     */
    private void testLongPoll(AmazonSQS receiverSqs) {
    	
    	messageMap = new HashMap<String, String>();

    	try {
    		
    		queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(1);
			receiveMessageRequest.setWaitTimeSeconds(20);
			
			long start = System.currentTimeMillis();
			
			(new MessageSender()).start();
			
			logger.info("calling receive message");
			
			ReceiveMessageResult receiveMessageResult = receiverSqs.receiveMessage(receiveMessageRequest);
			
			logger.info("receive message returns");
			
			long end = System.currentTimeMillis();
			
			assertTrue("No message received", receiveMessageResult.getMessages().size() == 1);
			
			assertTrue("Wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals("test message"));
			
			assertTrue("Message came back too fast: " + (end-start) + " ms", end-start >= 4750);
			
			assertTrue("Message came back too slow: " + (end-start) + " ms", end-start <= 5250);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			fail("failed long poll test: " + ex.getMessage());
		}
    }

    /**
     * Simple functional test: Call receive() on a queue with a default 15 sec wait time, then 5 sec later call 
     * send() and check if message is received exactly once after around 5 sec.
     */
    @Test
    public void testLongPollQueue() {
    	
    	messageMap = new HashMap<String, String>();

    	try {
            
    		queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);
    		
    		SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest();
    		setQueueAttributesRequest.setQueueUrl(queueUrl);
            attributeParams.put("ReceiveMessageWaitTimeSeconds", "15");
    		setQueueAttributesRequest.setAttributes(attributeParams);

    		cqs1.setQueueAttributes(setQueueAttributesRequest);

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(1);
			
			long start = System.currentTimeMillis();
			
			(new MessageSender()).start();
			
			logger.info("calling receive message");
			
			// note: we are calling receivemessage without waittime set and yet we should see long poll
			// behavior because the queue has a default wait time set
			
			ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
			
			logger.info("receive message returns");
			
			long end = System.currentTimeMillis();
			
			assertTrue("No message received", receiveMessageResult.getMessages().size() == 1);
			
			assertTrue("Wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals("test message"));
			
			assertTrue("Message came back too fast: " + (end-start) + " ms", end-start >= 4750);
			
			assertTrue("Message came back too slow: " + (end-start) + " ms", end-start <= 5250);
	        
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }

    @Test
    public void testLongPoll() {
    	testLongPoll(cqs1);
    }
    
    @Test
    public void testLongPollWithLoadBalancer() {
    	
    	if (this.cqsAlt == null) {
    		logger.info("skipping load balanced long poll test due to missing alternate sqs service url");
    		return;
    	}
    	
    	testLongPoll(cqsAlt);
    }
    
    /**
     * Simple functional test: Like testLongPoll above but in this test send() happens before
     * receive(). Here we ensure that the message is received immediately despite a long poll
     * timeout of 20 sec.
     */
    @Test
    public void testLongPollNoDelay() {

    	messageMap = new HashMap<String, String>();

    	try {
    		
    		queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(1);
			receiveMessageRequest.setWaitTimeSeconds(20);
			
			long start = System.currentTimeMillis();
			
            cqs1.sendMessage(new SendMessageRequest(queueUrl, "test message"));
			
			logger.info("calling receive message");
			
			ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
			
			logger.info("receive message returns");
			
			long end = System.currentTimeMillis();
			
			assertTrue("No message received", receiveMessageResult.getMessages().size() == 1);
			
			assertTrue("Wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals("test message"));
			
			assertTrue("Message came back too slow: " + (end-start) + " ms", end-start <= 250);
	        
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }

    /**
     * Test if long poll calls timeout after desired periods of wait time (e.g. after 1, 5, 20 sec) and if
     * the normal empty response comes back indicating no messages available.
     */
    private void testLongPollTimeout(int timeoutSecs) {
    	
    	messageMap = new HashMap<String, String>();

    	try {
    		
    		queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(1);
			receiveMessageRequest.setWaitTimeSeconds(timeoutSecs);
			
			long start = System.currentTimeMillis();
			
			logger.info("calling receive message");
			
			ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
			
			logger.info("receive message returns");
			
			long end = System.currentTimeMillis();
			
			assertTrue("Unexpected message received", receiveMessageResult.getMessages().size() == 0);
			
			assertTrue("Receive came back too fast: " + (end-start) + " ms", end-start >= timeoutSecs*1000-100);
			
			assertTrue("Receive came back too slow: " + (end-start) + " ms", end-start <= timeoutSecs*1000+100);
	        
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }
    
    @Test
    public void testLongPollTimeout20() {
    	testLongPollTimeout(20);
    }
    
    @Test
    public void testLongPollTimeout5() {
    	testLongPollTimeout(5);
    }

    @Test
    public void testLongPollTimeout1() {
    	testLongPollTimeout(1);
    }

    private class MultiMessageSender extends Thread {

    	int count;
    	int delay;
    	
    	public MultiMessageSender(int count, int delay) {
    		this.count = count;
    		this.delay = delay;
    	}
    	
    	public void run() {
    		try {
    			Thread.sleep(delay);
    			for (int i=0; i<count; i++) {
    				cqs1.sendMessage(new SendMessageRequest(queueUrl, "test message " + i));
    			}
			} catch (Exception ex) {
				logger.error("error", ex);
			}
    	}
    }
    
    /** 
     * Single-threaded load test, with one thread sending 5000 messages and another thread receiving
     * messages. Test verifies that 5000 unique messages are received with no duplicates (all 5000
     * messages have different content to spot duplicates). Test can be benchmarked against sending 
     * and receiving 5000 messages without using the long poll feature (WaitTime parameter not set).
     */
    
    private void testLongPollLoad(int timeoutSecs) {
    	
    	messageMap = new HashMap<String, String>();
    	
    	// set timeoutSecs to 0 to test traditional polling receives (mainly for benchmarking)
    	
    	try {

    		queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		(new MultiMessageSender(5000,0)).start();

	        int messageCounter = 0;
	        
	        long begin = System.currentTimeMillis();
	        
	        while (messageCounter < 5000) {
	        	
		        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				
				if (timeoutSecs > 0) {
					receiveMessageRequest.setWaitTimeSeconds(timeoutSecs);
				}
				
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
				messageCounter += receiveMessageResult.getMessages().size();
				
				if (receiveMessageResult.getMessages().size() == 1) {
					messageMap.put(receiveMessageResult.getMessages().get(0).getBody(), "");
					
					DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
					deleteMessageRequest.setQueueUrl(queueUrl);
					deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
					cqs1.deleteMessage(deleteMessageRequest);
				}
	        }
	        
	        long end = System.currentTimeMillis();
	        
	        logger.info("duration=" + (end-begin));
	        
	        assertTrue("wrong number of messages: " + messageMap.size(), messageMap.size() == 5000);
	        
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }

    @Test
    public void testLongPollLoadTO20() {
    	testLongPollLoad(20);
    }    
    
    @Test
    public void testLongPollLoadTO0() {
    	testLongPollLoad(0);
    }   
    
    /**
     * Functional test to check if correct errors are produced for invalid parameters. So far we only
     * test WaitSeconds > 20. Other tests could include WaitSeconds < 1 or WaitSeconds not an integer.
     */
    @Test
    public void testInvalidParameters() {
    	
    	messageMap = new HashMap<String, String>();

    	try {
    		
    		queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(1);
			
			// timeout > 20 sec, should fail
			
			receiveMessageRequest.setWaitTimeSeconds(21);
			
			boolean failed = false;
			
			try {
				cqs1.receiveMessage(receiveMessageRequest);
			} catch (AmazonServiceException ex) {
				assertTrue("Wrong error message: " + ex.getErrorCode(), ex.getErrorCode().equals("InvalidParameterValue"));
				failed = true;
			}
			
			assertTrue("Didn't fail", failed);
			
			receiveMessageRequest.setWaitTimeSeconds(-1);
			
			failed = false;
			
			try {
				cqs1.receiveMessage(receiveMessageRequest);
			} catch (AmazonServiceException ex) {
				assertTrue("Wrong error message: " + ex.getErrorCode(), ex.getErrorCode().equals("InvalidParameterValue"));
				failed = true;
			}
			
			assertTrue("Didn't fail", failed);
	        
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }
    
    private class MessageReceiver extends Thread {
    	
    	public void run() {
    		
    		try {
    	    
    			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
    			receiveMessageRequest.setQueueUrl(queueUrl);
    			receiveMessageRequest.setMaxNumberOfMessages(1);
    			receiveMessageRequest.setWaitTimeSeconds(20);
    			
    			ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
    			
    			if (receiveMessageResult.getMessages().size() == 1) {
    				
    				messageMap.put(receiveMessageResult.getMessages().get(0).getBody(), "");
    			
	    			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
	    			deleteMessageRequest.setQueueUrl(queueUrl);
	    			deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
	    			cqs1.deleteMessage(deleteMessageRequest);
    			}
    			
			} catch (Exception ex) {
				logger.error("test failed", ex);
				fail(ex.getMessage());
			}
    	}
    }
    
    /**
     * Multi-threaded load test. This test launches 25 concurrent message receivers each of them consecutively
     * calling receive() with a TO of 20 sec. After a dealy of 2 sec a single threaded messages sender starts 
     * sending 25 seconds. Test verifies that 25 unique messages are received. 
     */
    private void testConcurrentLPRequests(AmazonSQS senderSqs) {
    	
    	messageMap = new HashMap<String, String>();

    	try {
    		
    		queueUrl = getQueueUrl(1, USR.USER1);

	        // apparently there is a limit of 50 concurrent operations in aws sdk 
	        
	        int numMessages = 25;
	        
	        for (int i=0; i<numMessages; i++) {
	        	new MessageReceiver().start();
	        }
	        
	        Thread.sleep(2000);
	        
	    	for (int i=0; i<numMessages; i++) {
	    		senderSqs.sendMessage(new SendMessageRequest(queueUrl, "test message " + i));
			}
	    	
	    	Thread.sleep(1000);
	    	
	    	assertTrue("Wrong number of messages: " + messageMap.size(), messageMap.size() == numMessages);
    	
    	} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }
    
    @Test
    public void testConcurrentLPRequests() {
    	testConcurrentLPRequests(cqs1);
    }

    @Test
    public void testConcurrentLPRequestsLoadBalancer() {

    	if (cqsAlt == null) {
    		logger.info("skipping concurrent load balanced long poll test due to missing alternate sqs service url");
    		return;
    	}
    	
    	testConcurrentLPRequests(cqsAlt);
    }
}
