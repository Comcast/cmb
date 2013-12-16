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

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CQSDCFailoverTest extends CMBAWSBaseTest {
	
    private AmazonSQSClient currentSqs = null;
    private static String queueUrl;
    private static Map<String, String> messageMap;
    
    @Before
    public void setup() throws Exception {
    	
    	super.setup();
    	
    	PersistenceFactory.reset();
        messageMap = new HashMap<String, String>();
        
        try {
            currentSqs = cqs1;
	        queueUrl = getQueueUrl(1, USR.USER1);
	        Thread.sleep(1000);
            
        } catch (Exception ex) {
            logger.error("setup failed", ex);
            fail("setup failed: "+ex);
            return;
        }
    }
    
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
        
        if (queueUrl != null) {
        	currentSqs.deleteQueue(new DeleteQueueRequest(queueUrl));
        }
    } 
    
    private class MultiMessageSender extends Thread {

    	private int count;
    	
    	public MultiMessageSender(int count) {
    		this.count = count;
    	}
    	
    	public void run() {
    		try {
    			logger.info("starting sending messages");
    			for (int i=0; i<count; i++) {
					currentSqs.sendMessage(new SendMessageRequest(queueUrl, "test message " + i));
    			}
			} catch (Exception ex) {
				logger.error("error", ex);
			}
    	}
    }
    
    /**
     * Test simulating a DC-failover while sending a total of 5000 messages into a single queue. This 
     * test requires that sqs and alternateSqs point to two separate api servers using two separate redis
     * instances but using a shared cassandra ring.
     */
    @Test
    public void testDCFailover() {
    	
    	if (cqsAlt == null) {
    		logger.info("skipping dc failover test due to missing alternate sqs service url");
    		return;
    	}

    	try {

    		int NUM_MESSAGES = 1000;
    		
	        (new MultiMessageSender(NUM_MESSAGES)).start();

	        Thread.sleep(2000);
	        
	        int messageCounter = 0;

	        long begin = System.currentTimeMillis();
			boolean messageFound = false;

        	logger.info("starting receiving messages");
        	
        	int readFailures = 0;

        	while (messageCounter < NUM_MESSAGES || messageFound) {
	        	
	        	try {
        		
		        	// flip to second data center half-way through the test
		        	
		        	if (messageCounter >= NUM_MESSAGES/2 && currentSqs == cqs1) {
		        		currentSqs = cqsAlt;
		        		logger.info("switching to secondary data center");
		        	}
		        	
			        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
					receiveMessageRequest.setQueueUrl(queueUrl);
					receiveMessageRequest.setMaxNumberOfMessages(1);
					
					ReceiveMessageResult receiveMessageResult = currentSqs.receiveMessage(receiveMessageRequest);
					
					if (receiveMessageResult.getMessages().size() == 1) {
					
						messageCounter += receiveMessageResult.getMessages().size();
						messageMap.put(receiveMessageResult.getMessages().get(0).getBody(), "");
						
						DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
						deleteMessageRequest.setQueueUrl(queueUrl);
						deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
						currentSqs.deleteMessage(deleteMessageRequest);
						
						messageFound = true;
					
						logger.info("receiving message " + messageCounter);
					
					} else {
						messageFound = false;
					}
				
	        	} catch (Exception ex) {
	        		logger.error("read failure", ex);
	        		readFailures++;
	        	}
	        }
	        
	        long end = System.currentTimeMillis();
	        
	        logger.info("duration=" + (end-begin));
	        
	        assertTrue("wrong number of messages: " + messageMap.size(), messageMap.size() == NUM_MESSAGES);
	        assertTrue("read failures: " + readFailures, readFailures == 0);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
		}
    }
}
