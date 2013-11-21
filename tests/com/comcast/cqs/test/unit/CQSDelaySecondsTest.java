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
import org.junit.Test;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CQSDelaySecondsTest extends CMBAWSBaseTest {
	
    private static HashMap<String, String> attributeParams = new HashMap<String, String>();
    
    static {
        attributeParams.put("MessageRetentionPeriod", "600");
        attributeParams.put("VisibilityTimeout", "20");
        //attributeParams.put("DelaySeconds", "20");
    }
    
    @Test
    public void testDelayQueue() {

    	try {
    		
    		String queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);
    		
            cqs1.sendMessage(new SendMessageRequest(queueUrl, "test message"));

            long ts1 = System.currentTimeMillis();
            
            while (true) {
            
				logger.info("checking for messages");

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				//receiveMessageRequest.setWaitTimeSeconds(20);
				
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);

				if (receiveMessageResult.getMessages().size() == 1) {
					assertTrue("Wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals("test message"));
					logger.info("message found");
					long duration = System.currentTimeMillis() - ts1;
					assertTrue("Wrong delay " + duration, duration >= 18000 && duration <= 25000);
					break;
				}
				
				if (System.currentTimeMillis() - ts1 > 25*1000) {
					fail("no message found in 25 seconds");
				}
				
				Thread.sleep(500);
            }
			
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }
    
    @Test
    public void testDelayMessage() {

    	try {

    		String queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);

    		SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, "test message");
    		sendMessageRequest.setDelaySeconds(10);
    		
            cqs1.sendMessage(sendMessageRequest);

            long ts1 = System.currentTimeMillis();
            
            while (true) {
            
				logger.info("checking for messages");

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				//receiveMessageRequest.setWaitTimeSeconds(20);
				
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);

				if (receiveMessageResult.getMessages().size() == 1) {
					assertTrue("Wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals("test message"));
					logger.info("message found");
					long duration = System.currentTimeMillis() - ts1;
					assertTrue("Wrong delay " + duration, duration >= 8000 && duration <= 15000);
					break;
				}
				
				if (System.currentTimeMillis() - ts1 > 25*1000) {
					fail("no message found in 25 seconds");
				}
				
				Thread.sleep(500);
            }
			
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }

    @Test
    public void testMultipleDelayMessage() {

    	try {

    		String queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);
            long ts1 = System.currentTimeMillis();

    		for (int i=0; i<1000; i++) {
    			logger.info("event=sending_message no=" + i);
    			SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, "test message " + i);
	    		sendMessageRequest.setDelaySeconds(30);
	            cqs1.sendMessage(sendMessageRequest);
    		}

            int count = 0;
            
            while (true) {
            
				logger.info("event=scanning_for_delayed_messages found_so_far=" + count + " seconds_passed=" + ((System.currentTimeMillis()-ts1)/1000));

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(10);
				receiveMessageRequest.setWaitTimeSeconds(1);
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);

				count += receiveMessageResult.getMessages().size();
				
				if (count >=1000) {
					break;
				}

				if (System.currentTimeMillis() - ts1 > 120*1000) {
					fail("only " + count + " messages found in 120 seconds");
				}
            }

            ts1 = System.currentTimeMillis();
            count = 0;
            
            while (true) {
                
				logger.info("event=scanning_for_revisible_messages found_so_far=" + count + " seconds_passed=" + ((System.currentTimeMillis()-ts1)/1000));

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(10);
				receiveMessageRequest.setWaitTimeSeconds(1);
				receiveMessageRequest.setVisibilityTimeout(20);
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);

				count += receiveMessageResult.getMessages().size();
				
				for (Message msg : receiveMessageResult.getMessages()) {
					ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest(queueUrl, msg.getReceiptHandle(), 23);
					cqs1.changeMessageVisibility(changeMessageVisibilityRequest);
				}
				
				if (count >=1000) {
					break;
				}

				if (System.currentTimeMillis() - ts1 > 120*1000) {
					fail("only " + count + " messages found in 120 seconds");
				}
            }
            
            ts1 = System.currentTimeMillis();
            count = 0;

            while (true) {
                
				logger.info("event=deleting_newly_revisible_messages found_so_far=" + count + " seconds_passed=" + ((System.currentTimeMillis()-ts1)/1000));

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(10);
				receiveMessageRequest.setWaitTimeSeconds(1);
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);

				count += receiveMessageResult.getMessages().size();
				
				for (Message msg : receiveMessageResult.getMessages()) {
					DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, msg.getReceiptHandle());
					cqs1.deleteMessage(deleteMessageRequest);
				}

				if (count >=1000) {
					break;
				}

				if (System.currentTimeMillis() - ts1 > 120*1000) {
					fail("only " + count + " messages found in 120 seconds");
				}
            }
			
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }    
    
    /**
     * Change queue attributes to turn queue into a standard non-delay queue and check functionality
     */
    @Test
    public void testNoDelay() {

    	try {
    		
    		String queueUrl = getQueueUrl(1, USR.USER1);
    		cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributeParams));

    		Thread.sleep(1000);

    		SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest();
    		setQueueAttributesRequest.setQueueUrl(queueUrl);
	        attributeParams.put("DelaySeconds", "0");
    		setQueueAttributesRequest.setAttributes(attributeParams);

    		cqs1.setQueueAttributes(setQueueAttributesRequest);
    		
            cqs1.sendMessage(new SendMessageRequest(queueUrl, "test message"));

            long ts1 = System.currentTimeMillis();
            
            while (true) {
            
				logger.info("checking for messages");

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				//receiveMessageRequest.setWaitTimeSeconds(20);
				
				ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);

				if (receiveMessageResult.getMessages().size() == 1) {
					assertTrue("Wrong message content", receiveMessageResult.getMessages().get(0).getBody().equals("test message"));
					logger.info("message found");
					long duration = System.currentTimeMillis() - ts1;
					assertTrue("Wrong delay " + duration, duration >= 0 && duration <= 2000);
					break;
				}
				
				if (System.currentTimeMillis() - ts1 > 25*1000) {
					fail("no message found in 25 seconds");
				}
				
				Thread.sleep(500);
            }
			
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }
}
