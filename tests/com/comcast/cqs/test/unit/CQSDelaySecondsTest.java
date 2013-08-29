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
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;

public class CQSDelaySecondsTest {
	
    private static Logger logger = Logger.getLogger(CQSIntegrationTest.class);

    private AmazonSQS sqs = null;
    
    private String cqsServiceUrl = CMBProperties.getInstance().getCQSServiceUrl();

    private HashMap<String, String> attributeParams = new HashMap<String, String>();
    private User user = null;
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 
    
    private static String queueUrl;
   
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

            sqs = new AmazonSQSClient(credentialsUser);
            sqs.setEndpoint(cqsServiceUrl);
            
            queueUrl = null;
            
    		String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);

	        attributeParams.put("MessageRetentionPeriod", "600");
	        attributeParams.put("VisibilityTimeout", "30");
	        attributeParams.put("DelaySeconds", "20");
	        
	        createQueueRequest.setAttributes(attributeParams);
	        
	        queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	        
	        ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
			messagePersistence.clearQueue(queueUrl, 0);
			
			logger.info("queue " + queueUrl + "created");
	        
	        Thread.sleep(1000);
            
        } catch (Exception ex) {
            logger.error("setup failed", ex);
            fail("setup failed: "+ex);
            return;
        }
        
    }
    
    /**
     * Test queue with default delay seconds of 20
     */
    @Test
    public void testDelayQueue() {

    	try {

            sqs.sendMessage(new SendMessageRequest(queueUrl, "test message"));

            long ts1 = System.currentTimeMillis();
            
            while (true) {
            
				logger.info("checking for messages");

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				//receiveMessageRequest.setWaitTimeSeconds(20);
				
				ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);

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
			ex.printStackTrace();
		}
    }
    
    /**
     * Overwrite default delay with message delay
     */
    @Test
    public void testDelayMessage() {

    	try {

    		SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, "test message");
    		sendMessageRequest.setDelaySeconds(10);
    		
            sqs.sendMessage(sendMessageRequest);

            long ts1 = System.currentTimeMillis();
            
            while (true) {
            
				logger.info("checking for messages");

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				//receiveMessageRequest.setWaitTimeSeconds(20);
				
				ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);

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
			ex.printStackTrace();
		}
    }
    
    /**
     * Change queue attributes to turn queue into a standard non-delay queue and check functionality
     */
    @Test
    public void testNoDelay() {

    	try {
    		
    		SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest();
    		setQueueAttributesRequest.setQueueUrl(queueUrl);
	        attributeParams.put("DelaySeconds", "0");
    		setQueueAttributesRequest.setAttributes(attributeParams);

    		sqs.setQueueAttributes(setQueueAttributesRequest);
    		
            sqs.sendMessage(new SendMessageRequest(queueUrl, "test message"));

            long ts1 = System.currentTimeMillis();
            
            while (true) {
            
				logger.info("checking for messages");

				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				//receiveMessageRequest.setWaitTimeSeconds(20);
				
				ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);

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
			ex.printStackTrace();
		}
    }

    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
        
        if (queueUrl != null) {
        	sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
        }
    }    
}
