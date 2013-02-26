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
import java.util.concurrent.ThreadPoolExecutor;
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

        	//BasicAWSCredentials credentialsUser = new BasicAWSCredentials("WOA5HC1GEDQYXF8LZYB1", "FwpY/FvhZvMjrlM45SKpkLowoZm8HV6SVnkkljRf");
        	
        	sqs = new AmazonSQSClient(credentialsUser);
            
            //sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());
            sqs.setEndpoint("http://localhost:7070/");
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
    	CreateNQueues(1);
    }

    @Test
    public void Create10Queues() {
    	CreateNQueues(10);
    }

    @Test
    public void Create100Queues() {
    	CreateNQueues(100);
    }

    @Test
    public void Create1000Queues() {
    	CreateNQueues(1000);
    }

    @Test
    public void Create10000Queues() {
    	CreateNQueues(10000);
    }
    
    @Test
    public void Create100000Queues() {
    	CreateNQueues(100000);
    }

    @Test
    public void CreateQueuesConcurrent() {
    	
    	ScheduledThreadPoolExecutor ep = new ScheduledThreadPoolExecutor(10);
    	
    	ep.submit((new Runnable() { public void run() { CreateNQueues(10); }}));
    	ep.submit((new Runnable() { public void run() { CreateNQueues(10); }}));
    	ep.submit((new Runnable() { public void run() { CreateNQueues(10); }}));
    	ep.submit((new Runnable() { public void run() { CreateNQueues(10); }}));
    	
    	logger.info("ALL TEST LAUNCHED");

    	try {
			ep.shutdown();
    		ep.awaitTermination(60, TimeUnit.MINUTES);
		} catch (InterruptedException ex) {
			logger.error("fail", ex);
			fail(ex.getMessage());
		}

    	logger.info("ALL TEST FINISHED");
    	
    	for (String message : report) {
    		logger.info(message);
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

    private void CreateNQueues(long n) {
    	
        List<String> queueUrls = new ArrayList<String>();
        Map<String, String> messageMap = new HashMap<String, String>();

    	try {
    		
    		long counter = 0;
    		long createFailures = 0;
    		long totalTime = 0;
    		
    		for (int i=0; i<n; i++) {
    			
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
    		
    		for (int i=0; i<100; i++) {
    			
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
    		
    		for (int i=0; i<110; i++) {
    			
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
    		
    		String message = "create failuers: " + createFailures +  " delete failures: " + deleteFailures + " send failures: " + sendFailures + " read failures: " + readFailures + " empty reads: " + emptyResponses + " messages found: " + messagesFound + " messages sent: " + messagesSent + " out of order: " + outOfOrder + " duplicates: " + duplicates + " distinct messages: " + messageMap.size(); 
    		
    		logger.info(message);
    		report.add(new Date() + ": " + message);
    		
    		assertTrue("Create failures: " + createFailures, createFailures == 0);
    		assertTrue("Delete failures: " + deleteFailures, deleteFailures == 0);
    		assertTrue("Send failures: " + sendFailures, sendFailures == 0);
    		assertTrue("Read failures: " + readFailures, readFailures == 0);
    		//assertTrue("Empty reads: " + emptyResponses, emptyResponses == 0);
    		assertTrue("Wrong number of messages found!", messagesFound == messagesSent);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		}
    }
}
