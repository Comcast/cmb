package com.comcast.cqs.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

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
    
    private static List<String> queueUrls;

    @Before
    public void setup() throws Exception {
    	
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
        
        queueUrls = new ArrayList<String>();
        
        try {
        	
            IUserPersistence userPersistence = new UserCassandraPersistence();
 
            user = userPersistence.getUserByName("cqs_unit_test");

            if (user == null) {
                user = userPersistence.createUser("cqs_unit_test", "cqs_unit_test");
            }

            BasicAWSCredentials credentialsUser = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());

            sqs = new AmazonSQSClient(credentialsUser);
            sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());
            
        } catch (Exception ex) {
            logger.error("setup failed", ex);
            fail("setup failed: "+ex);
            return;
        }
        
        attributeParams.put("MessageRetentionPeriod", "600");
        attributeParams.put("VisibilityTimeout", "30");
    }
    
    @Test
    public void Create1000Queues() {
    	CreateNQueues(1000);
    }

    //@Test
    public void Create10000Queues() {
    	CreateNQueues(10000);
    }
    
    //@Test
    public void Create100000Queues() {
    	CreateNQueues(100000);
    }

    private boolean receiveMessage(String queueUrl) {

    	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
		receiveMessageRequest.setQueueUrl(queueUrl);
		receiveMessageRequest.setMaxNumberOfMessages(1);

		ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
		
		if (receiveMessageResult.getMessages().size() == 1) {
		
	    	DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
			deleteMessageRequest.setQueueUrl(queueUrl);
			deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
			sqs.deleteMessage(deleteMessageRequest);
		
			return true;
		}
		
		return false;
    }

    private void CreateNQueues(long n) {

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
	    	        
	    	        logger.info("average creation millis: " + (totalTime/(i+1)));
	    	        
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
    		counter = 0;
    		
    		for (String queueUrl : queueUrls) {
	            try {
	    			sqs.sendMessage(new SendMessageRequest(queueUrl, "test message"));
	            	logger.info("sent message on queue " + counter + ": " + queueUrl);
	            	counter++;
	            } catch (Exception ex) {
    				logger.error("send failure", ex);
    				sendFailures++;
	            }
    		}
    		
    		Thread.sleep(1000);

    		long readFailures = 0;
    		long emptyResponses = 0;
    		counter = 0;
    		
    		for (String queueUrl : queueUrls) {

    			boolean messageReceived = false;
    			
    			try {
    				
    				messageReceived = receiveMessage(queueUrl);
    				
    				if (messageReceived) {
	    				logger.info("received message on queue " + counter + ":" + queueUrl);
	    				counter++;
    				} else {
	    				logger.info("no message found on queue " + counter + ":"  + queueUrl);
	    				emptyResponses++;
    				}
					
    			} catch (Exception ex) {
    				
    				logger.error("read failure, will retry: " + queueUrl, ex);
    				readFailures++;
    			}
    		}

    		Thread.sleep(1000);
    		
    		long deleteFailures = 0;
    		counter = 0;
    		
    		for (String queueUrl : queueUrls) {
    			try {
	            	sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
	            	logger.info("deleted queue " + counter + ": " + queueUrl);
	            	counter++;
    			} catch (Exception ex) {
    				logger.error("delete failure", ex);
    				deleteFailures++;
    			}
    		}
    		
    		logger.info("create failuers: " + createFailures +  " delete failures: " + deleteFailures + " send failures: " + sendFailures + " read failures: " + readFailures + " empty reads: " + emptyResponses);
    		
    		assertTrue("Create failures: " + createFailures, createFailures == 0);
    		assertTrue("Delete failures: " + deleteFailures, deleteFailures == 0);
    		assertTrue("Send failures: " + sendFailures, sendFailures == 0);
    		assertTrue("Read failures: " + readFailures, readFailures == 0);
    		assertTrue("Empty reads: " + emptyResponses, emptyResponses == 0);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		}
    }
}
