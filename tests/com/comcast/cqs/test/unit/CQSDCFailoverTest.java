package com.comcast.cqs.test.unit;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

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
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;

public class CQSDCFailoverTest {
	
    private static Logger logger = Logger.getLogger(CQSIntegrationTest.class);

    private AmazonSQS sqs = null;
    
    // alternateSqs is referring to a separate cqs api server using a separate redis service but the same
    // cassandra ring to simulate fail-over across data center boundaries
    
    //todo: put url of alternate cqs service here
    
    private String alternateCqsServiceUrl = "http://sqs-test5.plaxo.com:6060/";
    private AmazonSQS alternateSqs = null;
    
    private HashMap<String, String> attributeParams = new HashMap<String, String>();
    private User user = null;
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 
    
    private static String queueUrl;
    private static Map messageMap;
    
	public static volatile boolean useAlternateSqs = false;

    @Before
    public void setup() throws Exception {
    	
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
        
        messageMap = new HashMap<String, String>();
        
        try {
        	
            IUserPersistence userPersistence = new UserCassandraPersistence();
 
            user = userPersistence.getUserByName("cqs_unit_test");

            if (user == null) {
                user = userPersistence.createUser("cqs_unit_test", "cqs_unit_test");
            }

            BasicAWSCredentials credentialsUser = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());

            sqs = new AmazonSQSClient(credentialsUser);
            sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());
            
            if (alternateCqsServiceUrl != null) {
            	alternateSqs = new AmazonSQSClient(credentialsUser);
            	alternateSqs.setEndpoint(alternateCqsServiceUrl);
            }
            
            queueUrl = null;
            
    		String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
	        createQueueRequest.setAttributes(attributeParams);
	        
	        queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	        
	        ICQSMessagePersistence messagePersistence = RedisCachedCassandraPersistence.getInstance();
			messagePersistence.clearQueue(queueUrl);
			
			logger.info("queue " + queueUrl + "created");
	        
	        Thread.sleep(1000);
            
        } catch (Exception ex) {
            logger.error("setup failed", ex);
            fail("setup failed: "+ex);
            return;
        }
        
        attributeParams.put("MessageRetentionPeriod", "600");
        attributeParams.put("VisibilityTimeout", "30");
    }
    
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
        
        if (queueUrl != null) {
        	sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
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
    				if (useAlternateSqs) {
    					alternateSqs.sendMessage(new SendMessageRequest(queueUrl, "test message " + i));
    				} else {
    					sqs.sendMessage(new SendMessageRequest(queueUrl, "test message " + i));
    				}
    			}
			} catch (Exception ex) {
				logger.error("error", ex);
			}
    	}
    }
    
    private void receiveMessage() {
    	
    }
    
    /**
     * Test simulating a DC-failover while sending a total of 5000 messages into a single queue. This 
     * test requires that sqs and alternateSqs point to two separate api servers using two separate redis
     * instances but using a shared cassandra ring.
     */
    @Test
    public void testDCFailover() {

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
        		
	        		AmazonSQS cqs = sqs;
		        	
		        	if (useAlternateSqs) {
		        		cqs = alternateSqs;
		        	}
		        	
		        	// flip to second data center half-way through the test
		        	
		        	if (messageCounter >= NUM_MESSAGES/2 && !useAlternateSqs) {
		        		useAlternateSqs = true;
		        		logger.info("switching to secondary data center");
		        	}
		        	
			        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
					receiveMessageRequest.setQueueUrl(queueUrl);
					receiveMessageRequest.setMaxNumberOfMessages(1);
					
					ReceiveMessageResult receiveMessageResult = cqs.receiveMessage(receiveMessageRequest);
					
					if (receiveMessageResult.getMessages().size() == 1) {
					
						messageCounter += receiveMessageResult.getMessages().size();
						messageMap.put(receiveMessageResult.getMessages().get(0).getBody(), "");
						
						DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
						deleteMessageRequest.setQueueUrl(queueUrl);
						deleteMessageRequest.setReceiptHandle(receiveMessageResult.getMessages().get(0).getReceiptHandle());
						cqs.deleteMessage(deleteMessageRequest);
						
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
