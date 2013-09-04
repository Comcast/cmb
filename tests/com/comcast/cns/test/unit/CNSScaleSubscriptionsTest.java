package com.comcast.cns.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;

public class CNSScaleSubscriptionsTest {
	
	private static Logger logger = Logger.getLogger(CNSScaleTopicsTest.class);

    private AmazonSNS sns = null;
    private AmazonSQS sqs = null;
    
    private User user = null;
    private Random randomGenerator = new Random();
    private final static String TOPIC_PREFIX = "TSTT"; 
    private final static String QUEUE_PREFIX = "TSTQ"; 
    
    private static List<String> queueUrls;

    @Before
    public void setup() throws Exception {
    	
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
        
        queueUrls = new ArrayList<String>();
        
        try {
        	
            IUserPersistence userPersistence = new UserCassandraPersistence();
 
            user = userPersistence.getUserByName("cns_unit_test");

            if (user == null) {
                user = userPersistence.createUser("cns_unit_test", "cns_unit_test");
            }

            BasicAWSCredentials credentialsUser = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());

            sns = new AmazonSNSClient(credentialsUser);
            sns.setEndpoint(CMBProperties.getInstance().getCNSServiceUrl());
            
            sqs = new AmazonSQSClient(credentialsUser);
            sqs.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());
            
        } catch (Exception ex) {
            logger.error("setup failed", ex);
            fail("setup failed: "+ex);
            return;
        }
    }
    
    @After    
    public void tearDown() {
    }
    
    @Test
    public void Create250Subscriptions() {
    	CreateNSubscriptions(250);
    }

    @Test
    public void Create1200Subscriptions() {
    	CreateNSubscriptions(1200);
    }
	
    private void CreateNSubscriptions(long n) {

    	try {
    		
    		long counter = 0;
    		long totalTime = 0;
    		
    		String topicName = TOPIC_PREFIX + randomGenerator.nextLong();
	        CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
	        String topicArn = sns.createTopic(createTopicRequest).getTopicArn();
	        logger.info("created topic " + counter + ": " + topicArn);
    		
    		for (int i=0; i<n; i++) {
    			
        		String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
    	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
    	        
    	        long start = System.currentTimeMillis();
    	        
    	        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
    	        
    	        long end = System.currentTimeMillis();
    	        totalTime += end-start;
    	        
    	        logger.info("average creation millis: " + (totalTime/(i+1)));
    	        
    	        queueUrls.add(queueUrl);

    	        logger.info("created queue " + counter + ": " + queueUrl);
    	        
    	        counter++;
    		}    	        
    		
    		Thread.sleep(1000);

    		long subscribeFailures = 0;
    		counter = 0;
    		
    		for (int i=0; i<n; i++) {
	            try {
	            	sns.subscribe(new SubscribeRequest(topicArn, "cqs", com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrls.get(i))));
	            	logger.info("subscribed queue to topic " + counter + ": " + queueUrls.get(i));
	            	counter++;
	            } catch (Exception ex) {
    				logger.error("subscribe failure", ex);
    				subscribeFailures++;
	            }
    		}
    		
    		Thread.sleep(2000);

            try {
    			sns.publish(new PublishRequest(topicArn, "test message"));
            	logger.info("published message on topic " + counter + ": " + topicArn);
            } catch (Exception ex) {
				logger.error("publish failure", ex);
            }
    		
    		Thread.sleep(10000);
    		
			try {
            	sns.deleteTopic(new DeleteTopicRequest(topicArn));
            	logger.info("deleted topic " + counter + ": " + topicArn);
			} catch (Exception ex) {
				logger.error("delete failure", ex);
			}
			
			long messageCount = 0;
    		
    		for (String queueUrl : queueUrls) {
    		
	    		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
	    		receiveMessageRequest.setQueueUrl(queueUrl);
	    		receiveMessageRequest.setMaxNumberOfMessages(1);
	    		
	    		ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
	    		counter = receiveMessageResult.getMessages().size();
	    		
	    		logger.info(messageCount + " found message in queue " + queueUrl);
	    		
	    		for (Message message : receiveMessageResult.getMessages()) {
	    			
	    			messageCount++;
	    		
	    	    	DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
	    			deleteMessageRequest.setQueueUrl(queueUrl);
	    			deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
	    			sqs.deleteMessage(deleteMessageRequest);
	    		}
	    		
	    		sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
    		
    		}
    		
    		logger.info("subscribe failures: " + subscribeFailures + " messages found: " + messageCount);
    		
    		assertTrue("Subscribe failures: " + subscribeFailures, subscribeFailures == 0);
    		assertTrue("Wrong number of messages found: " + messageCount, messageCount == n);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		}
    }
}
