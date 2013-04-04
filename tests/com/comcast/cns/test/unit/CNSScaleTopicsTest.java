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
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;

public class CNSScaleTopicsTest {

	private static Logger logger = Logger.getLogger(CNSScaleTopicsTest.class);

    private AmazonSNS sns = null;
    private AmazonSQS sqs = null;
    
    private User user = null;
    private Random randomGenerator = new Random();
    private final static String TOPIC_PREFIX = "TSTT"; 
    
    private String queueUrl;
    
    private static List<String> topicArns;

    @Before
    public void setup() throws Exception {
    	
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
        
        topicArns = new ArrayList<String>();
        
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
            
            queueUrl = sqs.createQueue(new CreateQueueRequest("MessageSink")).getQueueUrl();

        } catch (Exception ex) {
            logger.error("setup failed", ex);
            fail("setup failed: "+ex);
            return;
        }
    }
    
    @After    
    public void tearDown() {
    	//sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
    }
    
    @Test
    public void Create1000Topics() {
    	CreateNTopics(1000);
    }

    @Test
    public void Create100Topics() {
    	CreateNTopics(100);
    }

    @Test
    public void Create10Topics() {
    	CreateNTopics(10);
    }

    @Test
    public void Create1Topics() {
    	CreateNTopics(1);
    }

    private void CreateNTopics(long n) {

    	try {
    		
    		long counter = 0;
    		long createFailures = 0;
    		long totalTime = 0;
    		
    		for (int i=0; i<n; i++) {
    			
    			try {
                
	        		String topicName = TOPIC_PREFIX + randomGenerator.nextLong();
	    	        CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
	    	        
	    	        long start = System.currentTimeMillis();
	    	        
	    	        String topicArn = sns.createTopic(createTopicRequest).getTopicArn();
	    	        
	    	        long end = System.currentTimeMillis();
	    	        totalTime += end-start;
	    	        
	    	        logger.info("average creation millis: " + (totalTime/(i+1)));
	    	        
	    	        topicArns.add(topicArn);
	
	    	        logger.info("created topic " + counter + ": " + topicArn);
	    	        
	    	        counter++;
    	        
    			} catch (Exception ex) {
    				logger.error("create failure", ex);
    				createFailures++;
    	        }
    		}
    		
    		Thread.sleep(1000);

    		long subscribeFailures = 0;
    		counter = 0;
    		
    		for (String topicArn : topicArns) {
	            try {
	            	sns.subscribe(new SubscribeRequest(topicArn, "cqs", com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl)));
	            	logger.info("subscribed queue to topic " + counter + ": " + topicArn);
	            	counter++;
	            } catch (Exception ex) {
    				logger.error("subscribe failure", ex);
    				subscribeFailures++;
	            }
    		}
    		
    		Thread.sleep(1000);

    		long publishFailures = 0;
    		counter = 0;
    		
    		for (String topicArn : topicArns) {
	            try {
	    			sns.publish(new PublishRequest(topicArn, "test message " + counter));
	            	logger.info("published message on topic " + counter + ": " + topicArn);
	            	counter++;
	            } catch (Exception ex) {
    				logger.error("publish failure", ex);
    				publishFailures++;
	            }
    		}
    		
    		Thread.sleep(1000);
    		
    		long deleteFailures = 0;
    		counter = 0;
    		
    		for (String topicArn : topicArns) {
    			try {
	            	sns.deleteTopic(new DeleteTopicRequest(topicArn));
	            	logger.info("deleted topic " + counter + ": " + topicArn);
	            	counter++;
    			} catch (Exception ex) {
    				logger.error("delete failure", ex);
    				deleteFailures++;
    			}
    		}
    		
    		counter = 0;
    		long totalCount = 0;
    		
    		do {
    		
	    		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
	    		receiveMessageRequest.setQueueUrl(queueUrl);
	    		receiveMessageRequest.setMaxNumberOfMessages(10);
	    		
	    		ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
	    		counter = receiveMessageResult.getMessages().size();
	    		totalCount += counter;
	    		
	    		logger.info("found " + counter + " messages in queue");
	    		
	    		for (Message message : receiveMessageResult.getMessages()) {
	    		
	    	    	//logger.info("\t" + message.getBody());
	    			
	    			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
	    			deleteMessageRequest.setQueueUrl(queueUrl);
	    			deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
	    			sqs.deleteMessage(deleteMessageRequest);
	    		}
    		} while (counter > 0);
    		
    		logger.info("create failuers: " + createFailures +  " delete failures: " + deleteFailures + " publish failures: " + publishFailures + " subscribe failures: " + subscribeFailures + " messages found: " + totalCount);
    		
    		assertTrue("Create failures: " + createFailures, createFailures == 0);
    		assertTrue("Delete failures: " + deleteFailures, deleteFailures == 0);
    		assertTrue("Send failures: " + publishFailures, publishFailures == 0);
    		assertTrue("Subscribe failures: " + subscribeFailures, subscribeFailures == 0);
    		assertTrue("Wrong number of messages found: " + totalCount, totalCount == n);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		}
    }
}
