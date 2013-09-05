package com.comcast.cns.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CNSScaleTopicsTest extends CMBAWSBaseTest {

    private String queueUrl;
    private static List<String> topicArns = null;

    //@Test
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
    		
    		topicArns = new ArrayList<String>();
    		
    		long counter = 0;
    		long createFailures = 0;
    		long totalTime = 0;
    		
    	    queueUrl = getQueueUrl(1, USR.USER1);
    	    
    	    Thread.sleep(1000);
    		
    		for (int i=0; i<n; i++) {
    			
    			try {
                
	    	        long start = System.currentTimeMillis();
	    	        
	    	        String topicArn = getTopic(i, USR.USER1);
	    	        
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
	            	cns1.subscribe(new SubscribeRequest(topicArn, "cqs", com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl)));
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
	    			cns1.publish(new PublishRequest(topicArn, "test message " + counter));
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
    		long totalCount = 0;
    		
    		do {
    		
	    		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
	    		receiveMessageRequest.setQueueUrl(queueUrl);
	    		receiveMessageRequest.setMaxNumberOfMessages(10);
	    		
	    		ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
	    		counter = receiveMessageResult.getMessages().size();
	    		totalCount += counter;
	    		
	    		logger.info("found " + counter + " messages in queue");
	    		
	    		for (Message message : receiveMessageResult.getMessages()) {
	    		
	    	    	//logger.info("\t" + message.getBody());
	    			
	    			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
	    			deleteMessageRequest.setQueueUrl(queueUrl);
	    			deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
	    			cqs1.deleteMessage(deleteMessageRequest);
	    		}
    		} while (counter > 0);
    		
    		logger.info("create failuers: " + createFailures +  " delete failures: " + deleteFailures + " publish failures: " + publishFailures + " subscribe failures: " + subscribeFailures + " messages found: " + totalCount);
    		
    		assertTrue("Create failures: " + createFailures, createFailures == 0);
    		assertTrue("Delete failures: " + deleteFailures, deleteFailures == 0);
    		assertTrue("Send failures: " + publishFailures, publishFailures == 0);
    		assertTrue("Subscribe failures: " + subscribeFailures, subscribeFailures == 0);
    		assertTrue("Wrong number of messages found: " + totalCount, totalCount == n);
	        
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }
}
