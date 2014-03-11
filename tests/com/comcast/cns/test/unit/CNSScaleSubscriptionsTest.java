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

public class CNSScaleSubscriptionsTest extends CMBAWSBaseTest {
	
    private static List<String> queueUrls = null;
    
    @Test
    public void Create1Subscriptions() {
    	CreateNSubscriptions(1);
    }

    @Test
    public void Create10Subscriptions() {
    	CreateNSubscriptions(10);
    }

    @Test
    public void Create100Subscriptions() {
    	CreateNSubscriptions(100);
    }

    @Test
    public void Create101Subscriptions() {
    	CreateNSubscriptions(101);
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
    		
    		queueUrls = new ArrayList<String>();
    		
    		long counter = 0;
    		long totalTime = 0;
    		
	        String topicArn = getTopic(1, USR.USER1);
    		
    		for (int i=0; i<n; i++) {
    			
    	        long start = System.currentTimeMillis();
    	        
    	        String queueUrl = getQueueUrl(i,USR.USER1);
    	        
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
	            	cns1.subscribe(new SubscribeRequest(topicArn, "cqs", com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrls.get(i))));
	            	logger.info("subscribed queue to topic " + counter + ": " + queueUrls.get(i));
	            	counter++;
	            } catch (Exception ex) {
    				logger.error("subscribe failure", ex);
    				subscribeFailures++;
	            }
    		}
    		
    		Thread.sleep(2000);

            try {
    			cns1.publish(new PublishRequest(topicArn, "test message"));
            	logger.info("published message on topic " + counter + ": " + topicArn);
            } catch (Exception ex) {
				logger.error("publish failure", ex);
            }
    		
    		Thread.sleep(10000);
    		
			long messageCount = 0;
    		
    		for (String queueUrl : queueUrls) {
    		
	    		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
	    		receiveMessageRequest.setQueueUrl(queueUrl);
	    		receiveMessageRequest.setMaxNumberOfMessages(1);
	    		
	    		ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
	    		counter = receiveMessageResult.getMessages().size();
	    		
	    		logger.info(messageCount + " found message in queue " + queueUrl);
	    		
	    		for (Message message : receiveMessageResult.getMessages()) {
	    			
	    			messageCount++;
	    		
	    	    	DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
	    			deleteMessageRequest.setQueueUrl(queueUrl);
	    			deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
	    			cqs1.deleteMessage(deleteMessageRequest);
	    		}
    		}
    		
    		logger.info("subscribe failures: " + subscribeFailures + " messages found: " + messageCount);
    		
    		assertTrue("Subscribe failures: " + subscribeFailures, subscribeFailures == 0);
    		assertTrue("Wrong number of messages found: " + messageCount, messageCount == n);
	        
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }
}
