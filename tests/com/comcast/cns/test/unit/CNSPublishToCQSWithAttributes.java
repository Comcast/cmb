package com.comcast.cns.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;

public class CNSPublishToCQSWithAttributes extends CMBAWSBaseTest {
	
    @Test
    public void TestPublishToCQSWithAttributes() {

    	try {
    		
	        String queueUrl = getQueueUrl(0, USR.USER1);
	        String topicArn = getTopic(0, USR.USER1);
    		
    		Thread.sleep(1000);

    		long subscribeFailures = 0;
    		
            try {
            	SubscribeRequest subscribeRequest = new SubscribeRequest(topicArn, "cqs", com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl));
            	SubscribeResult subscribeResult = cns1.subscribe(subscribeRequest);
            	logger.info("subscribed queue to topic " + queueUrl);
    			SetSubscriptionAttributesRequest setSubscriptionAttributesRequest = new SetSubscriptionAttributesRequest(subscribeResult.getSubscriptionArn(), "RawMessageDelivery", "true");
    			cns1.setSubscriptionAttributes(setSubscriptionAttributesRequest);
            } catch (Exception ex) {
				logger.error("subscribe failure", ex);
				subscribeFailures++;
            }
    		
    		Thread.sleep(1000);

            try {
            	PublishRequest publishRequest = new PublishRequest(topicArn, "test message");
            	MessageAttributeValue value = new MessageAttributeValue();
            	value.setDataType("String");
            	value.setStringValue("abc");
				publishRequest.addMessageAttributesEntry("mystring", value);
    			cns1.publish(publishRequest);
            	logger.info("published message on topic " + topicArn);
            } catch (Exception ex) {
				logger.error("publish failure", ex);
            }
    		
    		Thread.sleep(1000);
    		
			long messageCount = 0;
    		
    		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
    		receiveMessageRequest.setQueueUrl(queueUrl);
    		receiveMessageRequest.setMaxNumberOfMessages(1);
        	receiveMessageRequest.setMessageAttributeNames(new ArrayList<String>(Arrays.asList("All")));
    		
    		ReceiveMessageResult receiveMessageResult = cqs1.receiveMessage(receiveMessageRequest);
    		messageCount = receiveMessageResult.getMessages().size();
    		
    		logger.info(messageCount + " found message in queue " + queueUrl);
    		
    		for (Message message : receiveMessageResult.getMessages()) {
    			
    			assertTrue("wrong message content " + message.getBody(), message.getBody().equals("test message"));
    			
        		assertTrue("wrong number of message attributes " + message.getMessageAttributes().size(), message.getMessageAttributes().size() == 1);
        		assertTrue("message attribute is null", message.getMessageAttributes().containsKey("mystring"));
        		assertTrue("message attribute has wrong value " + message.getMessageAttributes().get("mystring"), message.getMessageAttributes().get("mystring").getStringValue().equals("abc"));
    			
    	    	DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
    			deleteMessageRequest.setQueueUrl(queueUrl);
    			deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
    			cqs1.deleteMessage(deleteMessageRequest);
    		}
    		
    		logger.info("subscribe failures: " + subscribeFailures + " messages found: " + messageCount);
    		
    		assertTrue("Subscribe failures: " + subscribeFailures, subscribeFailures == 0);
    		assertTrue("Wrong number of messages found: " + messageCount, messageCount == 1);
	        
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
    }
}
