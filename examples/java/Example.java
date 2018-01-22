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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.CMBTestingConstants;

public class Example {
	
    private static Logger logger = Logger.getLogger(Example.class);
    private final static String QUEUE_PREFIX = "TSTQ_"; 
    
	public static String getArnForQueueUrl(String url) {
		
		if (url == null) {
			return null;
		}
		
		String elements[] = url.split("/");
		
		if (elements.length != 5) {
			return null;
		}
		
		String arn = "arn:cmb:cqs:" + "ccp" + ":" + elements[3] + ":" + elements[4];

		return arn;
	}
	
	public static String httpGet(String url) throws IOException {
		
	    URL confirmationEndpoint = new URL(url);
	    URLConnection conn = confirmationEndpoint.openConnection();
	    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	    String inputLine;
	    String response = "";

        while ((inputLine = in.readLine()) != null) {
	        response += inputLine;
        }
	    
        logger.info(response);
        
        in.close();
        
        return response;
	}
	
	public static void main(String [ ] args) {
		
    	try {
    		
            Util.initLog4jTest();
    		
            //TODO: set user id and credentials for two distinct users
            
            // user "cqs_test_1"
                     
            BasicAWSCredentials user1Credentials = new BasicAWSCredentials("<access_key>", "<secret_key>");

            // user "cqs_test_2"
            
            //String user2Id = "<user_id>";
            String user2Id = "389653920093";
            
            BasicAWSCredentials user2Credentials = new BasicAWSCredentials("<access_key>", "<secret_key>");

            // service urls
            
            //TODO: add service URLs
            
            //String cqsServerUrl = "http://<host>:<port>";
            //String cnsServerUrl = "http://<host>:<port>";
            
            String cqsServerUrl = "http://localhost:6059";
            String cnsServerUrl = "http://localhost:6061";

            // initialize service

            AmazonSQSClient sqs = new AmazonSQSClient(user1Credentials);
            sqs.setEndpoint(cqsServerUrl);

            AmazonSNSClient sns = new AmazonSNSClient(user2Credentials);
            sns.setEndpoint(cnsServerUrl);
        
            // create queue
            
    		Random randomGenerator = new Random();
    		
	    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
	        
	        HashMap<String, String> attributeParams = new HashMap<String, String>();
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
	        createQueueRequest.setAttributes(attributeParams);
	        String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	        
	        AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setQueueUrl(queueUrl);
	        addPermissionRequest.setActions(Arrays.asList("SendMessage"));
	        addPermissionRequest.setLabel(UUID.randomUUID().toString());
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2Id));        
	        sqs.addPermission(addPermissionRequest);
	        
	        // create topic
	        
	        String topicName = "TSTT" + randomGenerator.nextLong();
	        	
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
			CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
			String topicArn = createTopicResult.getTopicArn();
			
			// subscribe and confirm cqs endpoint
			
			SubscribeRequest subscribeRequest = new SubscribeRequest();
			String queueArn = getArnForQueueUrl(queueUrl);
			subscribeRequest.setEndpoint(queueArn);
			subscribeRequest.setProtocol("cqs");
			subscribeRequest.setTopicArn(topicArn);
			SubscribeResult subscribeResult = sns.subscribe(subscribeRequest);
			String subscriptionArn = subscribeResult.getSubscriptionArn();
			
			if (subscriptionArn.equals("pending confirmation")) {
				
				Thread.sleep(500);
				
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
				receiveMessageRequest.setQueueUrl(queueUrl);
				receiveMessageRequest.setMaxNumberOfMessages(1);
				ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
				
				List<Message> messages = receiveMessageResult.getMessages();
				
				if (messages != null && messages.size() == 1) {
					
	    			JSONObject o = new JSONObject(messages.get(0).getBody());
	    			
	    			if (!o.has("SubscribeURL")) {
	    				throw new Exception("message is not a confirmation messsage");
	    			}
	    			
	    			String subscriptionUrl = o.getString("SubscribeURL");
	    			httpGet(subscriptionUrl);
	    			
	    			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
	    			deleteMessageRequest.setReceiptHandle(messages.get(0).getReceiptHandle());
	    			deleteMessageRequest.setQueueUrl(queueUrl);
	    			sqs.deleteMessage(deleteMessageRequest);
				
				} else {
					throw new Exception("no confirmation message found");
				}
			}
			
			// publish and receive message
			
			PublishRequest publishRequest = new PublishRequest();
			String messageText = "quamvis sint sub aqua, sub aqua maledicere temptant";
			publishRequest.setMessage(messageText);
			publishRequest.setSubject("unit test message");
			publishRequest.setTopicArn(topicArn);
			sns.publish(publishRequest);
			
			Thread.sleep(500);

			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
			receiveMessageRequest.setQueueUrl(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(1);
			ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
			
			List<Message> messages = receiveMessageResult.getMessages();
			
			if (messages != null && messages.size() == 1) {
				
				String messageBody = messages.get(0).getBody();
				
				if (!messageBody.contains(messageText)) {
					throw new Exception("message text not found");
				}
				
    			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
    			deleteMessageRequest.setReceiptHandle(messages.get(0).getReceiptHandle());
    			deleteMessageRequest.setQueueUrl(queueUrl);
    			sqs.deleteMessage(deleteMessageRequest);

			} else {
				throw new Exception("no messages found");
			}
			
			// subscribe and confirm http endpoint

			String id = randomGenerator.nextLong() + "";
			String endPointUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + id;
			String lastMessageUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "info/" + id + "?showLast=true";
			
			subscribeRequest = new SubscribeRequest();
			subscribeRequest.setEndpoint(endPointUrl);
			subscribeRequest.setProtocol("http");
			subscribeRequest.setTopicArn(topicArn);
			subscribeResult = sns.subscribe(subscribeRequest);
			subscriptionArn = subscribeResult.getSubscriptionArn();
			
			if (subscriptionArn.equals("pending confirmation")) {
				
				Thread.sleep(500);
				
				String response = httpGet(lastMessageUrl);
					
    			JSONObject o = new JSONObject(response);
    			
    			if (!o.has("SubscribeURL")) {
    				throw new Exception("message is not a confirmation messsage");
    			}
    			
    			String subscriptionUrl = o.getString("SubscribeURL");
    			
    			response = httpGet(subscriptionUrl);
			}			
			
			// publish and receive message
			
			publishRequest = new PublishRequest();
			publishRequest.setMessage(messageText);
			publishRequest.setSubject("unit test message");
			publishRequest.setTopicArn(topicArn);
			sns.publish(publishRequest);
			
			Thread.sleep(500);
			
			String response = httpGet(lastMessageUrl);
			
			if (response != null && response.length() > 0) {
				
				if (!response.contains(messageText)) {
					throw new Exception("message text not found");
				}
				
			} else {
				throw new Exception("no messages found");
			}
			
			// delete queue and topic
			
			DeleteTopicRequest  deleteTopicRequest = new DeleteTopicRequest(topicArn);
			sns.deleteTopic(deleteTopicRequest);
			
        	sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
        	
        	System.out.println("OK");
			
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
	}
}
