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
package com.comcast.cns.test.unit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Random;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import static org.junit.Assert.*;

public class RawMessageDeliveryTest {
	
    private static Logger logger = Logger.getLogger(RawMessageDeliveryTest.class);
    
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
	    
        in.close();
        
        return response;
	}
	
	@Test
	public void testRawMessageDelivery() throws Exception {
		
        Util.initLog4jTest();
        Random randomGenerator = new Random();

        IUserPersistence userPersistence = new UserCassandraPersistence();
        
        User user = userPersistence.getUserByName("cns_unit_test");

        if (user == null) {
            user = userPersistence.createUser("cns_unit_test", "cns_unit_test");
        }

        BasicAWSCredentials credentialsUser = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
    
        AmazonSNSClient sns = new AmazonSNSClient(credentialsUser);
        sns.setEndpoint(CMBProperties.getInstance().getCNSServiceUrl());

        // create topic
        String topicName = "RAWTEST_" + randomGenerator.nextLong();
		CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
		CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
		String topicArn = createTopicResult.getTopicArn();
		
		// subscribe and confirm http endpoint to receive raw message
		String id = randomGenerator.nextLong() + "";
		
		String rawEndPointUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + id;
		String rawEndPointLastMessageUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "info/" + id + "?showLast=true";
		
		SubscribeRequest rawEndPointSubscribeRequest = new SubscribeRequest();
		rawEndPointSubscribeRequest.setEndpoint(rawEndPointUrl);
		rawEndPointSubscribeRequest.setProtocol("http");
		rawEndPointSubscribeRequest.setTopicArn(topicArn);
		SubscribeResult subscribeResult = sns.subscribe(rawEndPointSubscribeRequest);
		String rawEndPointsubscriptionArn = subscribeResult.getSubscriptionArn();
		
		if (rawEndPointsubscriptionArn.equals("pending confirmation")) {
			
			Thread.sleep(500);				
			String response = httpGet(rawEndPointLastMessageUrl);
			logger.info(response);
			JSONObject o = new JSONObject(response);
			if (!o.has("SubscribeURL")) {
				throw new Exception("message is not a confirmation messsage");
			}
			String subscriptionUrl = o.getString("SubscribeURL");
			response = httpGet(subscriptionUrl);
			
			String startTag = "<SubscriptionArn>";
			String endTag = "</SubscriptionArn>";
			int startIndex = response.indexOf(startTag);
			int endIndex = response.indexOf(endTag);
			String subArn = response.substring(startIndex + startTag.length(), endIndex);
			if(subArn != null && !subArn.isEmpty()){
				rawEndPointsubscriptionArn = subArn;
			}
		}
		
		// set subscription attribute for raw message delivery
		
		Boolean rawMessageDelivery = true;
		try {
			SetSubscriptionAttributesRequest setSubscriptionAttributesRequest = new SetSubscriptionAttributesRequest(rawEndPointsubscriptionArn, "RawMessageDelivery", rawMessageDelivery.toString());
			sns.setSubscriptionAttributes(setSubscriptionAttributesRequest);
			
			Map<String, String> attributes = null;
			GetSubscriptionAttributesRequest getSubscriptionAttributesRequest = new GetSubscriptionAttributesRequest(rawEndPointsubscriptionArn);
			GetSubscriptionAttributesResult getSubscriptionAttributesResult = sns.getSubscriptionAttributes(getSubscriptionAttributesRequest);
			attributes = getSubscriptionAttributesResult.getAttributes();
			String rawMessageDeliveryStr = attributes.get("RawMessageDelivery");
			if(rawMessageDeliveryStr != null && !rawMessageDeliveryStr.isEmpty()){
				Boolean rawMessagePolicyReturn = Boolean.parseBoolean(rawMessageDeliveryStr);
				assertTrue("Set raw message delivery successful", rawMessagePolicyReturn);
			}
		} catch (Exception ex) {
			throw new Exception("Can't set raw message delivery attribute to subscription arn " + rawEndPointsubscriptionArn);
		}
		
		// subscribe and confirm http endpoint to receive JSON message
		id = randomGenerator.nextLong() + "";
		String jsonEndPointUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + id;
		String jsonEndPointLastMessageUrl = CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "info/" + id + "?showLast=true";
		
		SubscribeRequest jsonEndPointSubscribeRequest = new SubscribeRequest();
		jsonEndPointSubscribeRequest.setEndpoint(jsonEndPointUrl);
		jsonEndPointSubscribeRequest.setProtocol("http");
		jsonEndPointSubscribeRequest.setTopicArn(topicArn);
		SubscribeResult jsonSubscribeResult = sns.subscribe(jsonEndPointSubscribeRequest);
		String jsonEndPointsubscriptionArn = jsonSubscribeResult.getSubscriptionArn();
		
		if (jsonEndPointsubscriptionArn.equals("pending confirmation")) {
			
			Thread.sleep(500);				
			String response = httpGet(jsonEndPointLastMessageUrl);
			JSONObject o = new JSONObject(response);
			if (!o.has("SubscribeURL")) {
				throw new Exception("message is not a confirmation messsage");
			}
			String subscriptionUrl = o.getString("SubscribeURL");
			response = httpGet(subscriptionUrl);
			
			String startTag = "<SubscriptionArn>";
			String endTag = "</SubscriptionArn>";
			int startIndex = response.indexOf(startTag);
			int endIndex = response.indexOf(endTag);
			String subArn = response.substring(startIndex + startTag.length(), endIndex);
			if(subArn != null && !subArn.isEmpty()){
				jsonEndPointsubscriptionArn = subArn;
			}
		}
		
		
		// publish and receive message
		
		String messageText = "Pulish a raw message";
		PublishRequest publishRequest = new PublishRequest();
		publishRequest.setMessage(messageText);
		publishRequest.setSubject("unit test raw message");
		publishRequest.setTopicArn(topicArn);
		sns.publish(publishRequest);
		
		Thread.sleep(500);

		// check raw message is received			
		String response = httpGet(rawEndPointLastMessageUrl);
		if (response != null && response.length() > 0) {
			assertEquals("Receive raw message", response, messageText);				
		} else {
			throw new Exception("no messages found");
		}
		
		// check json message is received
		response = httpGet(jsonEndPointLastMessageUrl);
		if (response != null && response.length() > 0) {
			JSONObject obj = new JSONObject(response);
			assertNotNull("Received JSON message", obj);
		} else {
			throw new Exception("no messages found");
		}
		
		// delete topic
		DeleteTopicRequest  deleteTopicRequest = new DeleteTopicRequest(topicArn);
		sns.deleteTopic(deleteTopicRequest);
    	
    	System.out.println("OK");
	}
}
