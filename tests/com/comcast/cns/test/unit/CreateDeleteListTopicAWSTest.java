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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.*;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.AWSCredentialsHolder;
import com.comcast.cmb.test.tools.CMBTestingConstants;

import org.apache.log4j.Logger;
import org.junit.* ;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.RemovePermissionRequest;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sns.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;

import static org.junit.Assert.*;

public class CreateDeleteListTopicAWSTest {

    private static Logger logger = Logger.getLogger(CreateDeleteListTopicAWSTest.class);
    
    private Random rand = new Random();
    
    private final static String QUEUE_PREFIX = "TSTQ"; 
    private final static String TOPIC_PREFIX = "T"; 
    
	private User user = null;
	private User user2 = null;
	private User user3 = null;

	private AmazonSNS sns = null;
	private AmazonSQS sqs = null;

	private AmazonSNS sns2 = null;
	private AmazonSQS sqs2 = null;

	private AmazonSNS sns3 = null;
	private AmazonSQS sqs3 = null;

	@Before
    public void setup() throws Exception {
    	
    	boolean useLocalSns = true;

    	Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
		
        AWSCredentials awsCredentials = null;
        AWSCredentials awsCredentials2 = null;
        AWSCredentials awsCredentials3 = null;
		
		if (useLocalSns) {
			
			IUserPersistence userHandler = new UserCassandraPersistence();
			
			user = userHandler.getUserByName("cns_unit_test");
			
			if (user == null) { 
				user =  userHandler.createUser("cns_unit_test", "cns_unit_test");
			}
			
			user2 = userHandler.getUserByName("cns_unit_test_2");
			
			if (user2 == null) { 
				user2 =  userHandler.createUser("cns_unit_test_2", "cns_unit_test_2");
			}

			user3 = userHandler.getUserByName("cns_unit_test_3");
			
			if (user3 == null) { 
				user3 =  userHandler.createUser("cns_unit_test_3", "cns_unit_test_3");
			}

			awsCredentials = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
			awsCredentials2 = new BasicAWSCredentials(user2.getAccessKey(), user2.getAccessSecret());
			awsCredentials3 = new BasicAWSCredentials(user3.getAccessKey(), user3.getAccessSecret());
		
		} else {
			awsCredentials = AWSCredentialsHolder.initAwsCredentials();
			awsCredentials2 = AWSCredentialsHolder.initAwsCredentials();
			awsCredentials3 = AWSCredentialsHolder.initAwsCredentials();
		}
		
		ClientConfiguration clientConfiguration = new ClientConfiguration();

		sns = new AmazonSNSClient(awsCredentials, clientConfiguration);
	    sqs = new AmazonSQSClient(awsCredentials, clientConfiguration);

		sns2 = new AmazonSNSClient(awsCredentials2, clientConfiguration);
	    sqs2 = new AmazonSQSClient(awsCredentials2, clientConfiguration);

		sns3 = new AmazonSNSClient(awsCredentials3, clientConfiguration);
	    sqs3 = new AmazonSQSClient(awsCredentials3, clientConfiguration);

	    if (useLocalSns) {
			
	    	sns.setEndpoint(CMBProperties.getInstance().getCNSServerUrl());
			//sns.setEndpoint("http://localhost:7070/AWSProxy/");
			sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());
			
			sns2.setEndpoint(CMBProperties.getInstance().getCNSServerUrl());
			sqs2.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());

			sns3.setEndpoint(CMBProperties.getInstance().getCNSServerUrl());
			sqs3.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());
	    }
    }
    
	@After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
	
	@Test 
	public void testManySubscriptions() {
		
		String topicArn = null;
		List<String> queueUrls = new ArrayList<String>();
		
		try {
			
			// create topic

			String topicName = TOPIC_PREFIX + rand.nextLong();
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
			CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
			topicArn = createTopicResult.getTopicArn();

			for (int i=0; i<110; i++) {
				
				String queueName = QUEUE_PREFIX + rand.nextInt(25000);
		        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
		        queueUrls.add(sqs.createQueue(createQueueRequest).getQueueUrl());
			}
			
			Thread.sleep(500);
			
			for (String queueUrl : queueUrls) {
			
				SubscribeRequest subscribeRequest = new SubscribeRequest();
				String queueArn = com.comcast.cqs.util.Util.getNameForAbsoluteQueueUrl(queueUrl);
				subscribeRequest.setEndpoint(queueArn);
				subscribeRequest.setProtocol("cqs");
				subscribeRequest.setTopicArn(topicArn);
				
				sns.subscribe(subscribeRequest);
			}
			
			ListSubscriptionsByTopicRequest listSubscriptionsByTopicRequest = new ListSubscriptionsByTopicRequest();
			listSubscriptionsByTopicRequest.setTopicArn(topicArn);
			
			ListSubscriptionsByTopicResult listSubscriptionsByTopicResult = sns.listSubscriptionsByTopic(listSubscriptionsByTopicRequest);
			
			assertTrue(listSubscriptionsByTopicResult.getSubscriptions().size() == 100);
			
			listSubscriptionsByTopicRequest = new ListSubscriptionsByTopicRequest();
			listSubscriptionsByTopicRequest.setTopicArn(topicArn);
			listSubscriptionsByTopicRequest.setNextToken(listSubscriptionsByTopicResult.getNextToken());
			
			listSubscriptionsByTopicResult = sns.listSubscriptionsByTopic(listSubscriptionsByTopicRequest);

			assertTrue(listSubscriptionsByTopicResult.getSubscriptions().size() == 10);
		
		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("test failed: " + ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest();
					deleteTopicRequest.setTopicArn(topicArn);
					sns.deleteTopic(deleteTopicRequest);
				} catch (Exception e) { }
			}
			
			for (String queueUrl : queueUrls) {
				try {
					DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
					deleteQueueRequest.setQueueUrl(queueUrl);
					sqs.deleteQueue(deleteQueueRequest);
				} catch (Exception e) { }
			}
		}
	}
	
	@Test
	public void testTopicPermissions() {
		
		String topicArn = null;

		try {
			
			// create topic

			String topicName = TOPIC_PREFIX + rand.nextLong();
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
			CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
			topicArn = createTopicResult.getTopicArn();
			
	        // test set attribute permission
			
			AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setTopicArn(topicArn);
	        addPermissionRequest.setActionNames(Arrays.asList("SetTopicAttributes", "Publish"));
	        addPermissionRequest.setLabel("P1");
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
	        sns.addPermission(addPermissionRequest);
	        
	        SetTopicAttributesRequest setTopicAttributesRequest = new SetTopicAttributesRequest();
	        setTopicAttributesRequest.setAttributeName("DisplayName");
	        setTopicAttributesRequest.setAttributeValue("NewDisplayName");
	        setTopicAttributesRequest.setTopicArn(topicArn);
	        sns2.setTopicAttributes(setTopicAttributesRequest);
			
	        GetTopicAttributesRequest getTopicAttributesRequest = new GetTopicAttributesRequest();
	        getTopicAttributesRequest.setTopicArn(topicArn);
	        GetTopicAttributesResult result = sns.getTopicAttributes(getTopicAttributesRequest);
	        
	        assertTrue(result.getAttributes().get("DisplayName").equals("NewDisplayName"));
	
	        RemovePermissionRequest removePermissionRequest = new RemovePermissionRequest();
	        removePermissionRequest.setTopicArn(topicArn);
	        removePermissionRequest.setLabel("P1");
	        sns.removePermission(removePermissionRequest);
	        
	        boolean expectedExceptionreceived = false;
	        
	        try {
	            setTopicAttributesRequest = new SetTopicAttributesRequest();
	            setTopicAttributesRequest.setAttributeName("DisplayName");
	            setTopicAttributesRequest.setAttributeValue("NewDisplayName2");
	            setTopicAttributesRequest.setTopicArn(topicArn);
	            sns2.setTopicAttributes(setTopicAttributesRequest);
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("don't have permission"));
	        	expectedExceptionreceived = true;
	        }
	        
	        if (!expectedExceptionreceived) {
	        	fail("unexpected permission");
	        }
	        
	        // test publish permission
	        
	        expectedExceptionreceived = false;
	        
	        try {
		        PublishRequest publishRequest = new PublishRequest();
		        publishRequest.setMessage("hello world!!!");
		        publishRequest.setTopicArn(topicArn);
		        sns2.publish(publishRequest);
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("don't have permission"));
	        	expectedExceptionreceived = true;
	        }
	
	        if (!expectedExceptionreceived) {
	        	fail("unexpected permission");
	        }

	        addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setTopicArn(topicArn);
	        addPermissionRequest.setActionNames(Arrays.asList("Publish"));
	        addPermissionRequest.setLabel("P2");
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
	        sns.addPermission(addPermissionRequest);

	        PublishRequest publishRequest = new PublishRequest();
	        publishRequest.setMessage("hello world!!!");
	        publishRequest.setTopicArn(topicArn);
	        sns2.publish(publishRequest);

	        removePermissionRequest = new RemovePermissionRequest();
	        removePermissionRequest.setTopicArn(topicArn);
	        removePermissionRequest.setLabel("P2");
	        sns.removePermission(removePermissionRequest);

	        expectedExceptionreceived = false;
	        
	        try {
		        publishRequest = new PublishRequest();
		        publishRequest.setMessage("hello world!!!");
		        publishRequest.setTopicArn(topicArn);
		        sns2.publish(publishRequest);
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("don't have permission"));
	        	expectedExceptionreceived = true;
	        }
	
	        if (!expectedExceptionreceived) {
	        	fail("unexpected permission");
	        }
	        
	        // test add permission
	        
	        addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setTopicArn(topicArn);
	        addPermissionRequest.setActionNames(Arrays.asList("AddPermission"));
	        addPermissionRequest.setLabel("P3");
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
	        sns.addPermission(addPermissionRequest);
	        
	        addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setTopicArn(topicArn);
	        addPermissionRequest.setActionNames(Arrays.asList("Publish"));
	        addPermissionRequest.setLabel("P4");
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user3.getUserId()));        
	        sns2.addPermission(addPermissionRequest);
	        
	        publishRequest = new PublishRequest();
	        publishRequest.setMessage("hello world!!!");
	        publishRequest.setTopicArn(topicArn);
	        sns3.publish(publishRequest);
	        
	        // try some invalid stuff
	        
	        expectedExceptionreceived = false;
	        
	        try {
		        addPermissionRequest = new AddPermissionRequest();
		        addPermissionRequest.setTopicArn(topicArn);
		        addPermissionRequest.setActionNames(Arrays.asList("AddBoris"));
		        addPermissionRequest.setLabel("P5");
		        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
		        sns.addPermission(addPermissionRequest);
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("Invalid action parameter"));
	        	expectedExceptionreceived = true;
	        }
	
	        if (!expectedExceptionreceived) {
	        	fail("missing validation error");
	        }
	        
	        expectedExceptionreceived = false;
	        
	        try {
		        addPermissionRequest = new AddPermissionRequest();
		        addPermissionRequest.setTopicArn(topicArn);
		        addPermissionRequest.setActionNames(Arrays.asList(""));
		        addPermissionRequest.setLabel("P6");
		        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
		        sns.addPermission(addPermissionRequest);
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("Blank action parameter"));
	        	expectedExceptionreceived = true;
	        }
	
	        if (!expectedExceptionreceived) {
	        	fail("missing validation error");
	        }

		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("test failed: " + ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest();
					deleteTopicRequest.setTopicArn(topicArn);
					sns.deleteTopic(deleteTopicRequest);
				} catch (Exception e) { }
			}
		}
	}

	@Test
	public void testCreateSubscribePublishDeleteTopic() {
		
		String topicArn = null;
		String queueUrl = null;

		try {
			
			String topicName = TOPIC_PREFIX + rand.nextLong();
			CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
			CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
			topicArn = createTopicResult.getTopicArn();
			
			Thread.sleep(500);
			
			SubscribeRequest subscribeRequest1 = new SubscribeRequest();
			subscribeRequest1.setEndpoint(CMBTestingConstants.EMAIL_ENDPOINT);
			subscribeRequest1.setProtocol("email");
			subscribeRequest1.setTopicArn(topicArn);
			
			SubscribeResult subscribeResult1 = sns.subscribe(subscribeRequest1);
			
			subscribeResult1.getSubscriptionArn();
			
			Thread.sleep(500);

			SubscribeRequest subscribeRequest2 = new SubscribeRequest();
			subscribeRequest2.setEndpoint(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "nop/1234");
			subscribeRequest2.setProtocol("http");
			subscribeRequest2.setTopicArn(topicArn);
			
			SubscribeResult subscribeResult2 = sns.subscribe(subscribeRequest2);
			
			subscribeResult2.getSubscriptionArn();
					
	    	String queueName = QUEUE_PREFIX + rand.nextLong();
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
	        queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			
			Thread.sleep(500);
			
			//GetSubscriptionAttributesRequest subscriptionAttributesRequest = new GetSubscriptionAttributesRequest();
			//subscriptionAttributesRequest.setSubscriptionArn("arn:aws:sns:us-east-1:266126687520:Topic94:7dbfebde-ea96-42c7-9467-4bf6b3f4ac42");
			//GetSubscriptionAttributesResult attributes = sns.getSubscriptionAttributes(subscriptionAttributesRequest);
	        
			SubscribeRequest subscribeRequest3 = new SubscribeRequest();
			subscribeRequest3.setEndpoint(queueUrl);
			subscribeRequest3.setProtocol("cqs");
			subscribeRequest3.setTopicArn(topicArn);
			
			SubscribeResult subscribeResult3 = sns.subscribe(subscribeRequest3);
			
			subscribeResult3.getSubscriptionArn();
			
			Thread.sleep(500);

			PublishRequest publishRequest = new PublishRequest();
			publishRequest.setMessage("quamvis sint sub aqua, sub aqua maledicere temptant");
			publishRequest.setSubject("unit test message");
			publishRequest.setTopicArn(topicArn);
			
			sns.publish(publishRequest);

	        DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
	        deleteQueueRequest.setQueueUrl(queueUrl);
	        sqs.deleteQueue(deleteQueueRequest);

		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("test failed: " + ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest();
					deleteTopicRequest.setTopicArn(topicArn);
					sns.deleteTopic(deleteTopicRequest);
				} catch (Exception e) { }
			}

			if (queueUrl != null) {
				try {
					DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest();
					deleteQueueRequest.setQueueUrl(queueUrl);
					sqs.deleteQueue(deleteQueueRequest);
				} catch (Exception e) { }
			}
		}
	}
}
