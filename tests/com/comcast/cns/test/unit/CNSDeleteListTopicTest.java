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

import com.comcast.cmb.test.tools.CMBAWSBaseTest;

import org.junit.* ;

import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.RemovePermissionRequest;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.AddPermissionRequest;

import static org.junit.Assert.*;

public class CNSDeleteListTopicTest extends CMBAWSBaseTest {

	@Test 
	public void testManySubscriptions() {
		
		String topicArn = null;
		List<String> queueUrls = new ArrayList<String>();
		
		try {
			
			topicArn = getTopic(1, USR.USER1);
			
			for (int i=0; i<110; i++) {
		        queueUrls.add(getQueueUrl(i, USR.USER1));
			}
			
			Thread.sleep(500);
			
			logger.info("Subscribing all queues to topic " + topicArn);
			
			for (String queueUrl : queueUrls) {
			
				SubscribeRequest subscribeRequest = new SubscribeRequest();
				String queueArn = com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queueUrl);
				subscribeRequest.setEndpoint(queueArn);
				subscribeRequest.setProtocol("cqs");
				subscribeRequest.setTopicArn(topicArn);
				
				cns1.subscribe(subscribeRequest);
			}
			
			logger.info("Listing subscriptions");
			
			ListSubscriptionsByTopicRequest listSubscriptionsByTopicRequest = new ListSubscriptionsByTopicRequest();
			listSubscriptionsByTopicRequest.setTopicArn(topicArn);
			
			ListSubscriptionsByTopicResult listSubscriptionsByTopicResult = cns1.listSubscriptionsByTopic(listSubscriptionsByTopicRequest);
			
			assertTrue("First page should contain 100 subscriptions, found instead " + listSubscriptionsByTopicResult.getSubscriptions().size(),listSubscriptionsByTopicResult.getSubscriptions().size() == 100);
			
			listSubscriptionsByTopicRequest = new ListSubscriptionsByTopicRequest();
			listSubscriptionsByTopicRequest.setTopicArn(topicArn);
			listSubscriptionsByTopicRequest.setNextToken(listSubscriptionsByTopicResult.getNextToken());
			
			listSubscriptionsByTopicResult = cns1.listSubscriptionsByTopic(listSubscriptionsByTopicRequest);

			assertTrue("Second page should contain 10 subscriptions, found instead " + listSubscriptionsByTopicResult.getSubscriptions().size(), listSubscriptionsByTopicResult.getSubscriptions().size() == 10);
		
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}
	
	@Test
	public void testTopicPermissions() {
		
		String topicArn = null;

		try {
			
			topicArn = getTopic(1, USR.USER1);
			
			logger.info("Created topic " + topicArn + ", now setting attributes");
			
			AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setTopicArn(topicArn);
	        addPermissionRequest.setActionNames(Arrays.asList("SetTopicAttributes", "Publish"));
	        addPermissionRequest.setLabel("P1");
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
	        cns1.addPermission(addPermissionRequest);
	        
	        SetTopicAttributesRequest setTopicAttributesRequest = new SetTopicAttributesRequest();
	        setTopicAttributesRequest.setAttributeName("DisplayName");
	        setTopicAttributesRequest.setAttributeValue("NewDisplayName");
	        setTopicAttributesRequest.setTopicArn(topicArn);
	        cns2.setTopicAttributes(setTopicAttributesRequest);
			
	        GetTopicAttributesRequest getTopicAttributesRequest = new GetTopicAttributesRequest();
	        getTopicAttributesRequest.setTopicArn(topicArn);
	        GetTopicAttributesResult result = cns1.getTopicAttributes(getTopicAttributesRequest);
	        
	        assertTrue("Expected display name NewDisplayName, instead found " + result.getAttributes().get("DisplayName"), result.getAttributes().get("DisplayName").equals("NewDisplayName"));
	
	        RemovePermissionRequest removePermissionRequest = new RemovePermissionRequest();
	        removePermissionRequest.setTopicArn(topicArn);
	        removePermissionRequest.setLabel("P1");
	        cns1.removePermission(removePermissionRequest);
	        
	        logger.info("Now trying to do things without permission");
	        
	        try {
	            setTopicAttributesRequest = new SetTopicAttributesRequest();
	            setTopicAttributesRequest.setAttributeName("DisplayName");
	            setTopicAttributesRequest.setAttributeValue("NewDisplayName2");
	            setTopicAttributesRequest.setTopicArn(topicArn);
	            cns2.setTopicAttributes(setTopicAttributesRequest);
	            fail("missing expected exception");
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("don't have permission"));
	        }
	        
	        // test publish permission
	        
	        try {
		        PublishRequest publishRequest = new PublishRequest();
		        publishRequest.setMessage("hello world!!!");
		        publishRequest.setTopicArn(topicArn);
		        cns2.publish(publishRequest);
	            fail("missing expected exception");
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("don't have permission"));
	        }
	
	        addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setTopicArn(topicArn);
	        addPermissionRequest.setActionNames(Arrays.asList("Publish"));
	        addPermissionRequest.setLabel("P2");
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
	        cns1.addPermission(addPermissionRequest);

	        PublishRequest publishRequest = new PublishRequest();
	        publishRequest.setMessage("hello world!!!");
	        publishRequest.setTopicArn(topicArn);
	        cns2.publish(publishRequest);

	        removePermissionRequest = new RemovePermissionRequest();
	        removePermissionRequest.setTopicArn(topicArn);
	        removePermissionRequest.setLabel("P2");
	        cns1.removePermission(removePermissionRequest);

	        try {
		        publishRequest = new PublishRequest();
		        publishRequest.setMessage("hello world!!!");
		        publishRequest.setTopicArn(topicArn);
		        cns2.publish(publishRequest);
		        fail("missing expected exception");
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("don't have permission"));
	        }
	
	        // test add permission
	        
	        addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setTopicArn(topicArn);
	        addPermissionRequest.setActionNames(Arrays.asList("AddPermission"));
	        addPermissionRequest.setLabel("P3");
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
	        cns1.addPermission(addPermissionRequest);
	        
	        addPermissionRequest = new AddPermissionRequest();
	        addPermissionRequest.setTopicArn(topicArn);
	        addPermissionRequest.setActionNames(Arrays.asList("Publish"));
	        addPermissionRequest.setLabel("P4");
	        addPermissionRequest.setAWSAccountIds(Arrays.asList(user3.getUserId()));        
	        cns2.addPermission(addPermissionRequest);
	        
	        publishRequest = new PublishRequest();
	        publishRequest.setMessage("hello world!!!");
	        publishRequest.setTopicArn(topicArn);
	        cns3.publish(publishRequest);
	        
	        // try some invalid stuff
	        
	        try {
		        addPermissionRequest = new AddPermissionRequest();
		        addPermissionRequest.setTopicArn(topicArn);
		        addPermissionRequest.setActionNames(Arrays.asList("AddBoris"));
		        addPermissionRequest.setLabel("P5");
		        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
		        cns1.addPermission(addPermissionRequest);
		        fail("missing expected exception");
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("Invalid action parameter"));
	        }
	
	        try {
		        addPermissionRequest = new AddPermissionRequest();
		        addPermissionRequest.setTopicArn(topicArn);
		        addPermissionRequest.setActionNames(Arrays.asList(""));
		        addPermissionRequest.setLabel("P6");
		        addPermissionRequest.setAWSAccountIds(Arrays.asList(user2.getUserId()));        
		        cns1.addPermission(addPermissionRequest);
		        fail("missing expected exception");
	        } catch (Exception ex) {
	        	assertTrue(ex.getMessage().contains("Blank action parameter"));
	        }
	
		} catch (Exception ex) {
			logger.error("test failed", ex);
			fail(ex.getMessage());
		}
	}
}
