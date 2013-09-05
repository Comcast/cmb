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
package com.comcast.cqs.test.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.CQSQueueCassandraPersistence;
import com.comcast.cqs.persistence.ICQSQueuePersistence;
import com.comcast.cqs.util.CQSConstants;

public class CQSQueuePersistenceTest {

    protected static Logger logger = Logger.getLogger(CQSQueuePersistenceTest.class);
    private User user = null;
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 

    @Before
    public void setup() throws Exception {
    	
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();

        IUserPersistence userPersistence = new UserCassandraPersistence();
        user = userPersistence.getUserByName("cqs_unit_test");

        if (user == null) {
            user = userPersistence.createUser("cqs_unit_test", "cqs_unit_test");
        }
        
        ICQSQueuePersistence persistence = new CQSQueueCassandraPersistence();
		
		List<CQSQueue> queueList = persistence.listQueues(user.getUserId(), null, false);
		
		for (CQSQueue queue: queueList) {
			persistence.deleteQueue(queue.getRelativeUrl());
		}
    }
	
	@Test
	public void testCreateListDeleteQueue() {

		try {
			
			String region = CMBProperties.getInstance().getRegion();
			
			ICQSQueuePersistence persistence = new CQSQueueCassandraPersistence();
			deleteQueuesTest();
			
			List<CQSQueue> queueList = persistence.listQueues(user.getUserId(), null, false);
			assertEquals(queueList.size(), 0);

	    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
			
			CQSQueue queue = new CQSQueue(queueName, user.getUserId());
			persistence.createQueue(queue);
			
			queueName = QUEUE_PREFIX + randomGenerator.nextLong();
			queue = new CQSQueue(queueName, user.getUserId());
			persistence.createQueue(queue);

			queueName = QUEUE_PREFIX + randomGenerator.nextLong();
			queue = new CQSQueue(queueName, user.getUserId());

			persistence.createQueue(queue);
			
			queue = persistence.getQueue(user.getUserId(), queueName);
			assertQueue(region, queue);

			assertTrue(queue.getName().equals(queueName));
			
			queueList = persistence.listQueues(user.getUserId(), QUEUE_PREFIX, false);
			assertEquals(queueList.size(), 3);

			for (CQSQueue queue1 : queueList) {
				assertQueue(region, queue1);
			}
			
			assertEquals(queueList.size(), 3);
			
			for (CQSQueue queue1 : queueList) {
				assertQueue(region, queue1);
				persistence.deleteQueue(queue1.getRelativeUrl());
			}						

			queueList = persistence.listQueues(user.getUserId(), null, false);
			assertEquals(queueList.size(), 0);
					
		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("Test failed: " + ex.toString());
		}		   
	}

	private void assertQueue(String region, CQSQueue queue) {
		assertNotNull(queue);
		assertNotNull(queue.getArn());
		assertNotNull(queue.getRelativeUrl());
		assertTrue(queue.getCreatedTime() > 0);
		assertTrue(queue.getDelaySeconds() >= 0);
		assertNotNull(queue.getName());
		assertTrue(queue.getMaxMsgSize() > 0);
		assertTrue(queue.getMsgRetentionPeriod() > 0);
		assertNotNull(queue.getOwnerUserId());
		assertTrue(queue.getRegion().equals(region));
		assertTrue(queue.getVisibilityTO() > 0);
	}
	
	public void deleteQueuesTest() {
		
		ICQSQueuePersistence persistence = new CQSQueueCassandraPersistence();
		List<CQSQueue> queueList;
		
		try {
			
			queueList = persistence.listQueues(user.getUserId(), null, false);
			
			for (CQSQueue queue1 : queueList) {
				persistence.deleteQueue(queue1.getRelativeUrl());
			}				
			
		} catch (PersistenceException ex) {
            logger.error("test failed", ex);
			fail("Test failed: " + ex.toString());
		}
	}
	
	@Test
	public void updatePolicyTest() {
		
		ICQSQueuePersistence persistence = new CQSQueueCassandraPersistence();
		String policy = "{\n" +
				  "\"Version\": \"2008-10-17\",\n" +
				  "\"Id\": \"arn:aws:cqs:us-west-1:544265793414:MyTestQueue1/CQSDefaultPolicy\",\n" +
				  "\"Statement\": [\n" +
				    "{\n" +
				      "\"Sid\": \"Sid1331673493955\",\n" +
				      "\"Effect\": \"Allow\",\n" +
				      "\"Principal\": {\n" +
				        "\"AWS\": \"*\"\n" +
				      "},\n" +
				      "\"Action\": [\n" +
				        "\"CQS:ReceiveMessage\",\n" +
				        "\"CQS:SendMessage\"\n" +
				      "],\n" +
				      "\"Resource\": \"arn:aws:cqs:us-west-1:544265793414:MyTestQueue1\"\n" +
				    "}\n" +
				  "]\n" +
				"}";
		
		try {
			
	    	String queueName1 = QUEUE_PREFIX + randomGenerator.nextLong();
			CQSQueue queue1 = new CQSQueue(queueName1, user.getUserId());
			queue1.setPolicy(policy);
			persistence.createQueue(queue1);
			
	    	String queueName2 = QUEUE_PREFIX + randomGenerator.nextLong();
			CQSQueue queue2 = new CQSQueue(queueName2, user.getUserId());
			persistence.createQueue(queue2);
			persistence.updatePolicy(queue2.getRelativeUrl(), policy);
			
			for (CQSQueue e: persistence.listQueues(user.getUserId(), QUEUE_PREFIX, false)) {
				CQSQueue q = persistence.getQueue(e.getRelativeUrl());
				assertTrue(policy.equals(q.getPolicy()));
				persistence.deleteQueue(e.getRelativeUrl());
			}
			
		} catch (PersistenceException e) {
			fail("Test failed:" +  e.toString());
		}		
	}
	
	@Test
	public void testSetGetAttributes() {

		try {
			
			String userId = user.getUserId();
			
			ICQSQueuePersistence persistence = new CQSQueueCassandraPersistence();
			//deleteQueuesTest();
			
			List<CQSQueue> queueList = persistence.listQueues(userId, null, false);
			assertEquals(queueList.size(), 0);

	    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
			CQSQueue queue = new CQSQueue(queueName, userId);
			persistence.createQueue(queue);
			
			queueList = persistence.listQueues(userId, null, false);
			assertEquals(queueList.size(), 1);
			
			String queueURL = queue.getRelativeUrl();
			String key = CQSConstants.COL_VISIBILITY_TO;
			String value = "5";
			String key2 = CQSConstants.COL_MSG_RETENTION_PERIOD;
			String value2 = "30";
			String key3 = CQSConstants.COL_MAX_MSG_SIZE;
			String value3 = "4002";
			String key4 = CQSConstants.COL_DELAY_SECONDS;
			String value4 = "26";
			String key5 = CQSConstants.COL_POLICY;
			String value5 = "no";

			HashMap<String, String> values = new HashMap<String, String>();
			values.put(key, value);
			values.put(key2, value2);
			values.put(key3, value3);
			values.put(key4, value4);
			values.put(key5, value5);
			persistence.updateQueueAttribute(queueURL, values);
			queueName = queue.getName();
			CQSQueue queue2 = persistence.getQueue(userId, queueName);
			assertTrue(queue2.getVisibilityTO() == 5);
			assertTrue(queue2.getMsgRetentionPeriod() == 30);
			assertTrue(queue2.getMaxMsgSize() == 4002);
			assertTrue(queue2.getDelaySeconds() == 26);
			assertTrue(queue2.getPolicy().equals("no"));
			
			key3 = CQSConstants.COL_MAX_MSG_SIZE;
			value3 = "4150";
			key4 = CQSConstants.COL_DELAY_SECONDS;
			value4 = "20";
			
			HashMap<String, String> values2 = new HashMap<String, String>();
			values2.put(key3, value3);
			values2.put(key4, value4);
			persistence.updateQueueAttribute(queueURL, values2);
			
			CQSQueue queue3 = persistence.getQueue(userId, queueName);
			assertTrue(queue3.getVisibilityTO() == 5);
			assertTrue(queue3.getMsgRetentionPeriod() == 30);
			assertTrue(queue3.getMaxMsgSize() == 4150);
			assertTrue(queue3.getDelaySeconds() == 20);
			assertTrue(queue3.getPolicy().equals("no"));
			
			key = CQSConstants.COL_VISIBILITY_TO;
			value = "8";
			key2 = CQSConstants.COL_MSG_RETENTION_PERIOD;
			value2 = "32";

			HashMap<String, String> values3 = new HashMap<String, String>();
			values3.put(key, value);
			values3.put(key2, value2);
			persistence.updateQueueAttribute(queueURL, values3);
			
			CQSQueue queue4 = persistence.getQueue(userId, queueName);
			assertTrue(queue4.getVisibilityTO() == 8);
			assertTrue(queue4.getMsgRetentionPeriod() == 32);
			assertTrue(queue4.getMaxMsgSize() == 4150);
			assertTrue(queue4.getDelaySeconds() == 20);
			assertTrue(queue4.getPolicy().equals("no"));
			
			queueList = persistence.listQueues(user.getUserId(), null, false);
			
			for(CQSQueue queue_: queueList) {
				persistence.deleteQueue(queue_.getRelativeUrl());
			}
			
		} catch (PersistenceException e) {
			fail("Test failed:" +  e.toString());
		}		
	}
	
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
