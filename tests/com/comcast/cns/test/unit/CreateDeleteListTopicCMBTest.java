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

import java.io.ByteArrayOutputStream;
import java.util.*;

import org.apache.log4j.Logger;
import org.jfree.util.Log;
import org.junit.* ;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.cns.controller.CNSControllerServlet;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.persistence.ICNSAttributesPersistence;
import com.comcast.cns.persistence.ICNSTopicPersistence;

import static org.junit.Assert.*;

public class CreateDeleteListTopicCMBTest {

	private static Logger logger = Logger.getLogger(CreateDeleteListTopicCMBTest.class);

	private User user;
	private Random rand = new Random();

	@Before
	public void setup() {

		try {

			Util.initLog4jTest();
			CMBControllerServlet.valueAccumulator.initializeAllCounters();
	        PersistenceFactory.reset();

			IUserPersistence userHandler = new UserCassandraPersistence();
			String userName = "cns_unit_test";
			user = userHandler.getUserByName(userName);

			if (user == null) {
				user =  userHandler.createUser(userName, userName);
			}

			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			CNSTestingUtils.deleteallTopics(cns, user, out);

		} catch (Exception ex) {
			fail(ex.toString());
		}
	}

	@After
	public void tearDown() {
		
		try {
			
			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			CNSTestingUtils.deleteallTopics(cns, user, out);

		} catch (Exception ex) {
			fail(ex.toString());
		}
		
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}

	@Test
	public void testCreateDeleteListTopicWithCassandraHandler() {

		try {

			ICNSTopicPersistence topicHandler = PersistenceFactory.getTopicPersistence();
			ICNSAttributesPersistence attributeHandler = PersistenceFactory.getCNSAttributePersistence();

			String topicName1 = "T" + rand.nextLong();
			String topicName2 = "T" + rand.nextLong();

			CNSTopic topic1 = topicHandler.createTopic(topicName1, topicName1, user.getUserId());
			
			Log.info("Created topic " + topic1.getArn() + " using topic handlers");

			// check default delivery policy on topic1

			CNSTopicAttributes attributes = attributeHandler.getTopicAttributes(topic1.getArn());

			assertTrue("Incorrect number of retries on default delivery policy (expected 3)", attributes.getEffectiveDeliveryPolicy().getDefaultHealthyRetryPolicy().getNumRetries() == 3);

			CNSTopic topic2 = topicHandler.createTopic(topicName2, topicName2, user.getUserId());

			// attempt creating a duplicate topic, should return existing

			CNSTopic topic3 = topicHandler.createTopic(topicName2, topicName2, user.getUserId());

			assertTrue("Created duplicate topic, expected to get existing one instead", topic2.equals(topic3));

			List<CNSTopic> topics = topicHandler.listTopics(user.getUserId(), null);
			
			int numTopics = topics.size();

			assertTrue("Expected at least 2 topics, instead found " + topics.size(), topics.size() >= 2);

			CNSTopic topic = topicHandler.getTopic(topic1.getArn());
			
			assertTrue("Expected topic " + topic1.getArn() + ", instead found " + topic.getArn(), topic.equals(topic1));

			assertFalse("Expected 2 distinct topics", topic1.equals(topic2));

			topicHandler.deleteTopic(topic1.getArn());
			topicHandler.deleteTopic(topic2.getArn());

			topics = topicHandler.listTopics(user.getUserId(), null);

			if (topics.size() != numTopics-2) {
				fail("Expected to get " + (numTopics-2) + " topics for user, instead found " + topics.size());
			}

		} catch (Exception ex) {
			fail(ex.toString());
		}	
	}

	@Test
	public void testCreateDeleteListTopicWithServlet() throws Exception {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		String topicName = "T" + rand.nextLong();
		CNSTestingUtils.addTopic(cns, user, out, topicName);

		String res = out.toString();
		String arn = CNSTestingUtils.getArnFromString(res);
		out.reset();
		
		CNSTestingUtils.listTopics(cns,user,out,null);
		
		res = out.toString();
		Vector<String> arns = CNSTestingUtils.getArnsFromString(res);
		
		assertTrue("Expected one topic, instead found " + arns.size(), arns.size() == 1);
		
		out.reset();
		
		CNSTestingUtils.deleteTopic(cns,user,out,arn);
		out.reset();
		
		CNSTestingUtils.listTopics(cns,user,out,null);

		res = out.toString();
		arns = CNSTestingUtils.getArnsFromString(res);
		out.reset();

		assertTrue("Expected 0 topics, instead found " + arns.size(), arns.size() == 0);

		try {
			CNSTestingUtils.deleteTopic(cns, user, out, arn);
		} catch (Exception ex) {
		}
		
		res = out.toString();
		assertTrue(CNSTestingUtils.verifyErrorResponse(res,"NotFound","Topic not found."));
		out.reset();

		CNSTestingUtils.listTopics(cns, user, out, null);
		
		res = out.toString();
		arns = CNSTestingUtils.getArnsFromString(res);
		
		assertTrue("Expected 0 topics, instead found " + arns.size(), arns.size() == 0);
	}

	@Test
	public void testCreateDeleteListManyTopicsWihtServlet() throws Exception {

		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		String res;
		Vector<String> arns;

		CNSTestingUtils.listTopics(cns, user, out, null);
		
		res = out.toString();
		arns = CNSTestingUtils.getArnsFromString(res);
		
		assertTrue(arns.size() == 0);

		logger.info("Adding 100 topics");
		
		for (int i=0; i<100; i++) {
			CNSTestingUtils.addTopic(cns, user, out, "MyTopic" + i);
			out.reset();
		}

		out.reset();

		CNSTestingUtils.listTopics(cns, user, out, null);
		
		res = out.toString();
		arns = CNSTestingUtils.getArnsFromString(res);
		assertTrue("Expected 100 topics, instead found " + arns.size(), arns.size() == 100);
		out.reset();

		int i = 100;
		
		try {
			CNSTestingUtils.addTopic(cns, user, out, "MyTopic" + i);
		} catch (Exception ex) {
			
		}
		
		res = out.toString();
		
		assertTrue("Expected topic limit exceeded exception", CNSTestingUtils.verifyErrorResponse(res,"TopicLimitExceeded", "Topic limit exceeded."));
		
		out.reset();

		for (String arn:arns) {
			CNSTestingUtils.deleteTopic(cns, user, out, arn);
			out.reset();
		}

		CNSTestingUtils.listTopics(cns, user, out, null);

		res = out.toString();
		arns = CNSTestingUtils.getArnsFromString(res);
		assertTrue("Expected 0 topics, instead found " + arns.size(), arns.size() == 0);
	}

	@Test
	public void testTopicServletWithBadInputs() throws Exception {
		
		CNSControllerServlet cns = new CNSControllerServlet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		Vector<String> arns;
		CNSTestingUtils.listTopics(cns, user, out, "faketoken");
		arns = CNSTestingUtils.getArnsFromString(out.toString());
		assertTrue("Expected 0 topics, instead gto " + arns.size(), arns.size() == 0);
		out.reset();

		String topicName = null;
		CNSTestingUtils.addTopic(cns, user, out, topicName);
		assertTrue("Expected invalid parameter error", CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidParameter", "request parameter does not comply with the associated constraints."));
		out.reset();

		CNSTestingUtils.testCommand(cns, user, out, "FakeCommand");
		assertTrue("Expected invalid action error for FakeCommand", CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidAction", "FakeCommand is not a valid action"));
		out.reset();

		String topicArn = null;
		CNSTestingUtils.deleteTopic(cns, user, out, topicArn);
		assertTrue("Expected invalid parameter error", CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidParameter", "request parameter does not comply with the associated constraints."));
		out.reset();
	}
}
