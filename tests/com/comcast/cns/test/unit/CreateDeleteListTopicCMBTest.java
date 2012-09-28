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

		} catch (Exception ex) {
			logger.error("setup failed", ex);
			assertFalse(true);
		}
	}

	@After
	public void tearDown() {
		
		try {
			
			IUserPersistence dao = PersistenceFactory.getUserPersistence();
			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			CNSTestingUtils.deleteallTopics(cns, user, out);

			dao.deleteUser(user.getUserName());

		} catch (Exception e) {
			logger.error("tearDown failed Exception" + e, e);
			fail("tearDown failed Exception");
		}
		
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}

	@Test
	public void testCreateDeleteListTopic() {

		try {

			ICNSTopicPersistence topicHandler = PersistenceFactory.getTopicPersistence();
			ICNSAttributesPersistence attributeHandler = PersistenceFactory.getCNSAttributePersistence();

			String topicName1 = "T" + rand.nextLong();
			String topicName2 = "T" + rand.nextLong();

			CNSTopic topic1 = topicHandler.createTopic(topicName1, topicName1, user.getUserId());

			// check default delivery policy on topic1

			CNSTopicAttributes attributes = attributeHandler.getTopicAttributes(topic1.getArn());

			assertTrue(attributes.getEffectiveDeliveryPolicy().getDefaultHealthyRetryPolicy().getNumRetries() == 3);

			CNSTopic topic2 = topicHandler.createTopic(topicName2, topicName2, user.getUserId());

			// attempt creating a duplicate topic, should return existing

			CNSTopic topic3 = topicHandler.createTopic(topicName2, topicName2, user.getUserId());

			assertTrue(topic2.equals(topic3));

			List<CNSTopic> topics = topicHandler.listTopics(user.getUserId(), null);
			
			int numTopics = topics.size();

			assertTrue(topics.size() >= 1);

			for (CNSTopic t : topics) {
				logger.info(t.toString());
			}

			CNSTopic topic = topicHandler.getTopic(topic1.getArn());
			
			assertTrue(topic.equals(topic1));

			assertFalse(topic1.equals(topic2));

			topicHandler.deleteTopic(topic1.getArn());
			topicHandler.deleteTopic(topic2.getArn());

			topics = topicHandler.listTopics(user.getUserId(), null);

			if (topics.size() != numTopics-2) {
				fail("Expected to get " +(numTopics-2) + " topics for user. Instead got:" + topics.size());
			}

		} catch (Exception ex) {

			logger.error("Exception", ex);
			fail("Test failed: " + ex.toString());
		}	
	}

	@Test
	public void testCreateDeleteListTopicServlet() {

		try {

			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			CNSTestingUtils.deleteallTopics(cns, user, out);
			out.reset();

			try {
				CNSTestingUtils.addTopic(cns,user,out,"MyTopic2");
			} catch (Exception e) {
				assertFalse(true);
			}

			String res = out.toString();
			String arn = CNSTestingUtils.getArnFromString(res);
			out.reset();
			
			try {
				CNSTestingUtils.listTopics(cns,user,out,null);
			} catch (Exception e) {
				assertTrue(false);
			}
			
			res = out.toString();
			Vector<String> arns = CNSTestingUtils.getArnsFromString(res);
			assertTrue(arns.size() == 1);
			out.reset();
			
			try {
				CNSTestingUtils.deleteTopic(cns,user,out,arn);
			} catch (Exception e) {
				assertTrue(false);
			}
			out.reset();
			
			try {
				CNSTestingUtils.listTopics(cns,user,out,null);
			} catch (Exception e) {
				assertTrue(false);
			}

			res = out.toString();
			arns = CNSTestingUtils.getArnsFromString(res);
			out.reset();

			assertTrue(arns.size() == 0);

			try {
				CNSTestingUtils.deleteTopic(cns,user,out,arn);
			} catch (Exception e) {
				assertFalse(true);
			}

			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res,"NotFound","Topic not found."));
			out.reset();

			try {
				CNSTestingUtils.listTopics(cns,user,out,null);
			} catch (Exception e) {
				assertTrue(false);
			}
			
			res = out.toString();
			arns = CNSTestingUtils.getArnsFromString(res);
			
			assertTrue(arns.size() == 0);

		} catch (Exception ex) {
			logger.error("Exception", ex);
			fail("Test failed: " + ex.toString());
		}	
	}

	@Test
	public void testCreateDeleteListTopicServlet2() {

		try {

			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			CNSTestingUtils.deleteallTopics(cns, user, out);
			out.reset();

			String res;
			Vector<String> arns;

			try {
				CNSTestingUtils.listTopics(cns,user,out,null);
			} catch (Exception e) {
				assertTrue("exception on list topics", false);
			}
			
			res = out.toString();
			arns = CNSTestingUtils.getArnsFromString(res);
			assertTrue(arns.size() == 0);

			try {

				for (int i=0; i<100; i++) {
					CNSTestingUtils.addTopic(cns,user,out,"MyTopic" + i);
					out.reset();
				}
				
			} catch (Exception e) {
				logger.error("Exception", e);
				assertFalse("exception on adding 100 topics", true);
			}

			out.reset();

			try {
				CNSTestingUtils.listTopics(cns,user,out,null);
			} catch (Exception e) {
				assertTrue("exception on listTopics",false);
			}
			
			res = out.toString();
			arns = CNSTestingUtils.getArnsFromString(res);
			assertTrue("Incorrect number of topics, expected 100",arns.size() == 100);
			out.reset();

			try {
				int i = 100;
				CNSTestingUtils.addTopic(cns,user,out,"MyTopic" + i);
			} catch (Exception e) {
				assertFalse("exception on addTopic 100",true);				
			}
			
			res = out.toString();
			assertTrue(CNSTestingUtils.verifyErrorResponse(res,"TopicLimitExceeded", "Topic limit exceeded."));
			out.reset();

			try {

				for (String arn:arns) {
					CNSTestingUtils.deleteTopic(cns,user,out,arn);
					out.reset();
				}

			} catch (Exception e) {
				logger.error("Exception", e);
				assertFalse("Exception deleting all topics", true);
			}
			
			out.reset();

			try {
				CNSTestingUtils.listTopics(cns,user,out,null);
			} catch (Exception e) {
				assertTrue("Exception listing all topics", false);
			}

			res = out.toString();
			arns = CNSTestingUtils.getArnsFromString(res);
			assertTrue("Incorrect number of topics, expected 0", arns.size() == 0);

		} catch (Exception ex) {
			logger.error("Exception", ex);
			fail("Test failed: " + ex.toString());
		}	
	}

	@Test
	public void testBadInputs() {
		
		try {
			
			CNSControllerServlet cns = new CNSControllerServlet();
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			CNSTestingUtils.deleteallTopics(cns, user, out);
			out.reset();

			Vector<String> arns;
			CNSTestingUtils.listTopics(cns,user,out,"faketoken");
			logger.debug("resp:" + out.toString());
			arns = CNSTestingUtils.getArnsFromString(out.toString());
			assertTrue(arns.size() == 0);
			out.reset();

			String topicName = null;
			CNSTestingUtils.addTopic(cns, user, out, topicName);
			logger.debug("resp: " + out.toString());
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

			CNSTestingUtils.testCommand(cns, user, out, "FakeCommand");
			logger.debug("resp: " + out.toString());
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidAction", "FakeCommand is not a valid action"));
			out.reset();

			String topicArn = null;
			CNSTestingUtils.deleteTopic(cns, user, out, topicArn);
			logger.debug("resp: " + out.toString());
			assertTrue(CNSTestingUtils.verifyErrorResponse(out.toString(), "InvalidParameter", "request parameter does not comply with the associated constraints."));
			out.reset();

		} catch (Exception ex) {
			logger.error("Exception", ex);
			fail("Test failed: " + ex.toString());
		}	
	}
}
