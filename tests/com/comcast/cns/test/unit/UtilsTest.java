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

import static org.junit.Assert.*;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.util.Util;
import com.comcast.cqs.util.RandomNumberCollection;

public class UtilsTest {

	private static Logger logger = Logger.getLogger(UtilsTest.class);

	@Before
	public void setup() throws Exception {
		com.comcast.cmb.common.util.Util.initLog4jTest();
		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		PersistenceFactory.reset();
	}

	@Test
	public void testValidArn() {
		assertTrue(Util.isValidTopicArn("arn:cmb:cns:us-east-1:266126687520:Topic94"));	    
		assertFalse(Util.isValidTopicArn(CMBTestingConstants.EMAIL_ENDPOINT));	  
		assertTrue(Util.isValidTopicArn(Util.generateCnsTopicArn("Foo", "myregion", "12345")));
		assertTrue(Util.isValidTopicName("Foo-9"));
		assertFalse(Util.isValidTopicName("Foo!"));
		try {
			assertTrue(Util.isValidSubscriptionArn(Util.generateCnsTopicSubscriptionArn(Util.generateCnsTopicArn("Foo", "myregion", "12345"), CnsSubscriptionProtocol.http, "http://abc.com")));
			String sarn1 = Util.generateCnsTopicSubscriptionArn(Util.generateCnsTopicArn("Foo", "myregion", "12345"), CnsSubscriptionProtocol.http, "http://abc.com");
			String sarn2 = Util.generateCnsTopicSubscriptionArn(Util.generateCnsTopicArn("Foo", "myregion", "12345"), CnsSubscriptionProtocol.http, "http://abc.com");
			String sarn3 = Util.generateCnsTopicSubscriptionArn(Util.generateCnsTopicArn("Foo", "myregion", "12345"), CnsSubscriptionProtocol.http, "http://xyz.com");
			String sarn4 = Util.generateCnsTopicSubscriptionArn(Util.generateCnsTopicArn("Foo", "myregion", "12345"), CnsSubscriptionProtocol.cqs, "http://abc.com");
			logger.info("sarn1=" + sarn1);
			logger.info("sarn2=" + sarn2);
			logger.info("sarn3=" + sarn3);
			logger.info("sarn4=" + sarn4);
			assertTrue("sub arn mismatch", sarn1.equals(sarn2));
			assertTrue("sub arn mismatch", !sarn1.equals(sarn3));
			assertTrue("sub arn mismatch", !sarn1.equals(sarn4));
		} catch (NoSuchAlgorithmException e) {
			fail("faild to test valid subscription arn");
		}
	}

	@Test
	public void testMapEquals() {
		HashMap<String, String> m1 = new HashMap<String, String>();
		HashMap<String, String> m2 = new HashMap<String, String>();

		if (com.comcast.cmb.common.util.Util.isMapsEquals(m1, null)) {
			fail("Should have failed");
		}
		if (com.comcast.cmb.common.util.Util.isMapsEquals(null, m1)) {
			fail("Should have failed");
		}
		if (!com.comcast.cmb.common.util.Util.isMapsEquals(m1, m2)) {
			fail("Should have succeeded");
		}
		m1.put("foo", "val");
		m2.put("foo", "val");
		if (!com.comcast.cmb.common.util.Util.isMapsEquals(m1, m2)) {
			fail("Should have succeeded");
		}
		m1.put("foo2", "val");
		m2.put("foo2", "val");
		if (!com.comcast.cmb.common.util.Util.isMapsEquals(m1, m2)) {
			fail("Should have succeeded");
		}
		m1.put("foo3", "val2");
		m2.put("foo3", "val");
		if (com.comcast.cmb.common.util.Util.isMapsEquals(m1, m2)) {
			fail("Should have failed");
		}

	}

	@Test
	public void testSubList() {
		LinkedList<String> list = new LinkedList<String>();
		for (int i= 0; i<10;i++) {
			list.add(""+i);
		}
		List<List<String>> lofl = com.comcast.cmb.common.util.Util.splitList(list, 3);
		if (lofl.size() != 4) {
			fail("Expected 3 sublists");
		}
		if (lofl.get(0).size() != 3 || lofl.get(1).size() != 3 || lofl.get(2).size() != 3 || lofl.get(3).size() != 1) {
			fail("Wrong sizes");
		}

		List<String> list2 =  list.subList(0, 3);
		lofl = com.comcast.cmb.common.util.Util.splitList(list2, 3);
		if (lofl.size() != 1) {
			fail("Expected jsut one lofl");
		}
		if (lofl.get(0).size() != 3) {
			fail("Expected 3");
		}
	}

	@Test
	public void testRandomNumCol() throws Exception {
		RandomNumberCollection c = new RandomNumberCollection(10);
		Set<Integer> seen = new HashSet<Integer>();
		for(int i = 0; i < 10; i++) {
			int num = c.getNext();
			logger.info("got num=" + num);
			if (seen.contains(num)) {
				fail("returned " + num + " which was already returned");
			}
			seen.add(num);
		}

		c = new RandomNumberCollection(1);
		if (c.getNext() != 0) {
			fail("Expected 0");
		}
	}

	@After    
	public void tearDown() {
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}



}
