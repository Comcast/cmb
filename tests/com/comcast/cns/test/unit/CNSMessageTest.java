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

import java.util.Date;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSMessage.CNSMessageStructure;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CNSMessageTest {

	private static Logger log = Logger.getLogger(CNSMessageTest.class);

	@Before
	public void setup() throws Exception {
		Util.initLog4jTest();
		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		PersistenceFactory.reset();
	}

	@Test
	public void testMessage() {

		CNSMessage msg = new CNSMessage();
		msg.generateMessageId();
        msg.setTimestamp(new Date());
		msg.setUserId("test");

		try {
			msg.checkIsValid();
			fail("Should have thrown an invalid message exception");
		} catch(Exception e) {
		}

		msg.setMessage("test message");

		try {
			msg.checkIsValid();
			fail("Should have thrown and invalid message expection");
		} catch(Exception e) {
		}

		msg.setTopicArn("test-topic-arn");
		
		try {
			msg.checkIsValid();            
		} catch (Exception e) {
			fail("Should NOT have thrown and invalid message expection: " + e.getMessage());
		} 
		
		if (!msg.equals(msg)) {
			fail("Equals not reflexive for message");
		}

		msg.setMessage("test message");

		msg.setMessageStructure(CNSMessageStructure.json);
		
		try {
			msg.checkIsValid();
			fail("Should have thrown expection since message is not JSON");
		} catch (Exception e) {
			
		}

		msg.setMessage("{\"bogus\":\"val\"}");
		
		try {
			msg.checkIsValid();
			fail("Should have thrown expection since message is not JSON");
		} catch (Exception e) {
		}

		msg.setMessage("{\"email\"}:\"val\"}");

		try {
			msg.checkIsValid();
			fail("Should have thrown expection since message default is not specified");
		} catch (Exception e) {
		}

		msg.setMessage("{\"default\":\"send please\",\"email-json\":\"send in json\",\"email\":\"send in email\"}");
		
		try {
			msg.checkIsValid();            
		} catch(Exception e){
			fail("Should NOT have thrown expection: " + e.getMessage());
		}
		
		log.info(msg);

		StringBuffer sb = new StringBuffer();
		
		for (int i = 0; i < 100; i++) {
			sb.append('a');
		}
		
		msg.setSubject(sb.toString());
		
		try {
			msg.checkIsValid();
			fail("Should have thrown expection since message subject is too long");
		} catch(Exception e) {
		}
	}

	public static CNSMessage getMessage(String msg, CNSMessageStructure structure, String subject, String topicArn, String userId) {
		
		CNSMessage message = new CNSMessage();
        message.setTimestamp(new Date());
		message.setMessage(msg);
		message.setMessageStructure(structure);
		message.setSubject(subject);
		message.setTopicArn(topicArn);
		message.setUserId(userId);
		message.setMessageId("test-message-id1");
		
		return message;
	}

	@Test
	public void testSerializeAndDeserializeMessage() {

		CNSMessage p1 = getMessage("test", CNSMessageStructure.json, "test", "test-arn", "test-pub-userId");

		String str = p1.serialize();

		CNSMessage rec = CNSMessage.parseInstance(str);
		
		if (!p1.equals(rec)) {
			fail("Messages not equals after serialize / deserialize");
		}

		//null subject
		
		p1 = getMessage("test", CNSMessageStructure.json, null, "test-arn", "test-pub-userId");
		str = p1.serialize();

		rec = CNSMessage.parseInstance(str);
		
		if (!p1.equals(rec)) {
			fail("Messages not equals after serialize / deserialize");
		}

		//\n in message

		p1 = getMessage("testing \n 1 \n 2 \n 3<html>", CNSMessageStructure.json, "subject", "test-arn", "test-pub-userId");
		str = p1.serialize();

		rec = CNSMessage.parseInstance(str);
		if (!p1.equals(rec)) {
			fail("Messages not equals after serialize / deserialize");
		}
	}    

	@After    
	public void tearDown() {
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
}
