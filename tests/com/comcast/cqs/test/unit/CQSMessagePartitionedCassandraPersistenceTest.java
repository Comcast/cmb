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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.controller.CQSCache;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.CQSMessagePartitionedCassandraPersistence;
import com.comcast.cqs.persistence.CQSQueueCassandraPersistence;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.eaio.uuid.UUIDGen;

public class CQSMessagePartitionedCassandraPersistenceTest {

    protected static Logger logger = Logger.getLogger(CQSMessagePartitionedCassandraPersistenceTest.class);
	protected ICQSMessagePersistence persistence = new CQSMessagePartitionedCassandraPersistence();
	protected CQSQueueCassandraPersistence queuePersistence = new CQSQueueCassandraPersistence();
	protected CQSQueue queue = null;
	HashMap<String, String> attributes = null;
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 
	
	@Before
    public void setup() throws Exception {
		
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();

        IUserPersistence userPersistence = new UserCassandraPersistence();
        User user = userPersistence.getUserByName("cqs_unit_test");

        if (user == null) {
            user = userPersistence.createUser("cqs_unit_test", "cqs_unit_test");
        }
        
    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
        String queueUrl = CMBProperties.getInstance().getCQSServiceUrl() + user.getUserId() + "/" + queueName;

		attributes = new HashMap<String, String>();
        attributes.put("SenderId", user.getUserId());
        attributes.put("ApproximateReceiveCount", "0");
        attributes.put("ApproximateFirstReceiveTimestamp", "");			
		queue = queuePersistence.getQueue(queueUrl);
		
		if (queue == null) {
			queue = new CQSQueue("testQueue1824281", user.getUserId());
			queue.setRegion(CMBProperties.getInstance().getRegion());
			queuePersistence.createQueue(queue);
		}
		
		persistence.clearQueue(queue.getRelativeUrl(), 0);
    }

	@Test
	public void testSendMessage() throws NoSuchAlgorithmException, PersistenceException, IOException, InterruptedException, JSONException {
		
        CQSMessage message = new CQSMessage("This is a test message " + (new Random()).nextInt(), attributes);
        attributes.put("SentTimestamp", "" + Calendar.getInstance().getTimeInMillis());
		String receiptHandle = persistence.sendMessage(queue, 0, message);
		assertNotNull(receiptHandle);
	}
	
	@Test
	public void testSendMessageBatch() throws NoSuchAlgorithmException, PersistenceException, IOException, InterruptedException, JSONException {
		
        attributes.put("SentTimestamp", "" + Calendar.getInstance().getTimeInMillis());
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		
		for (int i=0; i<10; i++) {
			CQSMessage message = new CQSMessage("This is a test message id=" + i, attributes);
			message.setSuppliedMessageId("message_" + i);
			messageList.add(message);
		}
		
		Map<String, String> ret = persistence.sendMessageBatch(queue, 0, messageList);
		assertEquals(messageList.size(), ret.size());
	}

	@Test
	public void testGetMessages()  throws NoSuchAlgorithmException, PersistenceException, IOException, InterruptedException, JSONException {
		
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		
		for (int i=0; i<10; i++) {
			CQSMessage message = new CQSMessage("This is a test message id=" + i, attributes);
			messageList.add(message);
			persistence.sendMessage(queue, 0, message);
		}
		
		List<String> messageIdList = new ArrayList<String>(); 
		
		for (CQSMessage message: messageList) {
			messageIdList.add(message.getMessageId());
		}
		
		Map<String, CQSMessage> receivedMessages = persistence.getMessages(queue.getRelativeUrl(), messageIdList);
		assertTrue(receivedMessages.size() == messageIdList.size());
		
		for (CQSMessage message : messageList) {
			CQSMessage receivedMessage = receivedMessages.get(message.getMessageId());
			assertNotNull(receivedMessage);
			assertTrue(compareMessages(message, receivedMessage));
		}
	}
	
	@Test
	public void testGetMessagesBulk() throws PersistenceException, NoSuchAlgorithmException, IOException, InterruptedException, JSONException {
		
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		
		for (int i=0; i<105; i++) {
			CQSMessage message = new CQSMessage("This is a test message id=" + i, attributes);
			messageList.add(message);
			persistence.sendMessage(queue, 0, message);
		}
		
		List<String> messageIdList = new ArrayList<String>(); 
		
		for (CQSMessage message: messageList) {
			messageIdList.add(message.getMessageId());
		}
		
		Map<String, CQSMessage> receivedMessages = persistence.getMessages(queue.getRelativeUrl(), messageIdList);
		assertTrue(receivedMessages.size() == messageIdList.size());
		
		for (CQSMessage message : messageList) {
			CQSMessage receivedMessage = receivedMessages.get(message.getMessageId());
			assertNotNull(receivedMessage);
			assertTrue(compareMessages(message, receivedMessage));
		}
	}
	
	private boolean compareMessages(CQSMessage message1, CQSMessage message2) {
		
		return 
				(message1 != null) &&
				(message2 != null) &&
				(message1.getMessageId().equals(message2.getMessageId())) &&
				(message1.getBody().equals(message2.getBody()));
	}
	
	@Test 
	public void testPeekQueue() throws Exception {
		
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		
		for (int i=0; i<100; i++) {
			CQSMessage message = new CQSMessage("This is a test message id=" + i, attributes);
			messageList.add(message);
			persistence.sendMessage(queue, 0, message);
		}
		
		assertEquals(messageList.size(), getQueueMessageCount(queue.getRelativeUrl()));
		
		List<CQSMessage> peekMessageList = new ArrayList<CQSMessage>();
		List<CQSMessage> newMessageList = new ArrayList<CQSMessage>();
		String previousHandle = null;
		String nextHandle = null;
		int length = 25;
		
 		do {
			newMessageList = persistence.peekQueue(queue.getRelativeUrl(), 0, previousHandle, nextHandle, length);
			peekMessageList.addAll(newMessageList);
			
			if (newMessageList.size() > 0) {
				previousHandle = newMessageList.get(newMessageList.size() -1).getMessageId();
			}
		}
 		
		while (newMessageList.size() > 0);
 		
 		assertEquals(messageList.size(), peekMessageList.size());
 		previousHandle = null;
 		nextHandle = com.comcast.cqs.util.Util.hashQueueUrl(queue.getRelativeUrl()) + "_0_" + (CMBProperties.getInstance().getCQSNumberOfQueuePartitions()-1) +
 				":" + AbstractDurablePersistence.newTime(System.currentTimeMillis()+1209600000, false) + ":" + UUIDGen.getClockSeqAndNode();
 		peekMessageList.clear();
 		newMessageList.clear();
 		
 		do {
			newMessageList = persistence.peekQueue(queue.getRelativeUrl(), 0, previousHandle, nextHandle, length);			
			peekMessageList.addAll(newMessageList);
			
			if (newMessageList.size() > 0) {
				nextHandle = getMinMessageId(newMessageList);
			}
			
		} while (newMessageList.size() > 0);
 		
 		//assertEquals(messageList.size(), peekMessageList.size());
	}
    @Test 
    public void testPeekQueueRandom() throws Exception {
        
        List<CQSMessage> messageList = new ArrayList<CQSMessage>();
        
        for (int i=0; i<100; i++) {
            CQSMessage message = new CQSMessage("This is a test message id=" + i, attributes);
            messageList.add(message);
            persistence.sendMessage(queue, 0, message);
        }
        
        assertEquals(messageList.size(), getQueueMessageCount(queue.getRelativeUrl()));
        
        List<CQSMessage> messages = persistence.peekQueueRandom(queue.getRelativeUrl(), 0, 5);
        if (messages.size() != 5) {
            fail("Its very unlikely that we did not get 5 and there was no error. size=" + messages.size());
        }
    }
    
	private String getMinMessageId(List<CQSMessage> messageList) {
		
		String minMessageId = null;
		
		if (messageList == null || messageList.size() == 0) {
			return minMessageId;
		}
		
		for (CQSMessage message: messageList) {
			
			if (minMessageId == null) {
				minMessageId = message.getMessageId();
			} else if (minMessageId.compareTo(message.getMessageId()) > 0) {
				minMessageId = message.getMessageId();
			}
		}
		
		return minMessageId;
	}
	
	private long getQueueMessageCount(String queueUrl) throws Exception {
		
		CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
		int numberOfPartitions = queue.getNumberOfPartitions();
		int numberOfShards = queue.getNumberOfShards();
		String queueHash = com.comcast.cqs.util.Util.hashQueueUrl(queueUrl);
		long messageCount = 0;
		
		for (int k=0; k<numberOfShards; k++) {
			for (int i=0; i<numberOfPartitions; i++) {
				String queueKey = queueHash + "_" + k + "_" + i;
				long partitionCount = DurablePersistenceFactory.getInstance().getCount(CMBProperties.getInstance().getCQSKeyspace(), "CQSPartitionedQueueMessages", queueKey, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.COMPOSITE_SERIALIZER);
				messageCount += partitionCount;
				logger.debug("# of messages in " + queueKey + " =" + partitionCount);
			}
		}
		
		logger.debug("There are " + messageCount + " messages in queue " + queueUrl);
		return messageCount;
	}
	
    @After    
    public void tearDown() throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
    	
        logger.debug("teardown");
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
		persistence.clearQueue(queue.getRelativeUrl(), 0);
		queuePersistence.deleteQueue(queue.getRelativeUrl());    
	}
}
