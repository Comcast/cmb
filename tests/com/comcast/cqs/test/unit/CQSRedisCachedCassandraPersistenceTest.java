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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.controller.CQSMonitor;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.persistence.ICQSMessagePersistenceIdSequence;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence.QCacheState;
import com.comcast.cqs.util.CQSConstants;

public class CQSRedisCachedCassandraPersistenceTest {
    private static Logger log = Logger.getLogger(CQSRedisCachedCassandraPersistenceTest.class);

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();
        Thread.sleep(1000); //give previously running cache fillers to complete before we clear cache state
        redisP.testInterface.resetTestQueue();
        if (redisP.testInterface.getCacheState("testQueue") != null) {
            log.error("expected no cache state");
            fail("expected no cache state");
        }
    }
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }


    @Test
    public void testInitialize() {
        RedisCachedCassandraPersistence.getInstance();
    }
    
    @Test
    public void testGetState() throws Exception {
        RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();        

        if (redisP.testInterface.getCacheState("testQueue") != null) {
            fail("expected no cache state");
        }
        
        redisP.testInterface.setCacheState("testQueue", QCacheState.Filling, null, false);
        if (redisP.testInterface.getCacheState("testQueue") != QCacheState.Filling) {
            fail("Expected to get filling.");
        }
    }
    
    class TestStateSetter implements Runnable {
        CountDownLatch latch;
        boolean waitOrCount;
        public TestStateSetter(CountDownLatch latch, boolean waitOrCount) {
            this.latch = latch;
            this.waitOrCount = waitOrCount;
        }
        @Override
        public void run() {
            RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();
            try {
                QCacheState state;
                if (waitOrCount) {
                    latch.await();
                    state = QCacheState.Filling;
                } else {
                    latch.countDown();
                    state = QCacheState.OK;
                }
                System.out.println("setting the state");
                redisP.testInterface.setCacheState("testQueue", state, null, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        }        
    }
    @Test
    public void testSetStateRace() throws Exception {
        final CountDownLatch countdown = new CountDownLatch(1);
        ExecutorService e = Executors.newFixedThreadPool(11);
        for (int i = 0; i <10; i++) {
            e.submit(new TestStateSetter(countdown, true));
        }
        e.submit(new TestStateSetter(countdown, false));
        Thread.sleep(5000);
        e.shutdown();
        e.awaitTermination(5, TimeUnit.SECONDS);
        
    }
    
    @Test
    public void testMemId() throws Exception {
        RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();
        long currTs = System.currentTimeMillis();
        String memId = redisP.testInterface.getMemQueueMessage("45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:-8763141905575923938");
        System.out.println("memId=" + memId);
        long ts = redisP.testInterface.getMemQueueMessageCreatedTS(memId);
        if (ts != 1394146871586L) {
            fail("ts != 1394146871586L");
        }
        if (redisP.testInterface.getMemQueueMessageInitialDelay(memId) != 0) {
            fail("expected 0 for initialdelay");
        }
        if (!redisP.testInterface.getMemQueueMessageMessageId("45c1596598f85ce59f060dc2b8ec4ebb",memId).equals("45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:-8763141905575923938")) {
            fail("expected test-message-id");
        }
        String origId = "3cd3c502a3006162d11227c932198893_7:2800871485265150206:-8449878679487512567";
        memId = redisP.testInterface.getMemQueueMessage(origId);
        log.info("memId=" + memId);
        String messageId = redisP.testInterface.getMemQueueMessageMessageId("3cd3c502a3006162d11227c932198893",memId);
        if (!messageId.equals(origId)) {
            fail("orig!=recreated. recreated = " + messageId);
        }
    }
    
    @Test
    //test if the old format of Redis message key is still working in the new code
    public void testMemIdBackwardCompatible() throws Exception {
        RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();
//        String memId = redisP.testInterface.getMemQueueMessage("45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:-8763141905575923938");
        //this is the old format of Redis message key
        String memId = "1394146871586:0:45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:-8763141905575923938";
        System.out.println("memId=" + memId);
        long ts = redisP.testInterface.getMemQueueMessageCreatedTS(memId);
        if (ts != 1394146871586L) {
            fail("ts != 1394146871586L");
        }
        if (redisP.testInterface.getMemQueueMessageInitialDelay(memId) != 0) {
            fail("expected 0 for initialdelay");
        }
        if (!redisP.testInterface.getMemQueueMessageMessageId("45c1596598f85ce59f060dc2b8ec4ebb",memId).equals("45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:-8763141905575923938")) {
            fail("expected test-message-id");
        }
        String origId = "3cd3c502a3006162d11227c932198893_7:2800871485265150206:-8449878679487512567";
        memId = redisP.testInterface.getMemQueueMessage(origId);
        log.info("memId=" + memId);
        String messageId = redisP.testInterface.getMemQueueMessageMessageId("3cd3c502a3006162d11227c932198893",memId);
        if (!messageId.equals(origId)) {
            fail("orig!=recreated. recreated = " + messageId);
        }
    }
    
    //used to feed test-data to cache-filler
    class TestDataPersistence implements ICQSMessagePersistence, ICQSMessagePersistenceIdSequence{
        List<CQSMessage> messages;
        int headMarker = 0;
        TreeSet<String> getMessgeIds = new TreeSet<String>();
        
        public TestDataPersistence(int numMessages) throws NoSuchAlgorithmException, UnsupportedEncodingException {
            messages = new LinkedList<CQSMessage>();
            long timeHash = AbstractDurablePersistence.newTime(System.currentTimeMillis(), false);
            for (int i = 0; i < numMessages; i++) {
                CQSMessage msg = new CQSMessage(""+i, new HashMap<String, String>());
                msg.setMessageId("45c1596598f85ce59f060dc2b8ec4ebb_0_72:"+timeHash+":"+i);
                msg.setReceiptHandle(""+i);
                msg.setSuppliedMessageId(""+i);
                messages.add(msg);
            }  
            
        }
        @Override
        public String sendMessage(CQSQueue queue, int shard, CQSMessage message)
                throws PersistenceException, IOException, InterruptedException,
                NoSuchAlgorithmException {
            
            messages.add(message);
            return message.getMessageId();
        }

        @Override
        public Map<String, String> sendMessageBatch(CQSQueue queue,
        		int shard,
                List<CQSMessage> messages) throws PersistenceException,
                IOException, InterruptedException, NoSuchAlgorithmException {
            
            Map<String, String> ret = new HashMap<String, String>();
            this.messages.addAll(messages);
            for (CQSMessage msg : messages) {
                ret.put(msg.getMessageId(), msg.getMessageId());
            }
            return ret;
        }

        @Override
        public void deleteMessage(String queueUrl, String receiptHandle) throws PersistenceException {
            messages.remove(getTestIndexByMessageId(receiptHandle));
        }

        @Override
        public List<CQSMessage> receiveMessage(CQSQueue queue,
                Map<String, String> receiveAttributes)
                throws PersistenceException, IOException,
                NoSuchAlgorithmException, InterruptedException {
            return null;
        }

        @Override
        public boolean changeMessageVisibility(CQSQueue queue,
                String receiptHandle, int visibilityTO)
                throws PersistenceException, IOException,
                NoSuchAlgorithmException, InterruptedException {
            return false;
        }

        @Override
        public List<CQSMessage> peekQueue(String queueUrl, int shard, String previousReceiptHandle, String nextReceiptHandle, int length) throws PersistenceException, IOException, NoSuchAlgorithmException {
            List<CQSMessage> ret = new LinkedList<CQSMessage>();
            int prevHandle = (previousReceiptHandle == null ? -1 : getTestIndexByMessageId(previousReceiptHandle));
            log.info("prevhandle=" + previousReceiptHandle);
            for (int i = 0; i < length && i < (messages.size() - prevHandle -1); i++) {
                ret.add(messages.get(prevHandle + 1 + i));
            }
            return ret;
        }
        
        //MessageId example: "45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:999"
        private int getTestIndexByMessageId(String messageId){
        	String [] IdParts = messageId.split(":");
        	String IndexString = IdParts[IdParts.length-1];
        	return Integer.parseInt(IndexString);
        }

        @Override
        public void clearQueue(String queueUrl, int shard) throws PersistenceException,
                NoSuchAlgorithmException, UnsupportedEncodingException {
            messages.clear();
        }

        @Override
        public Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
            Map<String, CQSMessage> ret = new HashMap<String, CQSMessage>();
            for (String id : ids) {
            	// This is only for unit test
            	//example of id: 45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:20
            	String indexString = id.substring(id.lastIndexOf(":")+1);

                ret.put(id, messages.get(Integer.parseInt(indexString)));
            }
            getMessgeIds.addAll(ids);
            return ret;
        }
        @Override
        public List<String> getIdsFromHead(String queueUrl, int shard, int num) {
            LinkedList<String> ids = new LinkedList<String>();
            for (int i = headMarker; i < messages.size(); i++) {
                ids.add(RedisCachedCassandraPersistence.getInstance().testInterface.getMemQueueMessage(""+i));
            }
            return ids;
        }
        @Override
        public long getQueueMessageCount(String queueUrl) {
            return messages.size();
        }
        @Override
        public List<CQSMessage> peekQueueRandom(String queueUrl, int shard, int length)
                throws PersistenceException, IOException,
                NoSuchAlgorithmException {
            if (messages.size() == 0) return Collections.emptyList();
            return messages.subList(0, length);
        }
        
    }
    @Test
    public void testTestDataPersistence() throws Exception {
        TestDataPersistence p = new TestDataPersistence(2000);
        List<CQSMessage> msg = p.peekQueue("testQueue", 0, null, null, 1000);
        if (msg.size() != 1000) {
            fail("Expected 1k got:"+msg.size());
        }
        List<CQSMessage> newMsg = p.peekQueue("testQueue", 0, msg.get(999).getMessageId(), null, 1000);
        if (newMsg.size() != 1000) {
            fail("Expected 1k got:"+newMsg.size());
        }
        if (newMsg.get(0).getMessageId().equals(msg.get(999).getMessageId())) {
            fail("last element returned twice");
        }
        
        List<CQSMessage> newMsg2 = p.peekQueue("testQueue", 0, newMsg.get(999).getMessageId(), null, 1000);
        if (newMsg2.size() != 0) {
            fail("Expected 0");
        }
    }
    
    @Test
    public void testCacheFiller() throws Exception {
        RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();
        
        redisP.testInterface.setCassandraPersistence(new TestDataPersistence(2000));
        boolean checkCache = redisP.testInterface.checkCacheConsistency("testQueue");
        if (checkCache == true) {
            fail("first time around cache should not have existed and method should have returned false");
        }
        
        //check that the state is filling
        QCacheState st = redisP.testInterface.getCacheState("testQueue");
        if (st != QCacheState.Filling) {
            fail("Expected state of qcache is filling. Instead got:" + st.name());
        }
        
        //wait a second and check that its now ok
        Thread.sleep(2000);
        st = redisP.testInterface.getCacheState("testQueue");
        if (st != QCacheState.OK) {
            fail("Expected state of qcache is ok. Instead got:" + st.name());
        }
        checkCache = redisP.testInterface.checkCacheConsistency("testQueue");
        if (checkCache == false) {
            fail("Expected cache available");
        }
    }
        
    
    public RedisCachedCassandraPersistence testSendMessage(boolean batch) throws Exception {
        RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();

        TestDataPersistence testP = new TestDataPersistence(2000);
        
        redisP.testInterface.setCassandraPersistence(testP);
        boolean checkCache = redisP.testInterface.checkCacheConsistency("testQueue");
        if (checkCache == true) {
            fail("first time around cache should not have existed and method should have returned false");
        }
        
        //check that the state is filling
        QCacheState st = redisP.testInterface.getCacheState("testQueue");
        if (st != QCacheState.Filling) {
            fail("Expected state of qcache is filling. Instead got:" + st.name());
        }
        
        //wait a second and check that its now ok
        Thread.sleep(2000);
        checkCache = redisP.testInterface.checkCacheConsistency("testQueue");
        if (checkCache == false) {
            fail("Expected cache available");
        }

        //now sendMessage(Batch)
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        CQSMessage msg = new CQSMessage("test", new HashMap<String, String>());
//        msg.setMessageId("2000");
        long timeHash = AbstractDurablePersistence.newTime(System.currentTimeMillis(), false);
        msg.setMessageId("45c1596598f85ce59f060dc2b8ec4ebb_0_72:"+timeHash+":2000");
        msg.setSuppliedMessageId("2000");
        String newMessageId;
        if (!batch) {
            newMessageId = redisP.sendMessage(queue, 0, msg);
        } else {
            Map<String, String> clientToMessageId = redisP.sendMessageBatch(queue, 0, Arrays.asList(msg));
            if (clientToMessageId.size() != 1) {
                fail("Expected one messageId returned");
            }
            if (!clientToMessageId.containsKey("2000")) {
                fail("Expected client's messageId 2000 in response but not found");
            }
            newMessageId = clientToMessageId.get("2000");
        }
        
        if (testP.messages.size() != 2001) {
            fail("Expected to add one more message. count=" + testP.messages.size() );
        }
        if (redisP.getQueueMessageCount("testQueue", false) != 2001) {
            fail("Expected 2001. Got:" + redisP.getQueueMessageCount("testQueue", false));
        }
        
        //get message and ensure its what you added
        Map<String, CQSMessage> ret = redisP.getMessages("testQueue", Arrays.asList(newMessageId));
        if (ret.size() !=1) {
            fail("did not get the newly created mesage");
        }
        if (!ret.containsKey(newMessageId)) {
            fail("returned map does not have newMessageId");
        }
        return redisP;
    }
    
    @Test
    public void testSendMessage() throws Exception {
        testSendMessage(false);
        Long oldestMsgTS = CQSMonitor.getInstance().getOldestMessageCreatedTSMS("testQueue"); 
        if (oldestMsgTS == null || oldestMsgTS > System.currentTimeMillis()) {
            fail("Expected oldestMsgs. Instead failed");
        }
        log.info("oldestMsgTS=" + oldestMsgTS);
    }
    
    @Test
    public void testSendMessageBatch() throws Exception {
        testSendMessage(true);
    }    
    
    @Test
    public void testReceiveMessage() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "10");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "1"); //hide messages for 1 second
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 10) {
            fail("Expected 10 messages. got=" + messages.size());            
        }
        
        //ensure the cache hit ratio was 100%
        if (CQSMonitor.getInstance().getReceiveMessageCacheHitPercent("testQueue") != 100) {
            fail("Expected cache hit to be 100% instead got:" + CQSMonitor.getInstance().getReceiveMessageCacheHitPercent("testQueue"));
        }
        
        
        //capture the message-id of the first message and verify we don't get the same one again in the next batch
        String firstMessageId = messages.get(0).getMessageId();
        
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 10) {
            fail("Expected 10 messages. got=" + messages.size());            
        }
        
        for (CQSMessage msg : messages) {
            if (msg.getMessageId().equals(firstMessageId)) {
                fail("firstMessageId returned twice. Duplciates!: " + firstMessageId);
            }
        }
    }

    @Test
    public void testReceiveMessageBackwardCompatibile() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "10");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "1"); //hide messages for 1 second
        //before receive, change Redis key to old format.
        String queueUrl = "testQueue";
        boolean brokenJedis = false;
        ShardedJedis jedis = RedisCachedCassandraPersistence.getResource();
        try {
            //jedis is lame and does not have a constant for "-inf" which Redis supports. So we have to
            //pick an arbitrary old min value.
        	String redisKey = queueUrl + "-0-Q";
            Long redisListSize = jedis.llen(redisKey);
            String redisMemId = null;
            String redisOldMemId = null;
            for (int i = 0; i < redisListSize; i++) {
            	redisMemId = jedis.lpop(redisKey);
            	redisOldMemId = "1394146871592:0:"+redisP.testInterface.getMemQueueMessageMessageId("45c1596598f85ce59f060dc2b8ec4ebb",redisMemId);
            	jedis.rpush(redisKey, redisOldMemId);
            }
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
        	RedisCachedCassandraPersistence.returnResource(jedis, brokenJedis);
        }  
        //end of change Redis key
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 10) {
            fail("Expected 10 messages. got=" + messages.size());            
        }
        
        //ensure the cache hit ratio was 100%
        if (CQSMonitor.getInstance().getReceiveMessageCacheHitPercent("testQueue") != 100) {
            fail("Expected cache hit to be 100% instead got:" + CQSMonitor.getInstance().getReceiveMessageCacheHitPercent("testQueue"));
        }
        
        
        //capture the message-id of the first message and verify we don't get the same one again in the next batch
        String firstMessageId = messages.get(0).getMessageId();
        
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 10) {
            fail("Expected 10 messages. got=" + messages.size());            
        }
        
        for (CQSMessage msg : messages) {
            if (msg.getMessageId().equals(firstMessageId)) {
                fail("firstMessageId returned twice. Duplciates!: " + firstMessageId);
            }
        }
    }
    @Test
    public void testReceiveMessageNoDelay() throws Exception {
        //test that with 0 message visibility. The same message is returned everytime
        //also ensure the message-attributes are still done as with non-zero visibilityTO

        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "1");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "0"); //hide messages for 1 second
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 1) {
            fail("Expected 10 messages. got=" + messages.size());            
        }
        String firstMessageId = messages.get(0).getMessageId();
        
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 1) {
            fail("Expected 10 messages. got=" + messages.size());            
        }
        if (!messages.get(0).getMessageId().equals(firstMessageId)) {
            fail("Expected the same message to be returned subsequently if VTO is 0. Instead got:" + messages.get(0).getMessageId());
        }
        if (Integer.parseInt(messages.get(0).getAttributes().get(CQSConstants.APPROXIMATE_RECEIVE_COUNT)) != 2) {
            fail("Expected appx recv count 2. Got something else: "+ Integer.parseInt(messages.get(0).getAttributes().get(CQSConstants.APPROXIMATE_RECEIVE_COUNT)));
        }
        
    }
    
    @Test
    public void testSendReceiveTwiceScenario() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CMBProperties.getInstance().setRedisRevisibleFrequencySec(1); //1 sec
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "2001");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "1"); //hide messages for 1 second
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes); //get all messages
        if (messages.size() != 2001) {
            fail("Expected 2001 messages. got=" + messages.size());            
        }
        
        //now sleep 1 seconds
        Thread.sleep(1001);
        messages = redisP.receiveMessage(queue, receiveAttributes); //should kick off RevisibleProcessor
        Thread.sleep(4000);//let RevisibleProcessor finish
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 2001) {
            fail("Expected 2001 messages. got=" + messages.size());            
        }        
    }
    
    @Test
    public void testDeleteMessage() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "10");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "1"); //hide messages for 1 second
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 10) {
            fail("Expected 10 messages. got=" + messages.size());            
        }

        redisP.deleteMessage("testQueue", messages.get(0).getMessageId());
        
        //ensure that message0 was deleted from underlying storage
        TestDataPersistence pers = (TestDataPersistence) redisP.testInterface.getCassandraPersistence();
        if (pers.messages.size() != 2000) {
            fail("Expected after delete to have 2k messages. Got:" + pers.messages.size());
        }
        if (pers.messages.get(0).getMessageId().equals("0")) {
            fail("Expected to delete message with messageId 0. It still exists in underlying layer");
        }
        
    }
    
    //@Test
    public void testChangeVTO() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        
        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "2001");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "10"); //hide messages for 1 second
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 2001) {
            fail("Expected 2001 messages. got=" + messages.size());            
        }

        if (!redisP.changeMessageVisibility(queue, messages.get(0).getReceiptHandle(), 1)) {
            fail("Could not update visibilityTO");
        }
        String firstMsgRecptH = messages.get(0).getReceiptHandle();
        
        //wait for 1 second, call receive-message and get the 1 messages  
        Thread.sleep(1001);
        if (redisP.getQueueMessageCount("testQueue", true) != 1) {
            fail("Expected 1 message to become re-visible. Got something else");
        }
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 1 || !messages.get(0).getReceiptHandle().equals(firstMsgRecptH)) {
            fail("Expected 1 messages. got=" + messages.size());            
        }
        
        //now change message-visibility of received-message to 0 and see it become instantly visible
        if (!redisP.changeMessageVisibility(queue, messages.get(0).getReceiptHandle(), 0)) {
            fail("Could not update visibilityTO");
        }
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 1 || !messages.get(0).getReceiptHandle().equals(firstMsgRecptH)) {
            fail("Expected 1 messages. got=" + messages.size() + " with receipt handle=" + messages.get(0).getReceiptHandle());            
        }
    }
    
    //@Test
    public void testChangeVTOTwice() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        
        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "2001");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "10"); //hide messages for 1 second
        
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes);
        
        if (messages.size() != 2001) {
            fail("Expected 2001 messages. got=" + messages.size());            
        }

        if (!redisP.changeMessageVisibility(queue, messages.get(0).getReceiptHandle(), 1)) {
            fail("Could not update visibilityTO");
        }
        //now change message-visibility of received-message again
        if (!redisP.changeMessageVisibility(queue, messages.get(0).getReceiptHandle(), 1)) {
            fail("Could not update visibilityTO");
        }
        String firstMsgRecptH = messages.get(0).getReceiptHandle();
        
        //wait for 1 second, call receive-message and get the 1 messages  
        Thread.sleep(1001);
        if (redisP.getQueueMessageCount("testQueue", true) != 1) {
            fail("Expected 1 message to become re-visible. Got something else");
        }
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 1 || !messages.get(0).getReceiptHandle().equals(firstMsgRecptH)) {
            fail("Expected 1 messages. got=" + messages.size());            
        }        
    }
    
    //@Test
    public void testMsgAttrs() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        
        
        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "2001");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "10"); //hide messages for 1 second
        
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 2001) {
            fail("Expected 2001 messages. got=" + messages.size());            
        }
        CQSMessage firstMsg = messages.get(0);
        String firstRecvTS = null;
        if (firstMsg.getAttributes().size() < 2) {
            fail("Expected atleast the ApproximateReceiveCount & ApproximateFirstReceiveTimestamp");
        }
        if (firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP) == null ||
            Long.parseLong(firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP)) > System.currentTimeMillis()) {
            fail("Expected first time receive ts to be now or in the near past. its in the future");
        }
        firstRecvTS = firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP);
        if (firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_RECEIVE_COUNT) == null ||
                Long.parseLong(firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_RECEIVE_COUNT)) != 1) {
            fail("Expected first time APPROXIMATE_RECEIVE_COUNT to be 1. its:" + firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_RECEIVE_COUNT));
        }
        
        //now migrate message to re-visible set by changing message visibility.
        if (!redisP.changeMessageVisibility(queue, firstMsg.getReceiptHandle(), 1)) {
            fail("Could not update visibilityTO");
        }
        
        //wait for 1 second, call receive-message and get all the 1 messages  
        Thread.sleep(1001);
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 1 || !messages.get(0).getReceiptHandle().equals(firstMsg.getReceiptHandle())) {
            fail("Expected 1 messages. got=" + messages.size());            
        }
        firstMsg = messages.get(0);
        if (firstMsg.getAttributes().size() < 2) {
            fail("Expected atleast the ApproximateReceiveCount & ApproximateFirstReceiveTimestamp");
        }
        if (firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP) == null ||
            !firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP).equals(firstRecvTS)) {
            fail("Expected first time receive ts was expected:" + firstRecvTS + " instead got:" + firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP));
        }
        if (firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_RECEIVE_COUNT) == null ||
                Long.parseLong(firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_RECEIVE_COUNT)) != 2) {
            fail("Expected first time APPROXIMATE_RECEIVE_COUNT to be 2. its:" + firstMsg.getAttributes().get(CQSConstants.APPROXIMATE_RECEIVE_COUNT));
        }

        redisP.deleteMessage("testQueue", firstMsg.getReceiptHandle());

    }
    
    
    @Test
    public void testClearQ() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        

        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "10");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "1"); //hide messages for 1 second
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 10) {
            fail("Expected 10 messages. got=" + messages.size());            
        }
        
        log.info("----------Clearing");
        log.info("QCachestae=" + redisP.testInterface.getCacheState("testQueue"));
        redisP.clearQueue("testQueue", 0);
        
        messages = redisP.receiveMessage(queue, receiveAttributes);
        
        if (messages.size() !=0) {
            fail("Expected to find no queue messages");
        }
        TestDataPersistence pers = (TestDataPersistence) redisP.testInterface.getCassandraPersistence(); 
        if (pers.messages.size() != 0) {
            fail ("did not clear underlying persistent messages");
        }
    }    
    
    @Test
    public void testRevisible() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        

        HashMap<String, String> receiveAttributes = new HashMap<String, String>();
        receiveAttributes.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "2000");
        receiveAttributes.put(CQSConstants.VISIBILITY_TIMEOUT, "2"); //hide messages for 2 second
        List<CQSMessage> messages = redisP.receiveMessage(queue, receiveAttributes); //should kick off revisibilityprocessor
        if (messages.size() != 2000) {
            fail("Expected 2000 messages. got=" + messages.size());            
        }

        Thread.sleep(2001); //wait 2 second and ensure your old messages are all visible
        redisP.testInterface.scheduleRevisibilityProcessor("testQueue");
        Thread.sleep(4000);//give time for revisibilityprocessor to run
        messages = redisP.receiveMessage(queue, receiveAttributes);
        if (messages.size() != 2000) {
            fail("Expected 2000 messages. got=" + messages.size());            
        }
        
    }
    
    @Test
    public void testPeek() throws Exception {
        RedisCachedCassandraPersistence redisP = testSendMessage(false); //creates a queue with 2001 messages in it
        CQSQueue queue = new CQSQueue("testQueue", "testOwner");
        queue.setRelativeUrl("testQueue");        

        List<CQSMessage> ret = redisP.peekQueue("testQueue", 0, null, null, 10);
        if (ret.size() != 10) {
            fail("Expected 10. Got:" + ret.size());
        }
        log.info("10th el=" + ret.get(9).getMessageId());
        List<CQSMessage> ret2 = redisP.peekQueue("testQueue", 0, ret.get(9).getMessageId(), null, 10);
        if (ret2.size() != 10) {
            fail("Expected 10. Got:" + ret2.size());
        }
        for (CQSMessage msg : ret2) {
            if (msg.equals(ret.get(9).getMessageId())) {
                fail("object returned twice:" + ret.get(9).getMessageId());
            }
        }
        //get all the others
        List<CQSMessage> ret3 = redisP.peekQueue("testQueue", 0, ret2.get(9).getMessageId(), null, 2001); //length is way past the end
        if (ret3.size() != 2001 - 20) {
            fail("Expected:" + (2001-20) + " got:" + ret3.size());
        }
    }
    
    @Test
    public void testMessagePersistence() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
        
    	HashMap<String, String> att = new HashMap<String, String>();
        //att.put("testKey", "val");
        String body = "10268LLLDKDUY6UQAP5VKRPOQMEVT8YOM2DUG08XD8HN6ALRUGISBAQYXZTS5W7IIX15V1QDJL8W3UTQM6BH72UA16D53TMNWV0IGDVVIC5ZB2V7JMCE8MJPMODWFJPSG9GT9U9LVZ163419S3K8IB97592G4JMWR7U2PFYRDFCCVQ2H6J8QT46K2L8XWOHD99PQ8QLZ2K2H00I2TGS4564RWKYFEFOGXRZF2ZN22AYDSXQ2BR8YIG6CSI2QYN9C8CY37MB3U1UK9L123C84KIC1LBMQC5ZJPYJHFG5SY4CR5TBD3RB7VXB4S7NSG0IA1ZYDQPDETT0OPSH5VCAXV82AO99JQ4FDV1ERW0RT3MGDQZHNONXWKNDAMN3F6U1BCILZIXG4TDJ9N5A7EWOVMJP5DZBG8HOLFFD5QC3CVK0OZ6HKWJFETNDNE236WMC2HLZU9N5KCWSZ0BRJBB5XMW1RSHGHY0EBIEIH3RCY5UA0CZDFAQ3ORLDUGYXUQFDKK74E5YAVTKCDTUNXA6LKHVUWEY8JPWUH6ZR1DJZY0NCK61NPU1RRKDIK1DFIPE3X24SUSJXD4ERZISBAJXKNDB4FDYZEN924O9JXR9YH2BXXFEIHWZHN2QDPFBONBMI4UP4ZSH5XXX6YOJCM2ZERTHN5Z39EQHCJTIU30OZF3Z5J9FY1C5BCKX1CCHBLUPA6JMI18HU0ORSQ6ZMTJP4GT1FIFZB2OAEK8R89AXS7DFPLUBRAM6OLQ5EENNVV9G0UBROB8ZOM59TFZQQAEQSL9AD51A7IR0UW171VKS3YPBBWSKTNROKISRKQ49DQ06Y1TRGEWJ8YHM0SL9BACJWFXL3FLUV8E5VYWZM2NSRNO2TVE2O6TSJL2R3YZQIABDINXZ0X48KLQRVEZFDW3OYEL9Z3Z0ALLR1ZPE9E9MWFTHPROVCKI1LST8FVSEQAIUAZZI755IM2MJUW66NHX8K1U3SQV3OS4AG3CYOMKDII3J1ONHAS9HVN7V50RZ6NPKO2C1YQ1V4Q40FNK30THWPX6VY451PBSB1TUGDJEYHJSD4PT0CZEDZFAFNJX6L4S8JCEC3RAO0L4BR3YTXZFZPQZSX8YBLVNIQ29J6PPP9RNFAFVDG49O3S9XNGOI6JSY7GR2NDBKDEEBBJP1B8RFS7KB5VHGCVNQ9YNDEJ8DFUOGFAUSXTY3M181J56P71W9WJHKZSWPZMAYMK7RMR5TZOX0AT5TN0MWHC3N5UZJQR0PR55TFJDARR33DSQK35RQA82PQ7XS2L1DFDEAJ1M0GB6YD9OQGUEW51GO8EY3TJP17LG3Y1V4OUQU858YYLV0QOXIBBIW7YPN48D7HU8IPEFAOW5EHC21N0WD5M9W6HTQHDVYUDI29P1MTH4KHDRWEZUZO77QK64P4Y4TG1T34H47DD06LNE4VYF9D2S7KMY5BZY408KFGPCC6KE6YMZICSHJWS4UZK4B06J9R30LZDXEV9MT8VEDVBF5LOK3V2YHJODRU14OUYOKCX27BC0DZKFRREUF2XRCMYNN1SWMDYZFYOP8AFRB8Q1LKYVIR98CJJS3MH65OJ3Y4PX0QJW2RFY328KIN1C9NUIJN77QY0HDUD2Z7GF37KYXQCF95Q4QIPKHFNE98X3T8GXXTYGTKKGYZNLV1VFLC8UN65HE6AB5RHQLOLW8ES44V47BJDQXSLFDP0WHSRZUUTOPZY15IGGL87VTSS1LO61QM28EQD8794KJZ3SNRFTUFKFMN3U2A9RJO08PB2FK95T9W62ZLJDWZFHPSB81MSHQ4PX10GREP94T6UDKHH0XQS76KKTCU2FUS77WHMFWDSKWWVJAUKDAU84EPE5G0NTG3QP9Y4LJDXAQJPIEKQ5BGD30AWPHXGNY9C336UVEIPPBB583Z3R0OTUT829JDF1OR36VEOVH7FQ7GTZQOPK4_9";
        CQSMessage msg = new CQSMessage(body, att);
        String messageId = msg.getMessageId();
        
        //msg.setSuppliedMessageId("boo");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(baos);
        
        os.writeObject(msg);
        os.close();
        log.info("size of cqsmessage=" + baos.size());
        
        ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        CQSMessage msgRec = (CQSMessage) is.readObject();
        msgRec.setMessageId(messageId);

        if (!msgRec.equals(msg)) {
            fail("orig != rec. orig=" + msg + " rec=" + msgRec);
        }
    }
    
    @Test
    public void testByteSerialization() throws IOException {
        String a = "10268LLLDKDUY6UQAP5VKRPOQMEVT8YOM2DUG08XD8HN6ALRUGISBAQYXZTS5W7IIX15V1QDJL8W3UTQM6BH72UA16D53TMNWV0IGDVVIC5ZB2V7JMCE8MJPMODWFJPSG9GT9U9LVZ163419S3K8IB97592G4JMWR7U2PFYRDFCCVQ2H6J8QT46K2L8XWOHD99PQ8QLZ2K2H00I2TGS4564RWKYFEFOGXRZF2ZN22AYDSXQ2BR8YIG6CSI2QYN9C8CY37MB3U1UK9L123C84KIC1LBMQC5ZJPYJHFG5SY4CR5TBD3RB7VXB4S7NSG0IA1ZYDQPDETT0OPSH5VCAXV82AO99JQ4FDV1ERW0RT3MGDQZHNONXWKNDAMN3F6U1BCILZIXG4TDJ9N5A7EWOVMJP5DZBG8HOLFFD5QC3CVK0OZ6HKWJFETNDNE236WMC2HLZU9N5KCWSZ0BRJBB5XMW1RSHGHY0EBIEIH3RCY5UA0CZDFAQ3ORLDUGYXUQFDKK74E5YAVTKCDTUNXA6LKHVUWEY8JPWUH6ZR1DJZY0NCK61NPU1RRKDIK1DFIPE3X24SUSJXD4ERZISBAJXKNDB4FDYZEN924O9JXR9YH2BXXFEIHWZHN2QDPFBONBMI4UP4ZSH5XXX6YOJCM2ZERTHN5Z39EQHCJTIU30OZF3Z5J9FY1C5BCKX1CCHBLUPA6JMI18HU0ORSQ6ZMTJP4GT1FIFZB2OAEK8R89AXS7DFPLUBRAM6OLQ5EENNVV9G0UBROB8ZOM59TFZQQAEQSL9AD51A7IR0UW171VKS3YPBBWSKTNROKISRKQ49DQ06Y1TRGEWJ8YHM0SL9BACJWFXL3FLUV8E5VYWZM2NSRNO2TVE2O6TSJL2R3YZQIABDINXZ0X48KLQRVEZFDW3OYEL9Z3Z0ALLR1ZPE9E9MWFTHPROVCKI1LST8FVSEQAIUAZZI755IM2MJUW66NHX8K1U3SQV3OS4AG3CYOMKDII3J1ONHAS9HVN7V50RZ6NPKO2C1YQ1V4Q40FNK30THWPX6VY451PBSB1TUGDJEYHJSD4PT0CZEDZFAFNJX6L4S8JCEC3RAO0L4BR3YTXZFZPQZSX8YBLVNIQ29J6PPP9RNFAFVDG49O3S9XNGOI6JSY7GR2NDBKDEEBBJP1B8RFS7KB5VHGCVNQ9YNDEJ8DFUOGFAUSXTY3M181J56P71W9WJHKZSWPZMAYMK7RMR5TZOX0AT5TN0MWHC3N5UZJQR0PR55TFJDARR33DSQK35RQA82PQ7XS2L1DFDEAJ1M0GB6YD9OQGUEW51GO8EY3TJP17LG3Y1V4OUQU858YYLV0QOXIBBIW7YPN48D7HU8IPEFAOW5EHC21N0WD5M9W6HTQHDVYUDI29P1MTH4KHDRWEZUZO77QK64P4Y4TG1T34H47DD06LNE4VYF9D2S7KMY5BZY408KFGPCC6KE6YMZICSHJWS4UZK4B06J9R30LZDXEV9MT8VEDVBF5LOK3V2YHJODRU14OUYOKCX27BC0DZKFRREUF2XRCMYNN1SWMDYZFYOP8AFRB8Q1LKYVIR98CJJS3MH65OJ3Y4PX0QJW2RFY328KIN1C9NUIJN77QY0HDUD2Z7GF37KYXQCF95Q4QIPKHFNE98X3T8GXXTYGTKKGYZNLV1VFLC8UN65HE6AB5RHQLOLW8ES44V47BJDQXSLFDP0WHSRZUUTOPZY15IGGL87VTSS1LO61QM28EQD8794KJZ3SNRFTUFKFMN3U2A9RJO08PB2FK95T9W62ZLJDWZFHPSB81MSHQ4PX10GREP94T6UDKHH0XQS76KKTCU2FUS77WHMFWDSKWWVJAUKDAU84EPE5G0NTG3QP9Y4LJDXAQJPIEKQ5BGD30AWPHXGNY9C336UVEIPPBB583Z3R0OTUT829JDF1OR36VEOVH7FQ7GTZQOPK4_9";
        byte []arr = a.getBytes("UTF-8");
        log.info("arrSize=" + arr.length);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(baos);
        
        os.write(arr);
        os.close();
        
        byte[] ser = baos.toByteArray();
        log.info("size=" + ser.length);
    }
    
    @Test
    public void testByteArray() {
        String key = "fool";
        RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();
        ShardedJedis jedis = redisP.testInterface.getResource();
        try {
            jedis.del(key);
            if (jedis.hset(SafeEncoder.encode(key), "test".getBytes(), "test".getBytes()) == 0L) {
                fail("Could not set");
            }
            if (!jedis.exists(key)) {
                fail("key does not exist");
            }
        } finally {
            redisP.testInterface.returnResource(jedis);
        }
    }
    
    @Test
    public void testhmget() throws Exception {
        RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();
        ShardedJedis jedis = redisP.testInterface.getResource();
        try {
            String key = "testhmget";
            jedis.hset(key, "test", "test");
            LinkedList<byte[]> fields = new LinkedList<byte[]>();
            fields.add(SafeEncoder.encode("test"));
            fields.add(SafeEncoder.encode("test2"));
            List<byte[]> vals = jedis.hmget(SafeEncoder.encode(key), fields.toArray(new byte[0][0]));
            System.out.println("vals.size=" + vals.size());
        } finally {
            redisP.testInterface.returnResource(jedis);
        }
    }
    
    @Test
    public void floatMath() {
        int a = 10;
        int b = 13;
        log.info((float)a/(float)b);
        log.info(   (int)(((float)a / (float)b) * 100)    ) ;
    }
}
