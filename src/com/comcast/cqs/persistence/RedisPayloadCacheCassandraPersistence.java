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
package com.comcast.plaxo.cqs.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cmb.common.util.Util;
import com.comcast.plaxo.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.plaxo.cqs.controller.CQSControllerServlet;
import com.comcast.plaxo.cqs.controller.CQSMonitor;
import com.comcast.plaxo.cqs.controller.CQSMonitor.CacheType;
import com.comcast.plaxo.cqs.model.CQSMessage;
import com.comcast.plaxo.cqs.model.CQSQueue;
import com.comcast.plaxo.cqs.persistence.RedisCachedCassandraPersistence.SetFailedException;

/**
 * This class encapsulates the caching of message payload and sits between the 
 * RedisCachedCassandraPersistence and the underlying Cassandra Persistence
 *
 *@author aseem, bwolf
 *
 * Class is thread-safe
 */
public class RedisPayloadCacheCassandraPersistence implements ICQSMessagePersistence {
    private static final Logger logger = Logger.getLogger(RedisPayloadCacheCassandraPersistence.class);
    
    private volatile ICQSMessagePersistenceIdSequence idSeq;
    private final ICQSMessagePersistence persistenceStorage; 
    public final TestInterface testInterface = new TestInterface();
    
    public RedisPayloadCacheCassandraPersistence(ICQSMessagePersistence persistenceStorage) {
        this.persistenceStorage = persistenceStorage;
    }
    
    public void setMessagePersistenceIdSequence(ICQSMessagePersistenceIdSequence idSeq) {
        this.idSeq = idSeq;        
    }
    
    public class TestInterface {
        public void initializeQ(String q) {
            ShardedJedis jedis = null;
            try {
                jedis = RedisCachedCassandraPersistence.getResource();
                jedis.del(q + "-P");
                jedis.del(q + "-P-STATE");
            } finally {
                if (jedis != null) {
                    RedisCachedCassandraPersistence.returnResource(jedis, false);
                }
            }
        }
        public long getCacheSize(String q) {
            ShardedJedis jedis = null;
            try {
                jedis = RedisCachedCassandraPersistence.getResource();
                return jedis.hlen(q + "-P");
            } finally {
                if (jedis != null) {
                    RedisCachedCassandraPersistence.returnResource(jedis, false);
                }
            }            
        }
        public Runnable getCacheFiller(String q) {
            return new PayloadCacheFiller(q);
        }        
        
        public boolean isInCache(String q, String messageId) {
            ShardedJedis jedis = null;
            try {
                jedis = RedisCachedCassandraPersistence.getResource();
                return jedis.hexists(q + "-P", messageId);
            } finally {
                if (jedis != null) {
                    RedisCachedCassandraPersistence.returnResource(jedis, false);
                }
            }            
            
        }
        public boolean isCacheFillingState(String q) {
            return RedisPayloadCacheCassandraPersistence.isCacheFillingState(q);
        }
        public void setCacheFillingState(String q, boolean setOrClear) throws SetFailedException {
            RedisPayloadCacheCassandraPersistence.checkAndSetCacheFillingState(q, setOrClear);
        }
    }
    
    /**
     * Set the state for a payloadcache or throw exception if someone else beat us to it.
     * This is an atomic operation
     * @param queueUrl
     * @param setOrClear if true will set, if false will delete
     * @param state State to set to. if null, then the state field is deleted
     * @throws SetFailedException
     * @return true if sentinel was set. False if not set or if we were clearing
     */
    private static boolean checkAndSetCacheFillingState(String queueUrl, boolean setOrClear) throws SetFailedException {
        long ts1 = System.currentTimeMillis();
        boolean brokenJedis = false;
        ShardedJedis jedis = RedisCachedCassandraPersistence.getResource();
        try {
            if (setOrClear) {
                if (jedis.exists(queueUrl + "-P-STATE")) {
                    return false;
                }
            }
            Jedis j = jedis.getShard(queueUrl + "-P-STATE");
            j.watch(queueUrl + "-P-STATE");
            if (setOrClear) {
                //check within transaction if its still not set
                if (j.exists(queueUrl + "-P-STATE")) {
                    j.unwatch();
                    throw new SetFailedException();
                }
            }
            Transaction tr = j.multi();
            if (!setOrClear) {
                tr.hdel(queueUrl + "-P-STATE", "STATE");
            } else {
                tr.hset(queueUrl + "-P-STATE", "STATE", "Y");
            }
            List<Object> resp = tr.exec();
            if (resp == null) {
                throw new SetFailedException();
            }
            return setOrClear; //return true if we were setting, false if we were clearing
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            RedisCachedCassandraPersistence.returnResource(jedis, brokenJedis);
            long ts2 = System.currentTimeMillis();
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
        }
    }
    /**
     * 
     * @param queueUrl
     * @return The Q-STATE value for a queue or null if none exists
     */
    private static boolean isCacheFillingState(String queueUrl) {
        long ts1 = System.currentTimeMillis();
        boolean brokenJedis = false;
        ShardedJedis jedis = RedisCachedCassandraPersistence.getResource();
        try {
            return jedis.exists(queueUrl + "-P-STATE");
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            RedisCachedCassandraPersistence.returnResource(jedis, brokenJedis);
            long ts2 = System.currentTimeMillis();
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
        }
    }


    
    /**
     * Fill payload cache with n elements from the head of queue
     */
    private class PayloadCacheFiller implements Runnable {
        final String queueUrl;
        public PayloadCacheFiller(String queueUrl) {
            this.queueUrl = queueUrl;
        }
        @Override
        public void run() {
            CQSControllerServlet.valueAccumulator.initializeAllCounters();            
            ShardedJedis jedis = null;
            boolean brokenJedis = false;
            try {
                jedis = RedisCachedCassandraPersistence.getResource();
                //delete the existing stale cache. Messages in it are probably messages 
                //that were not deleted due to race conditions between previous cache-filler
                //and deleteMessage requests
                jedis.del(queueUrl + "-P");
                int count = CMBProperties.getInstance().getRedisPayloadCacheSizePerQueue();
                List<String> orderedIds = idSeq.getIdsFromHead(queueUrl, count);
                if (orderedIds.size() > 0) {
                    if (orderedIds.size() > count) {
                        orderedIds = orderedIds.subList(0, count);
                    }
                    List<String> messageIds = new LinkedList<String>();
                    for (String id : orderedIds) {
                        messageIds.add(RedisCachedCassandraPersistence.getMemQueueMessageMessageId(id));
                    }

                    List<List<String>> lofl = Util.splitList(messageIds, 1000);
                    for (List<String> messageIdSubset : lofl) {
                        Map<String, CQSMessage> ret = persistenceStorage.getMessages(queueUrl, messageIdSubset);
                        Set<String> retKeys = ret.keySet();
                        Set<String> origKeys = new TreeSet<String>(messageIdSubset);
                        origKeys.removeAll(retKeys);
                        logger.info("size of origSet not returned by underlying layer:" + origKeys.size());
                        addMessagesToPayloadCache(queueUrl, new LinkedList<CQSMessage>(ret.values()), jedis);                    
                        logger.info("event=PayloadCacheFiller status=success cachedSize=" + messageIdSubset.size());
                    }
                }
                logger.info("event=PayloadCacheFiller CassandraTime=" + CQSControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime) + " redisTime=" + CQSControllerServlet.valueAccumulator.getCounter(AccumulatorName.RedisTime));
            } catch(Exception e) {
                logger.warn("event=PayloadCacheFiller status=failure", e);
                if (e instanceof JedisException) brokenJedis = true;
            } finally {
                if (jedis != null) {
                    RedisCachedCassandraPersistence.returnResource(jedis, brokenJedis);
                }
                boolean done = false;
                while (!done) {
                    try {
                        checkAndSetCacheFillingState(queueUrl, false);
                        done = true;
                    } catch(SetFailedException e) {
                        logger.error("event=PayloadCacheFiller populateCacheJobStatus=failure queueUrl=" + queueUrl);
                    }
                }
                CQSControllerServlet.valueAccumulator.deleteAllCounters();
            }
        }
    }
    
        
    private static void addMessagesToPayloadCache(String queueUrl, List<CQSMessage> messages, ShardedJedis jedis) throws IOException {
        if (messages.size() == 0) return;
        HashMap<byte[], byte[]> keyToVal = new HashMap<byte[], byte[]>();
        for (CQSMessage message : messages) {
            if (message == null) continue;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(message);
            oos.close(); 
            keyToVal.put(SafeEncoder.encode(message.getMessageId()), baos.toByteArray());
        }
        long ts1 = System.currentTimeMillis();
        if (keyToVal.size() == 0) return;
        String status = jedis.hmset(SafeEncoder.encode(queueUrl + "-P"), keyToVal);
        CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (System.currentTimeMillis() - ts1));
        if (!status.equals("OK")) {
            logger.warn("event=addMessagesToPayloadCache queueUrl=" + queueUrl + " status=" + status);
        }        
    }
    
    private static CQSMessage deserializeMessage(byte []val) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(val);
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (CQSMessage)ois.readObject();
    }

    
    @Override
    public String sendMessage(CQSQueue queue, CQSMessage message) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException {
        String messageId = persistenceStorage.sendMessage(queue, message);        
        return messageId;
    }

    @Override
    public Map<String, String> sendMessageBatch(CQSQueue queue, List<CQSMessage> messages) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException {
        Map<String, String> messageIds = persistenceStorage.sendMessageBatch(queue, messages);
                
        return messageIds;        
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) throws PersistenceException {
        ShardedJedis jedis = null;
        boolean brokenJedis = false;
        try {
            long ts1 = System.currentTimeMillis();
            jedis = RedisCachedCassandraPersistence.getResource();
            jedis.hdel(SafeEncoder.encode(queueUrl + "-P"), SafeEncoder.encode(receiptHandle));
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (System.currentTimeMillis() - ts1));            
            logger.info("event=deleteMessage cacheDelete=success");
        } catch(Exception e) {
            logger.warn("event=deleteMessage cacheDelete=failure desc=" + e.getMessage(), e);
            if (e instanceof JedisException) brokenJedis = true;
        } finally {
            if (jedis != null) {
                RedisCachedCassandraPersistence.returnResource(jedis, brokenJedis);
            }
        }
        
        persistenceStorage.deleteMessage(queueUrl, receiptHandle);        
    }

    @Override
    //PayloadCache does nothing for recieveMessage. its a pass-through
    public List<CQSMessage> receiveMessage(CQSQueue queue,
            Map<String, String> receiveAttributes) throws PersistenceException,
            IOException, NoSuchAlgorithmException, InterruptedException {

        throw new IllegalStateException("Operation not supported");
        
    }

    @Override
    //PayloadCache does nothing for changeMessageVisibility. its a pass-through
    public boolean changeMessageVisibility(CQSQueue queue, String receiptHandle, int visibilityTO) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException {
        throw new IllegalStateException("Operation not supported");
    }

    @Override
    //peek is called by upper layer when filling cache. This is where we can cache messages
    //Note: this implementation is tightly coupled in the way the RedisCachedCassandraPersistence calls
    //peekQueue to fill-cache. If peekQueue is called multiple times, we can potentially be caching
    //the same message multiple times but that's just over-writing over the same message.
    public List<CQSMessage> peekQueue(String queueUrl, String previousReceiptHandle, String nextReceiptHandle, int length) throws PersistenceException, IOException, NoSuchAlgorithmException {
        
        List<CQSMessage> messages = persistenceStorage.peekQueue(queueUrl, previousReceiptHandle, nextReceiptHandle, length);
        ShardedJedis jedis = null;
        boolean brokenJedis = false;
        try {            
            long ts1 = System.currentTimeMillis();
            jedis = RedisCachedCassandraPersistence.getResource();
            List<CQSMessage> messagesToCache = Collections.emptyList();
            Long hlen = jedis.hlen(queueUrl + "-P");
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (System.currentTimeMillis() - ts1));
            if (hlen == null || hlen < CMBProperties.getInstance().getRedisPayloadCacheSizePerQueue()) {
                int count = (int) (CMBProperties.getInstance().getRedisPayloadCacheSizePerQueue() - hlen);
                if (count < messages.size()) {
                    messagesToCache = messages.subList(0, count);
                } else {
                    messagesToCache = messages;
                }
                addMessagesToPayloadCache(queueUrl, messagesToCache, jedis);
            }
            logger.info("event=peekQueue cacheSet=success numCached=" + messagesToCache.size() + " queue_url=" + queueUrl);
        } catch(Exception e) {
            logger.warn("event=peekQueue cacheSet=failure queue_url=" + queueUrl, e);
            if (e instanceof JedisException) brokenJedis = true;
        } finally {
            if (jedis != null) {
                RedisCachedCassandraPersistence.returnResource(jedis, brokenJedis);
            }
        }
        
        return messages;
        
    }

    @Override
    public void clearQueue(String queueUrl) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        ShardedJedis jedis = null;
        boolean brokenJedis = false;
        try {
            long ts1 = System.currentTimeMillis();
            jedis = RedisCachedCassandraPersistence.getResource();
            jedis.del(queueUrl + "-P");
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (System.currentTimeMillis() - ts1));
            logger.warn("event=clearQueue cacheSet=success");
        } catch(Exception e) {
            logger.warn("event=clearQueue cacheSet=failure desc=" + e.getMessage(), e);
            if (e instanceof JedisException) brokenJedis = true;
        } finally {
            if (jedis != null) {
                RedisCachedCassandraPersistence.returnResource(jedis, brokenJedis);
            }
        }
        persistenceStorage.clearQueue(queueUrl);        
    }

    @Override
    /**
     * Message tries to get all messages from payload cache. For messages not found it fetches from underlying
     * persistence storage and kicks off a PayloadFilling job.    
     * @param queueUrl
     * @param ids The messageIds
     * @return map of messageId to CQSMessage object.
     * @throws PersistenceException
     * @throws NoSuchAlgorithmException
     * @throws UnsupportedEncodingException
     */
    public Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        //Its not too useful to cache the missed messages since they will most probably be deleted.
        //Instead missing implies empty cache and we should preFetch the next batch-size.
        if (ids.size() == 0) return Collections.emptyMap();
        HashMap<String, CQSMessage> messageIdToMessage = new HashMap<String, CQSMessage>();
        HashMap<Integer, String> idxToMessageId = new HashMap<Integer, String>(ids.size());
        int i = 0;
        for (String id : ids) {
            idxToMessageId.put(i++, id);
        }
        List<String> missedIds = new LinkedList<String>();
        ShardedJedis jedis = null;
        boolean brokenJedis = false;
        try {
            long ts1 = System.currentTimeMillis();
            jedis = RedisCachedCassandraPersistence.getResource();
            List<byte[]> fields = new LinkedList<byte[]>();
            for (String id : ids) {
                fields.add(SafeEncoder.encode(id));
            }
            
            List<byte[]> vals = jedis.hmget(SafeEncoder.encode(queueUrl + "-P"), fields.toArray(new byte[0][0]));
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (System.currentTimeMillis() - ts1));

            if (vals.size() != ids.size()) {
                throw new IllegalStateException("hmget did not recieve response for every key. val-size=" + vals.size() + ", ids-size=" + ids.size());
            }
            i = 0;
            for (byte []val : vals) {
                if (val != null) {
                    CQSMessage msg = deserializeMessage(val);
                    String messageId = idxToMessageId.get(i);
                    msg.setMessageId(messageId);
                    messageIdToMessage.put(messageId, msg);
                } else {
                    missedIds.add(idxToMessageId.get(i));
                }
                i++;                
            }
            
            //by here missedIds has all missed Ids, messageIdToMessage already populated with cached CQSMessages
        } catch(Exception e) {
            logger.warn("event=getMessages cacheGet=failure desc=" + e.getMessage(), e);
            if (e instanceof JedisException) brokenJedis = true;
            missedIds = ids; //get all form underlying storage
        } finally {
            if (jedis != null) {
                RedisCachedCassandraPersistence.returnResource(jedis, brokenJedis);
            }
        }
        
        //by here missedIds has messages to get from underlying storage
        if (missedIds.size() > 0) {
            if (shouldEnablePCache(queueUrl)) {
                try {            
                    if (!checkAndSetCacheFillingState(queueUrl, true)) {
                        RedisCachedCassandraPersistence.executor.submit(new PayloadCacheFiller(queueUrl));
                        logger.info("event=getMessages populateCacheJobStatus=scheduled queueUrl=" + queueUrl);
                    }
                } catch (Exception e) {
                    logger.info("event=getMessages populateCacheJobStatus=not_scheduled desc=someone_else_beat_us_or_exception queueUrl=" + queueUrl, e);
                }
            }
            Map<String, CQSMessage> messageIdToMessagePers = persistenceStorage.getMessages(queueUrl, missedIds);
            for (Map.Entry<String, CQSMessage> entry : messageIdToMessagePers.entrySet()) {
                messageIdToMessage.put(entry.getKey(), entry.getValue());
            }
        }
        logger.info("event=getMessages satus=success missedIds=" + missedIds.size() + " cachedIds=" + (ids.size() - missedIds.size()));
        CQSMonitor.Inst.registerCacheHit(queueUrl, (ids.size() - missedIds.size()), ids.size(), CacheType.PayloadCache);
        return messageIdToMessage;
    }
    
    /**
     * 
     * @param queueUrl
     * @return true if the payload cache should be enabled for this queue. false otherwise
     * Assumed: This will be called when pCache is effectively empty.
     */
    private boolean shouldEnablePCache(String queueUrl) {
        //Check if there are enough messages in the QCache
        int qCount = idSeq.getQCount(queueUrl); 
        if (qCount > 1000) {            
            //This implies there was a burst of sends. Kick off a payload cache filling
            logger.info("event=shouldEnablePCache status=yes qcount=" + qCount);
            return true;
        }
        logger.debug("event=shouldEnablePCache status=no qcount=" + qCount);
        return false;
    }

    @Override
    public List<CQSMessage> peekQueueRandom(String queueUrl, int length)
            throws PersistenceException, IOException, NoSuchAlgorithmException {
        return persistenceStorage.peekQueueRandom(queueUrl, length);
    }

    
}
