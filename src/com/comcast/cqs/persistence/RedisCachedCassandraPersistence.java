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
package com.comcast.cqs.persistence;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import me.prettyprint.hector.api.exceptions.HTimedOutException;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cqs.controller.CQSCache;
import com.comcast.cqs.controller.CQSControllerServlet;
import com.comcast.cqs.controller.CQSMonitor;
import com.comcast.cqs.controller.CQSMonitor.CacheType;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.Util;

/**
 * This class uses Redis as cache in front of our Cassandra access.
 * Currently we only cache message-ids and hidden/delayed state of messages.
 * The payloads are fetched from underlying persistence layer.
 * 
 * The class must exist as a singleton
 * @author aseem, bwolf, vvenkatraman
 * 
 * Class is thread-safe
 */

public class RedisCachedCassandraPersistence implements ICQSMessagePersistence, ICQSMessagePersistenceIdSequence {
	
    private static final Logger logger = Logger.getLogger(RedisCachedCassandraPersistence.class);
    private static final Random rand = new Random();
    
    private static RedisCachedCassandraPersistence Inst;
    
    public static ExecutorService executor;
    public static ExecutorService revisibilityExecutor;
    
    public final TestInterface testInterface = new TestInterface();
    
    /**
     * 
     * @return Singleton implementation of this object
     */
    public static RedisCachedCassandraPersistence getInstance() {
        return Inst;
    }
    
    private static JedisPoolConfig cfg = new JedisPoolConfig();
    private static ShardedJedisPool pool;
    static {
        initializeInstance();
        initializePool();
    }
    
    private static void initializeInstance() {
        CQSMessagePartitionedCassandraPersistence cassandraPersistence = new CQSMessagePartitionedCassandraPersistence();
        Inst = new RedisCachedCassandraPersistence(cassandraPersistence);            
    }
    
    private volatile ICQSMessagePersistence persistenceStorage; 
    
    private RedisCachedCassandraPersistence(ICQSMessagePersistence persistenceStorage) {
        this.persistenceStorage = persistenceStorage;
    }
    /**
     * Initialize the Redis connection pool
     */
    private static void initializePool() {
    	
        cfg.maxActive = CMBProperties.getInstance().getRedisConnectionsMaxActive();
        cfg.maxIdle = -1;
        List<JedisShardInfo> shardInfos = new LinkedList<JedisShardInfo>();
        String serverList = CMBProperties.getInstance().getRedisServerList();
        
        if (serverList == null) {
            throw new RuntimeException("Redis server list not specified");
        }
        
        String []arr = serverList.split(",");
        
        for (int i = 0; i < arr.length; i++) {
            
        	String []hostPort = arr[i].trim().split(":");
        	JedisShardInfo shardInfo = null;
        	if (hostPort.length != 2) {
        		// use Redis default port if one wasn't specified
        		 shardInfo = new JedisShardInfo(hostPort[0].trim(), 6379, 4000);
            } else {
            	 shardInfo = new JedisShardInfo(hostPort[0].trim(), Integer.parseInt(hostPort[1].trim()), 4000);
            }
            shardInfos.add(shardInfo);
        }
        
        pool = new ShardedJedisPool(cfg, shardInfos);
        executor = Executors.newFixedThreadPool(CMBProperties.getInstance().getRedisFillerThreads());
        revisibilityExecutor = Executors.newFixedThreadPool(CMBProperties.getInstance().getRedisRevisibleThreads());
        
        logger.info("event=initialize_redis pools_size=" + shardInfos.size() + " max_active=" + cfg.maxActive + " server_list=" + serverList);
    }
    
    /**
     * Possible states for queue
     * State if Unavailable should be set when any code determines the queue is in bad state or unavailable.
     * The checkCacheConsistency will take care of performing the appropriate actions on that queue
     */
    public enum QCacheState {
        Filling, //Cache is being filled by a thread 
        OK, //Cache is good for use 
        Unavailable; //Cache is unavailable for a single Q due to inconsistency issues.
    }

    public class TestInterface {
        public void setCassandraPersistence(ICQSMessagePersistence pers) {
            persistenceStorage = pers;
        }
        public ICQSMessagePersistence getCassandraPersistence() {
            return persistenceStorage;
        }
        public QCacheState getCacheState(String q) {
            return RedisCachedCassandraPersistence.this.getCacheState(q, 0);
        }        
        
        public void setCacheState(String queueUrl, QCacheState state, QCacheState oldState, boolean checkOldState) throws SetFailedException {
            RedisCachedCassandraPersistence.this.setCacheState(queueUrl, 0, state, oldState, checkOldState);
        }
        
        public String getMemQueueMessage(String messageId) {
            return RedisCachedCassandraPersistence.getMemQueueMessage(messageId);
        }
        
        public long getMemQueueMessageCreatedTS(String memId) {
            return RedisCachedCassandraPersistence.getMemQueueMessageCreatedTS(memId);
        }
        
        public int getMemQueueMessageInitialDelay(String memId) {
            return RedisCachedCassandraPersistence.getMemQueueMessageInitialDelay(memId);
        }
        
        public String getMemQueueMessageMessageId(String queueUrlHash, String memId) {
            return RedisCachedCassandraPersistence.getMemQueueMessageMessageId(queueUrlHash,memId);
        }
        
        public void resetTestQueue() {
            ShardedJedis jedis = getResource();
            try {
                jedis.del("testQueue-0-" + CQSConstants.REDIS_STATE);
                jedis.del("testQueue-0-Q");
                jedis.del("testQueue-0-H");
                jedis.del("testQueue-0-R");
                jedis.del("testQueue-0-F");
                jedis.del("testQueue-0-V");
                jedis.del("testQueue-0-VR");
            } finally {
                returnResource(jedis);
            }
        }
        
        public ShardedJedis getResource() {
            return RedisCachedCassandraPersistence.getResource();
        }
        
        public void returnResource(ShardedJedis jedis) {
            RedisCachedCassandraPersistence.returnResource(jedis, false);
        }
        
        public boolean checkCacheConsistency(String queueUrl) {
            return RedisCachedCassandraPersistence.this.checkCacheConsistency(queueUrl, 0,false);
        }
        
        public void scheduleRevisibilityProcessor(String queueUrl) throws SetFailedException {
            revisibilityExecutor.submit(new RevisibleProcessor(queueUrl, 0));
        }
    }
    
    // each queue's state is represented by the HashTable with key <Q>-S and the states are defined in the enum QCacheState
    
    static AtomicInteger numRedisConnections = new AtomicInteger(0);
    
    public int getNumRedisConnections() {
        return numRedisConnections.get();
    }
    
    public static ShardedJedis getResource() {
        ShardedJedis conn = pool.getResource();
        numRedisConnections.incrementAndGet();
        return conn;        
    }
    
    public static void returnResource(ShardedJedis jedis, boolean broken) {
        if (numRedisConnections.intValue() < 1) {
            throw new IllegalStateException("Returned more connections than acquired from pool");
        }
        numRedisConnections.decrementAndGet();
        if (broken) {
            pool.returnBrokenResource(jedis);
        } else {
            pool.returnResource(jedis);
        }
    }

    /**
     * Class represents the race condition where the caller was beaten by someone else.
     * In case of this exception, check the initial state again and retry if necessary.
     */
    static class SetFailedException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    /**
     * 
     * @param queueUrl
     * @return The state of the queue or null if none exists
     */
    private QCacheState getCacheState(String queueUrl, int shard) {
        
    	long ts1 = System.currentTimeMillis();
        ShardedJedis jedis = getResource();
        boolean brokenJedis = false;
        
        try {
            String st = jedis.hget(queueUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE, CQSConstants.REDIS_STATE);
            if (st == null) {
            	return null;
            }
            return QCacheState.valueOf(st);
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            returnResource(jedis, brokenJedis);
            long ts2 = System.currentTimeMillis();
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
        }
    }
    
    /**
     * Set the state for a queue or throw exception if someone else beat us to it.
     * This is an atomic operation.
     * @param queueUrl
     * @param checkOldState - check the oldState in transaction
     * @param oldState If checkOldstate is set we check within the WATCH..EXEC scope
     *  if the state is still the same as the old value. If not, we throw SetfailedException
     *  This helps do atomic CAS operations 
     * @param state State to set to. if null, then the state field is deleted
     * @throws SetFailedException
     */
    private void setCacheState(String queueUrl, int shard, QCacheState state, QCacheState oldState, boolean checkOldState) throws SetFailedException {
        
    	long ts1 = System.currentTimeMillis();
        boolean brokenJedis = false;
        ShardedJedis jedis = getResource();
        try {
            Jedis j = jedis.getShard(queueUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE);
            j.watch(queueUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE);
            if (checkOldState) {
                String oldStateStr = j.hget(queueUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE, CQSConstants.REDIS_STATE);
                if (oldState == null && oldStateStr != null) {
                    throw new SetFailedException();
                }
                if (oldState != null) {
                    if (oldStateStr == null || QCacheState.valueOf(oldStateStr) != oldState) {
                        j.unwatch();
                        throw new SetFailedException();
                    }
                }
            }
            Transaction tr = j.multi();
            if (state == null) {
                tr.hdel(queueUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE, CQSConstants.REDIS_STATE);
            } else {
                tr.hset(queueUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE, CQSConstants.REDIS_STATE, state.name());
            }
            List<Object> resp = tr.exec();
            if (resp == null) {
                throw new SetFailedException();
            }
        } catch(JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            returnResource(jedis, brokenJedis);
            long ts2 = System.currentTimeMillis();
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
        }
    }
    
    /**
     * Sets the sentinel flag in Redis denoting either visibility processor is running or cache filler is
     * running to prevent multiple from running and auto-expiring older runs.
     * @param queueUrl
     * @param visibilityOrCacheFiller
     */
    private void setCacheFillerProcessing(String queueUrl, int shard, int exp) {
        long ts1 = System.currentTimeMillis();
        boolean brokenJedis = false;
        ShardedJedis jedis = getResource();
        String suffix = "-F";
        try {
            if (exp > 0) {
                jedis.set(queueUrl  + "-" + shard + suffix, "Y");
                jedis.expire(queueUrl + "-" + shard + suffix, exp); //expire after exp seconds
            } else {
                jedis.del(queueUrl + "-" + shard + suffix);
            }
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            returnResource(jedis, brokenJedis);
            long ts2 = System.currentTimeMillis();
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
        }        
    }
    
    // TODO: once we can expect people to have Redis 2.6.12 or newer, the set command takes additional arguments that will make this
    // much simpler.  It will also require a newer version of the Jedis library to support it.
    // Example:
    // String resp = j.set(redisKey,"Y","NX","EX",exp)
    // return ( resp == nil)
    // 
    private static boolean checkAndSetFlag(String queueUrl, int shard, String suffix, int exp) throws SetFailedException {
        if (exp <= 0) {
        	throw new IllegalArgumentException("Redis expiration cannot be less than 0");
        }
        long ts1 = System.currentTimeMillis();
        boolean brokenJedis = false;
        String redisKey = queueUrl + "-" + shard + suffix;
        ShardedJedis jedis = getResource();
        try {
            if (jedis.exists(redisKey)) {
                return false;
            }
            Jedis j = jedis.getShard(redisKey);
            j.watch(redisKey);
           
            String val = j.get(redisKey);
            if (val != null) {
                j.unwatch();
                return false;
            }
            Transaction tr = j.multi();

            //sentinel expired. kick off new RevisibleProcessor job
            tr.set(redisKey, "Y");
            tr.expire(redisKey, exp); //expire after exp seconds
            // since we have called watch, tr.exec will return null in the case that someone else has modified
            // the redisKey since we started our transaction.  If it doesn't return null, the value hasn't changed out from
            // under us, so we return true since we set it
            List<Object> resp = tr.exec();
            return resp != null; 
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            returnResource(jedis, brokenJedis);
            long ts2 = System.currentTimeMillis();
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
        }         
    }
        
    /**
     * Check whether a visibility processing was kicked off within the last exp seconds. If not, kick one off.
     * Method is atomic.
     * @param queueUrl
     * @param exp in seconds
     * @return true if sentinel was set. false otherwise
     */
    private boolean checkAndSetVisibilityProcessing(String queueUrl, int shard, int exp) throws SetFailedException {
        if (checkAndSetFlag(queueUrl, shard, "-R", exp)) {
            revisibilityExecutor.submit(new RevisibleProcessor(queueUrl, shard));
            return true;
        }
        return false;
    }
    
    /**
     * Processes the re-visible set once every exp seconds synchronously.
     * @param queueUrl
     * @param exp
     * @return true if Revisible set processing was done. false otherwise
     */
    private static boolean checkAndProcessRevisibleSet(String queueUrl, int shard, int exp) throws SetFailedException {
        if (checkAndSetFlag(queueUrl, shard, "-VR", exp)) {
            //perform re-visible set processing. Get all memIds whose score (visibilityTO) is <= now
            long ts1 = System.currentTimeMillis();
            long ts2 = 0;
            boolean brokenJedis = false;
            ShardedJedis jedis = getResource();
            try {
                //jedis is lame and does not have a constant for "-inf" which Redis supports. So we have to
                //pick an arbitrary old min value.
                Set<String> revisibleSet = jedis.zrangeByScore(queueUrl + "-" + shard + "-V", System.currentTimeMillis() - (1000 * 3600 * 24 * 14), System.currentTimeMillis());
                for (String revisibleMemId : revisibleSet) {
                    jedis.rpush(queueUrl + "-" + shard + "-Q", revisibleMemId);
                    jedis.zrem(queueUrl + "-" + shard + "-V", revisibleMemId);
                }
                ts2 = System.currentTimeMillis();
                if (revisibleSet.size() > 0) {
                	logger.debug("event=redis_revisibility queue_url=" + queueUrl + " shard=" + shard + " num_made_revisible=" + revisibleSet.size() + " res_ts=" + (ts2 - ts1));
                }
            } catch (JedisException e) {
                brokenJedis = true;
                throw e;
            } finally {
                returnResource(jedis, brokenJedis);
                if (ts2 == 0) ts2 = System.currentTimeMillis();
                CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
            }         
            
            return true;
        }
        return false;        
    }
    
    private static boolean tryCheckAndProcessRevisibleSet(String queueUrl, int shard, int exp) {
        try {
            if (checkAndProcessRevisibleSet(queueUrl, shard, exp)) {                                                
                return true;
            } else {
                return false;
            }
        } catch (SetFailedException e) {
            return false;
        }
    }
    
    private boolean getProcessingState(String queueUrl, int shard, boolean visibilityOrCacheFiller) {
        long ts1 = System.currentTimeMillis();
        boolean brokenJedis = false;
        ShardedJedis jedis = getResource();
        String suffix = visibilityOrCacheFiller ? "-R" : "-F";
        try {
            return jedis.exists(queueUrl + "-" + shard + suffix);
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            returnResource(jedis, brokenJedis);
            long ts2 = System.currentTimeMillis();
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
        }        
    }
    
    //below are helper methods for encoding Cassandra-message-id into in-memory message-id
    
    /**
     * In memory message has format addedTS:initialDelay:<messge-id>
     * example - 1335302314:0:ABCDEFGHIJK
     * @param messageId The Cassandra message-id
     * @return The in-memory message-id
     */
    private static String getMemQueueMessage(String messageId) {
        if (messageId.length() == 0) {
            throw new IllegalArgumentException("Messge Id cannot be an empty string");
        }
        //messageID is like 45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:-8763141905575923938
        //the first part 45c1596598f85ce59f060dc2b8ec4ebb is hash of queue url, so replace it with 0 to save space in Redis
        //return example: 0:0:0_0_72:2923737900040323074:-8763141905575923938
        StringBuffer sb = new StringBuffer();
        sb.append("0").append(':').append("0").append(":0").append(messageId.substring(messageId.indexOf("_")));
        return sb.toString();
    }
    
    /**
     * 
     * @param memId
     * @return The created timestamp encoded in the memId
     */
    //Example 0:0:0_0_72:2923737900040323074:-8763141905575923938
    public static long getMemQueueMessageCreatedTS(String memId) {
        String []arr = memId.split(":");
        if (arr.length < 5) {
            throw new IllegalArgumentException("Bad format for memId. Must be of the form 0:0:<messge-id>. Got: " + memId);
        }
        return CassandraPersistence.getTimestampFromHash(Long.parseLong(arr[3]));        
    }
    
    /**
     * 
     * @param memId
     * @return The initial delay encoded in the memId
     */
    private static int getMemQueueMessageInitialDelay(String memId) {
        String []arr = memId.split(":");
        if (arr.length < 3) {
            throw new IllegalArgumentException("Bad format for memId. Must be of the form 0:0:<messge-id>. Got: " + memId);
        }
        return Integer.parseInt(arr[1]);                
    }
    
    /**
     * @param memId
     * @return The message-id encoded in memId
     */
    static String getMemQueueMessageMessageId(String queueUrlHash, String memId) {        
        String []arr = memId.split(":");
        if (arr.length < 3) {
            throw new IllegalArgumentException("Bad format for memId. Must be of the form 0:0:<messge-id>. Got: " + memId);
        }
        //memId example: 0:0:0_0_72:2923737900040323074:-8763141905575923938
        //replace arr[2] first 0 to queueUrlHash and return example: 45c1596598f85ce59f060dc2b8ec4ebb_0_72:2923737900040323074:-8763141905575923938
        StringBuffer sb = new StringBuffer(queueUrlHash);
        sb.append(arr[2].substring(arr[2].indexOf("_")));
        for (int i = 3; i < arr.length; i++) {
            sb.append(":").append(arr[i]);
        }
        return sb.toString();
    }
    
    /**
     * Class clears and then fills the cache for a given queue
     * When its done, it updates the following cache keys are available:
     *  <Q>-S = OK
     *  <Q>-Q = The in-memory list ("queue") of message ids
     *  <Q>-H = The hash table of hidden message ids along with the timestamp for when they should become re-visible
     *  <Q>-R = Existence implies recent processing of revisibility
     *  <Q>-F = Existence implies currently running CacheFiller
     *  <Q>-V = Set of delayed message ids implemented as a sorted set. Delayed messages cannot be deleted before they
     *          become visible. We synchronously retrieve due message ids from this set every so often as part of receivemessage()
     *  <Q>-VR = the sentinel flag the absence of which implies its time to process the delayed set.
     *  <Q>-A-<messageId> = The attributes for a message in a queue. Note, this requires that the messageId remain the same
     *    throughout the life-time of a message.
     */
    private class CacheFiller implements Runnable {
        final String queueUrl;
        final int shard;
        public CacheFiller(String queueUrl, int shard) {
            this.queueUrl = queueUrl;
            this.shard = shard;
        }
        @Override
        public void run() {
            CQSControllerServlet.valueAccumulator.initializeAllCounters();            
            //clear all existing in-memQueue and hidden set
            boolean brokenJedis = false;
            long ts1 = System.currentTimeMillis();
            ShardedJedis jedis = getResource();
            try {
            	logger.info("event=cache_filler_started queue_url=" + queueUrl + " shard=" + shard);
            	jedis.del(queueUrl + "-" + shard + "-Q");
                jedis.del(queueUrl + "-" + shard + "-H");
                jedis.del(queueUrl + "-" + shard + "-R");
                String previousReceiptHandle = null;
                List<CQSMessage> messages = persistenceStorage.peekQueue(queueUrl, shard, null, null, 1000);
                int totalCached = 0;
                while (messages.size() != 0) {
                    for (CQSMessage message : messages) {
                        addMessageToCache(queueUrl, shard, message, jedis);
                        totalCached++;
                    }
                    previousReceiptHandle = messages.get(messages.size() - 1).getMessageId();
                    messages = persistenceStorage.peekQueue(queueUrl, shard, previousReceiptHandle, null, 1000); //note: shard parameter should be in sync with receipt handle here
                }
                setCacheState(queueUrl, shard, QCacheState.OK, null, false);
                setCacheFillerProcessing(queueUrl, shard, 0);
                long ts3 = System.currentTimeMillis();
                //logger.debug("event=filled_cache  queue_url=" + queueUrl + " shard=" + shard +" num_cached=" + totalCached + " res_ms=" + (ts3 - ts1) + " redis_ms=" + CQSControllerServlet.valueAccumulator.getCounter(AccumulatorName.RedisTime));
                logger.info("event=cache_filler_finished  queue_url=" + queueUrl + " shard=" + shard +" num_cached=" + totalCached + " total_ms=" + (ts3 - ts1) + " redis_ms=" + CQSControllerServlet.valueAccumulator.getCounter(AccumulatorName.RedisTime) + " cass_ms=" + CQSControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime));
            } catch (Exception e) {
                if (e instanceof JedisException) {
                	brokenJedis = true;
                }
                //logger.error("event=cache_filler", e);
                logger.error("event=cache_filler_failed", e);
                trySettingCacheState(queueUrl, shard, QCacheState.Unavailable);
            } finally {
                returnResource(jedis, brokenJedis);
                CQSControllerServlet.valueAccumulator.deleteAllCounters();
            }
        }
    }
    
    /**
     * Class goes through all the hidden messages for a specific queue
     * and makes re-visible anything that was not deleted and has visibilityTO expired
     * Note: this is a low priority thread.
     */
    private class RevisibleProcessor implements Runnable {
    	
        private Logger log = Logger.getLogger(RevisibleProcessor.class);
        private final String queueUrl;
        private final int shard;
        private boolean setAndDeleteCounters = true;
        
        public RevisibleProcessor(String queueUrl, int shard) {
            this.queueUrl = queueUrl;
            this.shard = shard;
        }
        
        @Override
        public void run() {
            if (setAndDeleteCounters) {
                CQSControllerServlet.valueAccumulator.initializeAllCounters();
            }
            long ts0 = System.currentTimeMillis();
            QCacheState state = getCacheState(queueUrl, shard);
            if (state == null || state != QCacheState.OK) {
                log.debug("event=revisibility_check queue_url=" + queueUrl + " shard=" + shard +" queue_cache_state=" + (state==null?"null":state.name()) + " action=do_nothing");
                return;
            }
            boolean brokenJedis = false;
            ShardedJedis jedis = null;
            List<String> memIds = new LinkedList<String>();
            try {
                jedis = getResource();
                updateExpire(queueUrl, shard, jedis);
                long ts1 = System.currentTimeMillis();
                Set<String> keys = jedis.hkeys(queueUrl + "-" + shard + "-H");
                long ts2 = System.currentTimeMillis();
                CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
                log.debug("event=revisibility_check queue_url=" + queueUrl + " shard=" + shard + " invisible_set_size=" + keys.size());
                for (String key : keys) {
                    long ts3 = System.currentTimeMillis();
                    String val = jedis.hget(queueUrl + "-" + shard + "-H", key);
                    long ts4 = System.currentTimeMillis();
                    CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts4 - ts3));
                    if (val == null) {
                    	continue;                    
                    }
                    long visibilityTo = Long.parseLong(val);
                    if (visibilityTo < System.currentTimeMillis()) {
                        memIds.add(key);
                    }
                }
                //process memIds that should be re-visible
                for (String memId : memIds) {
                	jedis.rpush(queueUrl + "-" + shard + "-Q", memId);
                    jedis.hdel(queueUrl + "-" + shard + "-H", memId);
                }
                long ts3 = System.currentTimeMillis();
                log.debug("event=revisibility_check queue_url=" + queueUrl + " shard=" + shard + " num_made_revisible=" + memIds.size() + " redisTime=" + CQSControllerServlet.valueAccumulator.getCounter(AccumulatorName.RedisTime) + " responseTimeMS=" + (ts3 - ts0) + " hidden_set_size=" + keys.size());
            } catch (Exception e) {
                if (e instanceof JedisException) {
                	brokenJedis = true;
                }
                log.error("event=revisibility_check", e);
            } finally {
                if (jedis != null) {
                    returnResource(jedis, brokenJedis);
                }
                if (setAndDeleteCounters) {
                    CQSControllerServlet.valueAccumulator.deleteAllCounters();
                }
            }
        }
        
    }
    /**
     * Check if the queue is in the cache and in ok state. Else kick off initialization
     * and return false. 
     * @param queueUrl
     * @param trueOnFiller returns true if the current state is Filling.
     * @return true if the cache is good for use. false if it is unavailable
     */
    public boolean checkCacheConsistency(String queueUrl, int shard, boolean trueOnFiller) {
        try {
            // check if cache's state exists, if not return false and initialize cache
            // if cache's state is not OK, return false
            QCacheState state = getCacheState(queueUrl, shard);
            if (state == null || state == QCacheState.Unavailable) {
                try {
                    setCacheFillerProcessing(queueUrl, shard, 20 * 60); // this must be before setCacheState or else a race-condition                    
                    setCacheState(queueUrl, shard, QCacheState.Filling, state, true);
                    // we successfully set the state to filling, so we queue up filling job
                    executor.submit(new CacheFiller(queueUrl, shard));
                    logger.debug("event=initialize_queue_cache cache_state=" + (state == null ? "null" : state.name()) + " queue_url=" + queueUrl + " shard=" + shard);                        
                } catch (SetFailedException e) {
                    // someone beat us to it, so we return false for now and check again next time
                    logger.debug("event=initialize_queue_cache cache_state=" + (state == null ? "null" : state.name()) + " queue_url=" + queueUrl + " shard=" + shard);
                    state = getCacheState(queueUrl, shard);
                    if (state != null && state == QCacheState.Filling && trueOnFiller) {
                        return true; // otherwise continue and return false
                    }
                }
                return false;
            } else if (state != QCacheState.OK) { //must be filling
                // check if this is a stale filler
                if (!getProcessingState(queueUrl, shard, false)) {
                    try {
                        setCacheState(queueUrl, shard, null, null, false);
                        logger.debug("event=clear_stale_cache_filler cache_state=" + (state == null ? "null" : state.name()) + " queue_url=" + queueUrl + " shard=" + shard);
                    } catch (SetFailedException e) {
                        logger.debug("event=clear_stale_cache_filler cache_state=" + (state == null ? "null" : state.name()) + " queue_url=" + queueUrl + " shard=" + shard);
                    }
                }
                if (trueOnFiller && state == QCacheState.Filling) {
                    return true;
                }
                return false;
            }
            return true;
        } catch (JedisConnectionException e) {
            logger.warn("event=check_cache_consistency error_code=redis_unavailable num_connections=" + numRedisConnections.get());
            return false;
        }
    }

    /**
     * Add message to in-memory-queue. creationTS is now
     * @param queueUrl
     * @param message
     */
    private void addMessageToCache(String queueUrl, int shard, CQSMessage message, ShardedJedis jedis) {
        long ts1 = System.currentTimeMillis();
        Long newLen = jedis.rpush(queueUrl + "-" + shard + "-Q", getMemQueueMessage(message.getMessageId())); //TODO: currently initialDelay is always 0
        if (newLen.longValue() == 0) {
            throw new IllegalStateException("Could not add memId to queue");
        }
        long ts2 = System.currentTimeMillis();
        CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
    }
    
    /**
     * Method tries to set cache state and swallows all exceptions that are thrown
     * @param queueUrl
     * @param state
     */
    private void trySettingCacheState(String queueUrl, int shard, QCacheState state) {
        try {
            setCacheState(queueUrl, shard, state, null, false);
        } catch (Exception e) {
            logger.error("event=try_setting_cache_state queue_url=" + queueUrl + " shard=" + shard, e);
        } 
    }
    
    /**
     * Update the TTL for the queue
     * @param queueUrl
     * @param seconds
     * @param jedis
     */
    private void updateExpire(String queueUrl, int shard, ShardedJedis jedis) {
        int seconds = CMBProperties.getInstance().getRedisExpireTTLSec();
        long ts1 = System.currentTimeMillis();
        Long ret = jedis.expire(queueUrl + "-" + shard + "-Q", seconds);
        if (ret == 0) {
            logger.debug("event=could_not_update_expire queue_url=" + queueUrl + " shard=" + shard);
        }
        ret = jedis.expire(queueUrl + "-" + shard + "-H", seconds);
        if (ret == 0) {
            logger.debug("event=could_not_update_expire_for_hidden_set queue_url=" + queueUrl + " shard=" + shard);
        }
        ret = jedis.expire(queueUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE, seconds);
        if (ret == 0) {
            throw new IllegalStateException("event=could_not_update_expire_for_state queue_url=" + queueUrl + " shard=" + shard);
        }
        long ts2 = System.currentTimeMillis();
        CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
    }
    
    /**
     * @return the method will always return the mem-id generated by this layer which encodes the id provided by underlying persistence layer
     * Note: method updates the expire time of the queue attributes in cache.
     */
    @Override
    public String sendMessage(CQSQueue queue, int shard, CQSMessage message) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException {
    	// first persist to Cassandra and get message-id to put into cache        
        String messageId = persistenceStorage.sendMessage(queue, shard, message);        
        if (messageId == null) {
        	throw new IllegalStateException("Could not get id from underlying storage");
        }
        boolean cacheAvailable = checkCacheConsistency(queue.getRelativeUrl(), shard, true); //set in cache even if its filling
        String memId = null;
        boolean brokenJedis = false;
        ShardedJedis jedis = null;
        long ts1 = System.currentTimeMillis();
        try {
            if (cacheAvailable) {
            	int delaySeconds = 0;
            	if (queue.getDelaySeconds() > 0) {
            		delaySeconds = queue.getDelaySeconds();
            	}
            	if (message.getAttributes().containsKey(CQSConstants.DELAY_SECONDS)) {
            		delaySeconds = Integer.parseInt(message.getAttributes().get(CQSConstants.DELAY_SECONDS));
            	}
            	memId = getMemQueueMessage(messageId); 
            	jedis = getResource();
            	if (delaySeconds > 0) {
                    jedis.zadd(queue.getRelativeUrl() + "-" + shard + "-V", System.currentTimeMillis() + (delaySeconds * 1000), memId); //insert or update already existing            	    
            	} else {
	                jedis.rpush(queue.getRelativeUrl() + "-" + shard + "-Q", memId);
            	}
                logger.debug("event=send_message cache_available=true msg_id= " + memId + " queue_url=" + queue.getAbsoluteUrl() + " shard=" + shard);
            } else {
                logger.debug("event=send_message cache_available=false msg_id= " + memId + " queue_url=" + queue.getAbsoluteUrl() + " shard=" + shard);
            }
        } catch (JedisConnectionException e) {
            logger.warn("event=send_message error_code=redis_unavailable num_connections=" + numRedisConnections.get());
            brokenJedis = true;
            trySettingCacheState(queue.getRelativeUrl(), shard, QCacheState.Unavailable);
        } finally {
            if (jedis != null) {
                returnResource(jedis, brokenJedis);
            }
            long ts2 = System.currentTimeMillis();
            CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
        }
        return memId;
    }

    @Override
    public Map<String, String> sendMessageBatch(CQSQueue queue, int shard, List<CQSMessage> messages) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException {
        
        persistenceStorage.sendMessageBatch(queue, shard, messages);
        Map<String, String> memIds = new HashMap<String, String>();
        boolean cacheAvailable = checkCacheConsistency(queue.getRelativeUrl(), shard, true);//set in cache even if its filling
        ShardedJedis jedis = null;

        if (cacheAvailable) {
            try {
                jedis = getResource();
            } catch (JedisConnectionException e) {
                cacheAvailable = false;
                trySettingCacheState(queue.getRelativeUrl(), shard, QCacheState.Unavailable);
            }
        }
        // add messages in the same order as messages list
        boolean brokenJedis = false;
        try {
            for (CQSMessage message : messages) {
            	int delaySeconds = 0;
            	if (queue.getDelaySeconds() > 0) {
            		delaySeconds = queue.getDelaySeconds();
            	}
            	if (message.getAttributes().containsKey(CQSConstants.DELAY_SECONDS)) {
            		delaySeconds = Integer.parseInt(message.getAttributes().get(CQSConstants.DELAY_SECONDS));
            	}
                String clientId = message.getSuppliedMessageId();
                String messageId = message.getMessageId();
                String memId = getMemQueueMessage(messageId);
                if (cacheAvailable) {      
                    try {
                    	if (delaySeconds > 0) {
                            jedis.zadd(queue.getRelativeUrl() + "-" + shard + "-V", System.currentTimeMillis() + (delaySeconds * 1000), memId); //insert or update already existing            	    
                    	} else {
                    		jedis.rpush(queue.getRelativeUrl() + "-" + shard + "-Q", memId);
                    	}
                    } catch (JedisConnectionException e) {
                        trySettingCacheState(queue.getRelativeUrl(), shard, QCacheState.Unavailable);
                    }
                    logger.debug("event=send_message_batch cache_available=true msg_id= " + memId + " queue_url=" + queue.getAbsoluteUrl() + " shard=" + shard);
                } else {
                    logger.debug("event=send_message_batch cache_available=false msg_id= " + memId + " queue_url=" + queue.getAbsoluteUrl() + " shard=" + shard);
                }
                memIds.put(clientId, memId);            
            }
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            if (jedis != null) {
                returnResource(jedis, brokenJedis);
            }
        }
        return memIds;        
    }
    
    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) throws PersistenceException {
    	
        String queueUrlHash = Util.getQueueUrlHashFromCache(queueUrl);
    	//receiptHandle is memId
    	
    	String messageId = getMemQueueMessageMessageId(queueUrlHash, receiptHandle);
    	int shard = Util.getShardFromReceiptHandle(receiptHandle);
        boolean cacheAvailable = checkCacheConsistency(queueUrl, shard, false);
        
        if (cacheAvailable) {
            ShardedJedis jedis = null;
            boolean brokenJedis = false;
            try {
                jedis = getResource();
                long ts1 = System.currentTimeMillis();
                long numDeleted = jedis.hdel(queueUrl + "-" + shard + "-H", receiptHandle);
                if (numDeleted != 1) {
                    logger.warn("event=delete_message error_code=could_not_delelete_hidden_set queue_url=" + queueUrl + " shard=" + shard + " mem_id=" + receiptHandle);
                }
                if (jedis.del(queueUrl + "-" + shard + "-A-" + receiptHandle) == 0) {
                    logger.warn("event=delete_message error_code=could_not_delete_attributes queue_url=" + queueUrl + " shard=" + shard + " mem_id=" + receiptHandle);
                }
                long ts2 = System.currentTimeMillis();
                CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
            } catch (JedisConnectionException e) {
                logger.warn("event=delete_message error_code=redis_unavailable num_connections=" + numRedisConnections.get());
                brokenJedis = true;
                cacheAvailable = false;
                trySettingCacheState(queueUrl, shard, QCacheState.Unavailable);
            } finally {
            	if (jedis != null) {
                    returnResource(jedis, brokenJedis);
                }
            }
        }
        
        //delete from underlying persistence layer
        
        persistenceStorage.deleteMessage(queueUrl, messageId);
    }
    
    /**
     * 
     * @param queue
     * @param message
     * @return true if message is expired, false otherwise
     */
    //example of memId is: 0:0:0_0_72:2923737900040323074:-8763141905575923938
    private boolean isMessageExpired(CQSQueue queue, String memId) {
        if (getMemQueueMessageCreatedTS(memId) + (queue.getMsgRetentionPeriod() * 1000) < System.currentTimeMillis()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<CQSMessage> receiveMessage(CQSQueue queue, Map<String, String> receiveAttributes) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException {

        int shard = rand.nextInt(queue.getNumberOfShards());
        int maxNumberOfMessages = 1;
        int visibilityTO = queue.getVisibilityTO();

        if (receiveAttributes != null && receiveAttributes.size() > 0) {
            if (receiveAttributes.containsKey(CQSConstants.MAX_NUMBER_OF_MESSAGES)) {
                maxNumberOfMessages = Integer.parseInt(receiveAttributes.get(CQSConstants.MAX_NUMBER_OF_MESSAGES));
            }
            if (receiveAttributes.containsKey(CQSConstants.VISIBILITY_TIMEOUT)) {
                visibilityTO = Integer.parseInt(receiveAttributes.get(CQSConstants.VISIBILITY_TIMEOUT));
            }
        }
    
        boolean cacheAvailable = checkCacheConsistency(queue.getRelativeUrl(), shard, false);
        List<CQSMessage> ret = new LinkedList<CQSMessage>();
        
        if (cacheAvailable) {    
        	
        	// get the messageIds from in redis list
            
        	ShardedJedis jedis = null;
            boolean brokenJedis = false;
            
            try {
                
            	jedis = getResource();

                // process revisible-set before getting from queue
                
                tryCheckAndProcessRevisibleSet(queue.getRelativeUrl(), shard, CMBProperties.getInstance().getRedisRevisibleSetFrequencySec());

                try {
                    if (checkAndSetVisibilityProcessing(queue.getRelativeUrl(), shard, CMBProperties.getInstance().getRedisRevisibleFrequencySec())) {                                                
                        logger.debug("event=scheduled_revisibility_processor");
                    }
                } catch (SetFailedException e) {
                }

                boolean done = false;
                
                while (!done) {

                	boolean emptyQueue = false;
                    HashMap<String, String> messageIdToMemId = new HashMap<String, String>();
                    List<String> messageIds = new LinkedList<String>();
                    
                    for (int i = 0; i < maxNumberOfMessages && !emptyQueue; i++) {
                    
                    	long ts1 = System.currentTimeMillis();
                        String memId;
                        
                        if (visibilityTO > 0) {
                            memId = jedis.lpop(queue.getRelativeUrl() + "-" + shard + "-Q");
                        } else {
                            memId = jedis.lindex(queue.getRelativeUrl() + "-" + shard + "-Q", i);
                        }
                        
                        long ts2 = System.currentTimeMillis();
                        CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
                        
                        if (memId == null || memId.equals("nil")) { //done
                            emptyQueue = true;
                        } else {
                            String messageId = getMemQueueMessageMessageId(queue.getRelativeUrlHash(),memId); 
                            messageIds.add(messageId);
                            messageIdToMemId.put(messageId, memId);
                        }
                    }
                    
                    if (messageIds.size() == 0) {
                        CQSMonitor.getInstance().registerEmptyResp(queue.getRelativeUrl(), 1);
                        return Collections.emptyList();
                    }
                    
                    // By here messageIds have Underlying layer's messageids.
                    // Get messages from underlying layer. The total returned may not be what was
                    // in mem cache since master-master replication to Cassandra could mean other
                    // colo deleted the message form underlying storage. 

                    logger.debug("event=found_msg_ids_in_redis num_mem_ids=" + messageIds.size());
                    
                    try {
                        
                    	Map<String, CQSMessage> persisMap = persistenceStorage.getMessages(queue.getRelativeUrl(), messageIds);
                        
                    	for (Entry<String, CQSMessage> messageIdToMessage : persisMap.entrySet()) {
                        
                    		String memId = messageIdToMemId.get(messageIdToMessage.getKey());
                            
                    		if (memId == null) {
                            	throw new IllegalStateException("Underlying storage layer returned a message that was not requested");
                            }
                            
                    		CQSMessage message = messageIdToMessage.getValue();
                            
                    		if (message == null) {
                            	logger.warn("event=message_is_null msg_id=" + messageIdToMessage.getKey());
                            	continue;
                            }
                            
                            //check if message returned is expired. This will only happen if payload coming from payload-cache. Cassandra
                            //expires old messages just fine. If expired, skip over and continue. 
                            
                    		if (isMessageExpired(queue, memId)) {
                            
                    			try {
                                    logger.debug("event=message_expired message_id=" + memId);
                                	persistenceStorage.deleteMessage(queue.getRelativeUrl(), message.getMessageId());
                                } catch (Exception e) {
                                    //its fine to ignore this exception since message must have been auto-deleted from Cassandra
                                    //but we need to do this to clean up payloadcache
                                    logger.debug("event=message_already_deleted_in_cassandra msg_id=" + message.getMessageId());
                                }
                                
                    			continue;
                            }

                            //hide message if visibilityTO is greater than 0
                            
                    		if (visibilityTO > 0) {
                                long ts1 = System.currentTimeMillis();
                                jedis.hset(queue.getRelativeUrl() + "-" + shard + "-H", memId, Long.toString(System.currentTimeMillis() + (visibilityTO * 1000)));
                                long ts2 = System.currentTimeMillis();
                                CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
                            }

                            message.setMessageId(memId);
                            message.setReceiptHandle(memId);

                            //get message-attributes and populate in message
                            Map<String, String> msgAttrs = (message.getAttributes() != null) ?  message.getAttributes() : new HashMap<String, String>();
                            List<String> attrs = jedis.hmget(queue.getRelativeUrl() + "-" + shard + "-A-" + memId, CQSConstants.REDIS_APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, CQSConstants.REDIS_APPROXIMATE_RECEIVE_COUNT);
                            
                            if (attrs.get(0) == null) {
                                String firstRecvTS = Long.toString(System.currentTimeMillis());
                                jedis.hset(queue.getRelativeUrl() + "-" + shard + "-A-" + memId, CQSConstants.REDIS_APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, firstRecvTS);
                                msgAttrs.put(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, firstRecvTS);
                            } else {                            
                                msgAttrs.put(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, attrs.get(0));
                            }
                            
                            int recvCount = 1;
                            
                            if (attrs.get(1) != null) {
                                recvCount = Integer.parseInt(attrs.get(1)) + 1;
                            }
                            
                            jedis.hset(queue.getRelativeUrl() + "-" + shard + "-A-" + memId, CQSConstants.REDIS_APPROXIMATE_RECEIVE_COUNT, Integer.toString(recvCount));
                            jedis.expire(queue.getRelativeUrl() + "-" + shard +  "-A-" + memId, 3600 * 24 * 14); //14 days expiration if not deleted
                            msgAttrs.put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, Integer.toString(recvCount));
                            message.setAttributes(msgAttrs);
                            ret.add(message);
                        }
                        
                        if (ret.size() > 0) { //There may be cases where the underlying persistent message has two memIds while
                            //cache filling. In such cases trying to retrieve message for the second memId may result in no message
                            //returned. We should skip over those memIds and continue till we find at least one valid memId
                            done = true;
                        } else {
                            for (String messageId : messageIds) {
                                logger.debug("event=bad_mem_id_found msg_id=" + messageId + " action=skip_message");
                            }
                        }
                        
                    } catch (HTimedOutException e1) { //If Hector timedout, push messages back
                        logger.error("event=hector_timeout num_messages=" + messageIds.size() + " action=pushing_messages_back_to_redis");
                        if (visibilityTO > 0) {
                            for (String messageId : messageIds) {
                                String memId = messageIdToMemId.get(messageId);
                                jedis.lpush(queue.getRelativeUrl() + "-" + shard + "-Q", memId);
                            }
                        }
                        throw e1;
                    }
                } 
            } catch (JedisConnectionException e) {
                logger.warn("event=receive_message error_code=redis_unavailable num_connections=" + numRedisConnections.get());
                brokenJedis = true;
                trySettingCacheState(queue.getRelativeUrl(), shard, QCacheState.Unavailable);            
                cacheAvailable = false;
            } finally {
                if (jedis != null) {
                    returnResource(jedis, brokenJedis);
                }
            }
            
            CQSMonitor.getInstance().registerCacheHit(queue.getRelativeUrl(), ret.size(), ret.size(), CacheType.QCache); //all ids from cache
            logger.debug("event=messages_found cache=available num_messages=" + ret.size());
        
        } else { //get form underlying layer
        
        	List<CQSMessage> messages = persistenceStorage.peekQueueRandom(queue.getRelativeUrl(), shard, maxNumberOfMessages);
            
        	for (CQSMessage msg : messages) {
                String memId = getMemQueueMessage(msg.getMessageId()); //TODO: initialDelay is 0
                msg.setMessageId(memId);
                msg.setReceiptHandle(memId);
                Map<String, String> msgAttrs = (msg.getAttributes() != null) ?  msg.getAttributes() : new HashMap<String, String>();
                msgAttrs.put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, "1");
                msgAttrs.put(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, Long.toString(System.currentTimeMillis()));
                msg.setAttributes(msgAttrs);
                ret.add(msg);
            }
            // in this case there is no message hiding          
            CQSMonitor.getInstance().registerCacheHit(queue.getRelativeUrl(), 0, ret.size(), CacheType.QCache); //all ids missed cache
            logger.debug("event=messages_found cache=unavailable num_messages=" + ret.size());
        }
        
        if (ret.size() == 0) {
            CQSMonitor.getInstance().registerEmptyResp(queue.getRelativeUrl(), 1);
        }
        
        return ret;
    }

    /**
     * Only hidden messages can have their visibility changed. Method updates timestamp for the memId in hidden hashtable.
     * If the new visibilityTO is 0 we shortcut and make the message immediately visible.
     * @return true if visibility was changed, false otherwise
     */
    @Override
    public boolean changeMessageVisibility(CQSQueue queue, String receiptHandle, int visibilityTO) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException {
        
    	int shard = Util.getShardFromReceiptHandle(receiptHandle);
    	boolean cacheAvailable = checkCacheConsistency(queue.getRelativeUrl(), shard, false);

    	if (cacheAvailable) {
    		
            ShardedJedis jedis = null;
            boolean brokenJedis = false;
            
            try {
            	
                jedis = getResource();
                long ts1 = System.currentTimeMillis();
                if (visibilityTO == 0) { //make immediately visible
                    jedis.rpush(queue.getRelativeUrl() + "-" + shard + "-Q", receiptHandle);
               		jedis.hdel(queue.getRelativeUrl() + "-" + shard + "-H", receiptHandle);
                    return true;
                } else { //update new visibilityTO
            	    jedis.hset(queue.getRelativeUrl() + "-" + shard + "-H", receiptHandle, Long.toString(System.currentTimeMillis() + (visibilityTO * 1000)));                	
                }
                
                long ts2 = System.currentTimeMillis();
                CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
                return true; 
                
            } catch (JedisConnectionException e) {
                logger.warn("event=change_message_visibility reason=redis_unavailable num_connections=" + numRedisConnections.get());
                brokenJedis = true;
                cacheAvailable = false;
                return false;
            } finally {
                if (jedis != null) {
                    returnResource(jedis, brokenJedis);
                }
            }
            
        } else {
            return false;
        }
    }

    /**
     * Note: If cache is unavailable, we will return different id for a message than when the cache is available,
     * so we will have duplicates in that case. Also, we currently don't respect nextReceiptHandle rather only previousReceiptHandle and length.
     */
    @Override
    public List<CQSMessage> peekQueue(String queueUrl, int shard, String previousReceiptHandle, String nextReceiptHandle, int length) throws PersistenceException, IOException, NoSuchAlgorithmException {
        
    	boolean cacheAvailable = checkCacheConsistency(queueUrl, shard, false);
        
    	if (cacheAvailable) {
        
    		List<String> memIdsRet = getIdsFromHead(queueUrl, shard, previousReceiptHandle, length);                
            // by here memIdsRet should have memIds to return messages for
            Map<String, CQSMessage> ret = getMessages(queueUrl, memIdsRet);
            // return list in the same order as memIdsRet
            List<CQSMessage> messages = new LinkedList<CQSMessage>();
            ShardedJedis jedis = null;
            boolean brokenJedis = false;
            
            try {
            
            	jedis = getResource();
	            
            	for (String memId : memIdsRet) {
	            
            		CQSMessage message = ret.get(memId);
	                
            		if (message != null) {
            			
	                	// get message-attributes and populate in message
            			
	                    Map<String, String> msgAttrs = (message.getAttributes() != null) ?  message.getAttributes() : new HashMap<String, String>();
	                    List<String> attrs = jedis.hmget(queueUrl + "-" + shard + "-A-" + memId, CQSConstants.REDIS_APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, CQSConstants.REDIS_APPROXIMATE_RECEIVE_COUNT);
	                    
	                    if (attrs.get(0) != null) {
	                        msgAttrs.put(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, attrs.get(0));
	                    }
	                    
	                    if (attrs.get(1) != null) {
		                    msgAttrs.put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, Integer.toString(Integer.parseInt(attrs.get(1))));
	                    }
	                    
	                    message.setAttributes(msgAttrs);
	                    messages.add(message);
	                }
	            }
            } catch (JedisConnectionException e) {
                logger.warn("event=peek_message error_code=redis_unavailable num_connections=" + numRedisConnections.get());
                brokenJedis = true;
                cacheAvailable = false;
            } finally {
                if (jedis != null) {
                    returnResource(jedis, brokenJedis);
                }
            }
            
            logger.debug("event=peek_queue cache=available queue_url=" + queueUrl + " shard=" + shard + " num_messages=" + messages.size());
            return messages;
        
    	} else { //get from underlying storage
    		String relativeUrlHash = Util.getQueueUrlHashFromCache(queueUrl);
    		if (previousReceiptHandle != null) { //strip out this layer's id
                previousReceiptHandle = getMemQueueMessageMessageId(relativeUrlHash, previousReceiptHandle);
            }
            
    		if (nextReceiptHandle != null) {
                nextReceiptHandle = getMemQueueMessageMessageId(relativeUrlHash, nextReceiptHandle);
            }        
            
    		List<CQSMessage> messages = persistenceStorage.peekQueue(queueUrl, shard, previousReceiptHandle, nextReceiptHandle, length);
            
    		for (CQSMessage message : messages) {
                String memId = getMemQueueMessage(message.getMessageId()); //TODO: initialDelay is 0
                message.setMessageId(memId);
                message.setReceiptHandle(memId);
            }
    		
            logger.debug("event=peek_queue cache=unavailable queue_url=" + queueUrl + " shard=" + shard + " num_messages=" + messages.size());
            return messages;
        }
    }

    @Override
    public void clearQueue(String queueUrl, int shard) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
        
    	boolean cacheAvailable = checkCacheConsistency(queueUrl, shard, true);
        
    	if (cacheAvailable) {
            
    		boolean brokenJedis = false;
            ShardedJedis jedis = null;
            
            try {
                long ts1 = System.currentTimeMillis();
                jedis = getResource();
                Long num = jedis.del(queueUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE);
                logger.debug("num removed=" + num);
                num = jedis.del(queueUrl + "-" + shard + "-Q");
                logger.debug("num removed=" + num);
                num = jedis.del(queueUrl + "-" + shard + "-H");
                logger.debug("num removed=" + num);
                num = jedis.del(queueUrl + "-" + shard + "-R");
                logger.debug("num removed=" + num);
                num = jedis.del(queueUrl + "-" + shard + "-F");
                logger.debug("num removed=" + num);
                num = jedis.del(queueUrl + "-" + shard + "-V");
                logger.debug("num removed=" + num);
                num = jedis.del(queueUrl + "-" + shard + "-VR");
                logger.debug("num removed=" + num);
                long ts2 = System.currentTimeMillis();
                CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
                logger.debug("event=cleared_queue queue_url=" + queueUrl + " shard=" + shard);
            } catch (JedisConnectionException e) {
                logger.warn("event=clear_queue error_code=redis_unavailable num_connections=" + numRedisConnections.get());
                brokenJedis = true;
                trySettingCacheState(queueUrl, shard, QCacheState.Unavailable);
                cacheAvailable = false;
            } finally {
                if (jedis != null) {
                    returnResource(jedis, brokenJedis);
                }
            }
        }
    	
        //clear queue from underlying layer
    	
        persistenceStorage.clearQueue(queueUrl, shard);        
    }
    
    @Override
    public Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids) throws PersistenceException, NoSuchAlgorithmException, IOException {
        
    	//parse the ids generated by this layer to get underlying ids and get from underlying layer
        
    	List<String> messageIds = new LinkedList<String>();
        HashMap<String, String> messageIdToMemId = new HashMap<String, String>();
        String queueUrlHash = Util.getQueueUrlHashFromCache(queueUrl);
        for (String id : ids) {
            String messageId = getMemQueueMessageMessageId(queueUrlHash,id);
            messageIds.add(messageId);
            messageIdToMemId.put(messageId, id);
        }
        
        Map<String, CQSMessage> persisMap = persistenceStorage.getMessages(queueUrl, messageIds);
        Map<String, CQSMessage> ret = new HashMap<String, CQSMessage>();
        
        for (Entry<String, CQSMessage> idToMessage : persisMap.entrySet()) {
            CQSMessage msg = idToMessage.getValue();
            if (msg == null) {
            	continue;
            }
            String memId = messageIdToMemId.get(idToMessage.getKey());
            msg.setMessageId(memId);
            msg.setReceiptHandle(memId);
            ret.put(memId, msg);
        }
        
        return ret;
    }
    
    @Override
    public List<String> getIdsFromHead(String queueUrl, int shard, int num) throws PersistenceException {
        return getIdsFromHead(queueUrl, shard, null, num);
    }
    
    public List<String> getIdsFromHead(String queueUrl, int shard, String previousReceiptHandle, int num) throws PersistenceException {

    	if (previousReceiptHandle != null && Util.getShardFromReceiptHandle(previousReceiptHandle) != shard) {
    		logger.warn("event=inconsistent_shard_information shard=" + shard + " receipt_handle=" + previousReceiptHandle);
    	}
    	
    	boolean cacheAvailable = checkCacheConsistency(queueUrl, shard, true);
        
    	if (!cacheAvailable) {
        	return Collections.emptyList();
        }
        
        List<String> memIdsRet = new LinkedList<String>();
        boolean brokenJedis = false;
        ShardedJedis jedis = null;
        
        try {
        
        	jedis = getResource();
            
            // process re-visible set before getting from queue
            
            tryCheckAndProcessRevisibleSet(queueUrl, shard, CMBProperties.getInstance().getRedisRevisibleSetFrequencySec());

            try {
                if (checkAndSetVisibilityProcessing(queueUrl, shard, CMBProperties.getInstance().getRedisRevisibleFrequencySec())) {                                                
                    logger.debug("event=scheduled_revisibility_processor");
                }
            } catch (SetFailedException e) {
            }

            long llen = jedis.llen(queueUrl + "-" + shard + "-Q");
            
            if (llen == 0L) {
                return Collections.emptyList();
            }
            
            int retCount = 0;
            long i = 0L;
            boolean includeSet = (previousReceiptHandle == null) ? true : false;
            
            while (retCount < num && i < llen) {
            
            	List<String> memIds = jedis.lrange(queueUrl + "-" + shard + "-Q", i, i + num - 1);
                
            	if (memIds.size() == 0) {
                	break; // done
                }
                
            	i += num; // next time, exclude the last one in this set
                
            	for (String memId : memIds) {
                    if (!includeSet) {
                        if (memId.equals(previousReceiptHandle)) {
                            includeSet = true;
                            //skip over this one since it was the last el of the last call
                        }
                    } else {
                        memIdsRet.add(memId);
                        retCount++;
                    }
                }
            }
            
            // by here memIdsRet should have memIds to return messages for
            
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            if (jedis != null) {
                returnResource(jedis, brokenJedis);
            }
        }
        
        return memIdsRet;
    }
    
    /**
     * 
     * @param queueUrl
     * @param processRevisibilitySet if true, run re-visibility processing.
     * @return number of mem-ids in Redis Queue
     * @throws Exception 
     */
    public long getQueueMessageCount(String queueUrl, boolean processHiddenIds) throws Exception  {
    	
    	long messageCount = 0;
    	CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
    	int numberOfShards = 1;
    	
    	if (queue != null) {
    		numberOfShards = queue.getNumberOfShards();
    	}

		ShardedJedis jedis = null;
        boolean brokenJedis = false;
        
        try {

        	jedis = getResource();
            
        	for (int shard=0; shard<numberOfShards; shard++) {
            
        		boolean cacheAvailable = checkCacheConsistency(queueUrl, shard, true);
                
                if (!cacheAvailable) {
                	throw new IllegalStateException("Redis cache not available");
                }
                
                if (processHiddenIds) {
                	checkAndSetVisibilityProcessing(queueUrl, shard, CMBProperties.getInstance().getRedisRevisibleFrequencySec());
                    tryCheckAndProcessRevisibleSet(queueUrl, shard, CMBProperties.getInstance().getRedisRevisibleSetFrequencySec());
                }
            	
                messageCount += jedis.llen(queueUrl + "-" + shard + "-Q");
            }
        	
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            if (jedis != null) {
                returnResource(jedis, brokenJedis);
            }
        }  
        
        return messageCount;
    }
    
    /**
     * 
     * @param queueUrl
     * @return number of mem-ids in Redis Queue. If Redis queue is empty, do not load from Cassandra.
     * @throws Exception 
     */
    public long getRedisQueueMessageCount(String queueUrl) throws Exception  {
    	
    	long messageCount = 0;
    	CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
    	int numberOfShards = 1;
    	
    	if (queue != null) {
    		numberOfShards = queue.getNumberOfShards();
    	}

		ShardedJedis jedis = null;
        boolean brokenJedis = false;
        
        try {

        	jedis = getResource();
            
        	for (int shard=0; shard<numberOfShards; shard++) {         	
                messageCount += jedis.llen(queueUrl + "-" + shard + "-Q");
            }
        	
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            if (jedis != null) {
                returnResource(jedis, brokenJedis);
            }
        }  
        
        return messageCount;
    }
    
    @Override
    public long getQueueMessageCount(String queueUrl) {
        
    	long messageCount = 0;
        
    	try {
        	messageCount = getQueueMessageCount(queueUrl, false);
        } catch (Exception ex) {
    		logger.error("event=failed_to_get_number_of_messages queue_url=" + queueUrl);
        }
        
    	return messageCount;
    }

    /**
     * This method get the count from redis based ont he suffix
     * @param queueUrl
     * @param processFlag if true, run visibility or delay clean processing.
     * @return number of mem-ids in Redis Queue
     * @throws Exception 
     */
    private long getCount(String queueUrl, String suffix, boolean processHiddenIds) throws Exception  {
    	
    	long messageCount = 0;
    	CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
    	int numberOfShards = 1;
    	
    	if (queue != null) {
    		numberOfShards = queue.getNumberOfShards();
    	}

		ShardedJedis jedis = null;
        boolean brokenJedis = false;
        
        try {

        	jedis = getResource();
            
        	for (int shard=0; shard<numberOfShards; shard++) {
            
        		boolean cacheAvailable = checkCacheConsistency(queueUrl, shard, true);
                
                if (!cacheAvailable) {
                	throw new IllegalStateException("Redis cache not available");
                }
                //if check for hidden message, and processFlag is true, move the message from H to Q
                if(suffix.equals("-H")&&(processHiddenIds)){
                	checkAndSetVisibilityProcessing(queueUrl, shard, CMBProperties.getInstance().getRedisRevisibleFrequencySec());                                                 
                }
                //if check for delayed message, and processFlag is true, move the message from V to Q                	
                if (suffix.equals("-V")&&(processHiddenIds)) {
                    tryCheckAndProcessRevisibleSet(queueUrl, shard, CMBProperties.getInstance().getRedisRevisibleSetFrequencySec());
                }
            	
                //get the count number from
                if(suffix.equals("-H")){
                	messageCount += jedis.hlen(queueUrl + "-" + shard + suffix);
                } else if (suffix.equals("-V")){
                	messageCount += jedis.zcard(queueUrl + "-" + shard + suffix);
                }
            }
        	
        } catch (JedisException e) {
            brokenJedis = true;
            throw e;
        } finally {
            if (jedis != null) {
                returnResource(jedis, brokenJedis);
            }
        }  
        
        return messageCount;
    }
    
    
    /**
     * 
     * @param queueUrl
     * @param visibilityProcessFlag if true, run visibility processing.
     * @return number of mem-ids in Redis list
     * @throws Exception 
     */
    public long getQueueNotVisibleMessageCount(String queueUrl, boolean visibilityProcessFlag) throws Exception  {
    	
    	return getCount(queueUrl, "-H", visibilityProcessFlag);
    }

    public long getQueueNotVisibleMessageCount(String queueUrl) {
        
    	long messageCount = 0;
        
    	try {
        	messageCount = getQueueNotVisibleMessageCount(queueUrl, false);
        } catch (Exception ex) {
    		logger.error("event=failed_to_get_number_of_not_visible_messages queue_url=" + queueUrl);
        }
        
    	return messageCount;
    }

    /**
     * 
     * @param queueUrl
     * @param visibilityProcessFlag if true, run visibility processing.
     * @return number of mem-ids in Redis set for delayed messages
     * @throws Exception 
     */
    public long getQueueDelayedMessageCount(String queueUrl, boolean visibilityProcessFlag) throws Exception  {
    	
    	return getCount(queueUrl, "-V", visibilityProcessFlag);
    }
    
    public long getQueueDelayedMessageCount(String queueUrl) {
        
    	long messageCount = 0;
        
    	try {
        	messageCount = getQueueDelayedMessageCount(queueUrl, false);
        } catch (Exception ex) {
    		logger.error("event=failed_to_get_number_of_delayed_messages queue_url=" + queueUrl);
        }
        
    	return messageCount;
    }
    
    @Override
    public List<CQSMessage> peekQueueRandom(String queueUrl, int shard, int length) throws PersistenceException, IOException, NoSuchAlgorithmException {
        return persistenceStorage.peekQueueRandom(queueUrl, shard, length);
    }
    
	/**
	 * Get all redis shard infos
	 * @return list of hash maps, one for each shard
	 */
	public static List<Map<String, String>> getInfo() {
	    
		boolean brokenJedis = false;
	    ShardedJedis jedis = getResource();
	    List<Map<String, String>> shardInfos = new ArrayList<Map<String, String>>();
	    
	    try {
	    
	    	Collection<Jedis> shards = jedis.getAllShards();
	
	        for (Jedis shard : shards) {
	    		
	        	String info = shard.info();
	    		String lines[] = info.split("\n");
	    		Map<String, String> entries = new HashMap<String, String>();
	    		
	    		for (String line : lines) {
	    			
	    			if (line != null && line.length() > 0) {
	        			
	    				String keyValue[] = line.split(":");
	        			
	    				if (keyValue.length == 2) {
	        				entries.put(keyValue[0], keyValue[1]);
	        			}
	    			}
	    			
	    		}
	    		
	    		shardInfos.add(entries);
	    	}
	        
	        return shardInfos;
	
	    } catch (JedisException ex) {
	    
	    	brokenJedis = true;
	        throw ex;
	    
	    } finally {
	        returnResource(jedis, brokenJedis);
	    }
	}
	
	/**
	 * Get number of redis shards
	 * @return number of redis shards
	 */
	public static int getNumberOfRedisShards() {
	    
		boolean brokenJedis = false;
	    ShardedJedis jedis = getResource();
	    
	    try {
	    	return jedis.getAllShards().size();
	    } catch (JedisException ex) {
	    	brokenJedis = true;
	        throw ex;
	    } finally {
	        returnResource(jedis, brokenJedis);
	    }
	}
	
	/**
	 * Clear cache across all shards. Useful for data center fail-over scenarios.
	 */
	public static void flushAll() {
	    
		boolean brokenJedis = false;
	    ShardedJedis jedis = getResource();
	    
	    try {
	    
	    	Collection<Jedis> shards = jedis.getAllShards();
	    	
	    	for (Jedis shard : shards) {
	    		shard.flushAll();
	    	}
	    	
	    } catch (JedisException ex) {
	    
	    	brokenJedis = true;
	        throw ex;
	    
	    } finally {
	        returnResource(jedis, brokenJedis);
	    }
	}
	
	public static boolean isAlive() {

    	boolean atLeastOneShardIsUp = false;
		boolean brokenJedis = false;
		ShardedJedis jedis = getResource();
		
		try {

			Collection<Jedis> shards = jedis.getAllShards();
	    	
	    	for (Jedis shard : shards) {
	    	    try {
		    		shard.set("test", "test");
		    		atLeastOneShardIsUp |= shard.get("test").equals("test");
	    	    } catch (JedisException ex) {
	    	    	brokenJedis = true;
	    	    }	    	    
	    	}
    	
		} catch (Exception ex) {
	    	brokenJedis = true;
	    } finally {
	        returnResource(jedis, brokenJedis);
	    }
    	
    	return atLeastOneShardIsUp;
	}
}
