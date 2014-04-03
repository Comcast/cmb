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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.json.JSONException;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;

public class RedisSortedSetPersistence implements ICQSMessagePersistence, ICQSMessagePersistenceIdSequence {
	
    private static final Logger logger = Logger.getLogger(RedisSortedSetPersistence.class);
    private static final Random rand = new Random();
    
    private static RedisSortedSetPersistence instance;
    
    public static ExecutorService executor;
    
    private static JedisPoolConfig config = new JedisPoolConfig();
    private static ShardedJedisPool pool;
    static {
        initializeInstance();
        initializePool();
    }
    
    private static void initializeInstance() {
        CQSMessagePartitionedCassandraPersistence cassandraPersistence = new CQSMessagePartitionedCassandraPersistence();
        instance = new RedisSortedSetPersistence(cassandraPersistence);            
    }
    
    private volatile ICQSMessagePersistence payloadPersistence; 
    
    private RedisSortedSetPersistence(ICQSMessagePersistence persistenceStorage) {
        this.payloadPersistence = persistenceStorage;
    }
    /**
     * Initialize the Redis connection pool
     */
    private static void initializePool() {
    	
        config.maxActive = CMBProperties.getInstance().getRedisConnectionsMaxActive();
        config.maxIdle = -1;
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
        
        pool = new ShardedJedisPool(config, shardInfos);
        executor = Executors.newFixedThreadPool(CMBProperties.getInstance().getRedisFillerThreads());
        
        logger.info("event=initialize_redis pools_size=" + shardInfos.size() + " max_active=" + config.maxActive + " server_list=" + serverList);
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

    //TODO: cache filler
	
	//TODO: cache consistency check

	@Override
	public List<String> getIdsFromHead(String queueUrl, int shard, int num)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getQueueMessageCount(String queueUrl) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String sendMessage(CQSQueue queue, int shard, CQSMessage message)
			throws PersistenceException, IOException, InterruptedException,
			NoSuchAlgorithmException, JSONException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> sendMessageBatch(CQSQueue queue, int shard,
			List<CQSMessage> messages) throws PersistenceException,
			IOException, InterruptedException, NoSuchAlgorithmException,
			JSONException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteMessage(String queueUrl, String receiptHandle)
			throws PersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<CQSMessage> receiveMessage(CQSQueue queue,
			Map<String, String> receiveAttributes) throws PersistenceException,
			IOException, NoSuchAlgorithmException, InterruptedException,
			JSONException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean changeMessageVisibility(CQSQueue queue,
			String receiptHandle, int visibilityTO)
			throws PersistenceException, IOException, NoSuchAlgorithmException,
			InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<CQSMessage> peekQueue(String queueUrl, int shard,
			String previousReceiptHandle, String nextReceiptHandle, int length)
			throws PersistenceException, IOException, NoSuchAlgorithmException,
			JSONException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<CQSMessage> peekQueueRandom(String queueUrl, int shard,
			int length) throws PersistenceException, IOException,
			NoSuchAlgorithmException, JSONException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearQueue(String queueUrl, int shard)
			throws PersistenceException, NoSuchAlgorithmException,
			UnsupportedEncodingException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids)
			throws PersistenceException, NoSuchAlgorithmException,
			UnsupportedEncodingException, IOException, JSONException {
		// TODO Auto-generated method stub
		return null;
	}
}
