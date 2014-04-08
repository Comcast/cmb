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
package com.comcast.cqs.controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provide interface for CQS Monitoring
 * @author aseem
 *
 */
public interface CQSMonitorMBean {
    
    //---- The following are all in a rolling window
    /**
     * @param queueUrl the Queue Url
     * @return the number of messages returned by CQS as a result of the ReceiveMessage() call in a rolling window
     */
    public int getRecentNumberOfReceives(String queueUrl);

    /**
     * 
     * @param queueUrl
     * @return the number of messages received by CQS by the SendMessage() call in a rolling window
     */
    public int getRecentNumberOfSends(String queueUrl);

    /**
     * @param queueUrl
     * @return approximate number of messages in a queue in Redis
     */
    public int getQueueDepth(String queueUrl);
    /**
     * 
     * @param queueUrl
     * @return number of messages deleted in a rolling window
     */
    public int getNumberOfMessagesDeleted(String queueUrl);
    
    /**
     * 
     * @param queueUrl
     * @return number of empty responses in rolling-window
     */
    public int getRecentNumberOfEmptyReceives(String queueUrl);

    //----------End of rolling window metrics
    
    /**
     * @return number of open connections to redis
     */
    public int getNumberOpenRedisConnections();
    
    /**
     * 
     * @param queueUrl
     * @return The timestamp in milliseconds of the oldest message in queue or null if none exists
     */
    public Long getOldestAvailableMessageTS(String queueUrl);
    
	/**
	 * 
	 * @return Number of all long poll receives (active and dead) across all queues
	 */
    public long getNumberOfLongPollReceives();

    /**
	 * 
	 * @return Number of actively pending long poll receives across all queues
	 */
    public long getNumberOfActivelyPendingLongPollReceives();
    
    /**
     * 
     * @return Number of outdated but not yet cleaned up long poll receives across all queues
     */
    public long getNumberOfDeadLongPollReceives();
    
    /**
     * 
     * @param queueArn
     * @return Number of all long poll receives (active and dead) per queue
     */
    public long getNumberOfLongPollReceivesForQueue(String queueArn);
    
    /**
     * 
     * @return Number of redis shards
     */
    public int getNumberOfRedisShards();
    
    /**
     * 
     * @return Composite info string of all redis shards
     */
    public List<Map<String, String>> getRedisShardInfos();
    
    /**
     * Clear entire redis cache
     */
    public void flushRedis();
    
    /**
     * Get call stats for successful api calls
     * @return
     */
    public Map<String, AtomicLong> getCallStats();
    
    /**
     * Get call stats for failed api calls
     * @return
     */
    public Map<String, AtomicLong> getCallFailureStats();
    
    /**
     * Reset all in memory call stats
     */
    public void resetCallStats();
    
    /**
     * Get Cassandra cluster name (should be same for all api servers)
     */
    public String getCassandraClusterName();
    
    /**
     * Get Cassandra nodes this api server is aware of
     */
    public String getCassandraNodes();
    
    /**
     * 
     * @return
     */
    public int getAsyncWorkerPoolSize();
    
    /**
     * 
     * @return
     */
    public int getAsyncWorkerPoolQueueSize();
    
    /**
     * 
     * @return
     */
    public int getJettyCQSRequestHandlerPoolSize();

    /**
     * 
     * @return
     */
	public boolean isJettyCQSRequestHandlerPoolLowOnThreads();

	/**
	 * 
	 * @return
	 */
	public int getAsyncWorkerPoolActiveCount();

	/**
	 * 
	 * @return
	 */
	public int getJettyCNSRequestHandlerPoolSize();

	/**
	 * 
	 * @return
	 */
	boolean isJettyCNSRequestHandlerPoolLowOnThreads();
}
