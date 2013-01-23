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
    public int getNumberOfMessagesReturned(String queueUrl);

    /**
     * 
     * @param queueUrl
     * @return the number of messages received by CQS by the SendMessage() call in a rolling window
     */
    public int getNumberOfMessagesReceived(String queueUrl);
        
    /**
     * Get the In Mem cache hit rate in a rolling-window
     */
    public int getReceiveMessageCacheHitPercent(String queueUrl);
    
    /**
     * Get the payload cache hit rate in a rolling-window
     */
    public int getGetMessagePayloadCacheHitPercent(String queueUrl);
    
    /**s
     * @param queueUrl
     * @return approximate number of messages in a queue in a rolling window
     */
    public int getNumberOfMessagesInQueue(String queueUrl);
    
    /**
     * 
     * @param queueUrl
     * @return number of messages deleted in a rolling window
     */
    public int getNumberOfMessagesDeleted(String queueUrl);
    
    /**
     * This is exactly the number of messages returned in the window
     */
    public int getNumberOfMessagesMarkedInvisible(String queueUrl);
    
    /**
     * 
     * @param queueUrl
     * @return number of empty responses in rolling-window
     */
    public int getNumEmptyResponses(String queueUrl);

    //----------End of rolling window metrics
    
    /**
     * @return number of open connections to redis
     */
    public int getNumberOpenRedisConnections();
    
    /**
     * Number of messages in queue
     * @param queueUrl
     * @return
     */
    public int getNumberOfMessages(String queueUrl);

    /**
     * 
     * @param queueUrl
     * @return The timestamp in milliseconds of the oldest message in queue or null if none exists
     */
    public Long getOldestMessageCreatedTSMS(String queueUrl);
    
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
}
