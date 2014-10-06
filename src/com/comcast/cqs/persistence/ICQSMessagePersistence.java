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
import java.util.List;
import java.util.Map;

import org.json.JSONException;

import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;

/**
 * Interface to persist cqs message
 * @author baosen, aseem, vvenkatraman, bwolf
 */
public interface ICQSMessagePersistence {
	
	/**
	 * Redis
	 * @param memId
	 * @return
	 */
	public long getMemQueueMessageCreatedTS(String memId);
	
	/**
	 * Redis
	 * @return
	 */
	 public List<Map<String, String>> getInfo();
	
	/**
	 * Redis
	 * @return
	 */
	public int getNumberOfRedisShards();
	
	/**
	 * Redis
	 */
	 public void shutdown();
	
	/**
	 * Redis
	 */
	public void flushAll();
	
	/**
	 * Redis
	 * @return
	 */
	public boolean isAlive();
	
    /**
     * Create a message on a specific shard and a random partition.
     * @param queue The CQS queue to post the message
     * @param shard The shard to be used
     * @param message An instance of CQSMessage
     * @throws PersistenceException
     * @throws IOException 
     * @throws InterruptedException 
     * @return the message-=id
     * @throws NoSuchAlgorithmException 
     * @throws JSONException 
     */
    public String sendMessage(CQSQueue queue, int shard, CQSMessage message) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException, JSONException;

    /**
     * Create a batch of messages on a specific shard and a random partition.
     * Note: THe provided messages are modified with a new messageId
     * @param queue The CQS queue to post the messages
     * @param shard The shard to be used
     * @param messages A list of CQSMessage instances
     * @throws PersistenceException
     * @throws IOException 
     * @throws InterruptedException
     * @return mapping from client-provided-message-id to internal message-id 
     * @throws NoSuchAlgorithmException 
     * @throws JSONException 
     */
    public Map<String, String> sendMessageBatch(CQSQueue queue, int shard, List<CQSMessage> messages) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException, JSONException;

    /**
     * Delete a message given the receiptHandle
     * @param receiptHandle The receipt handle for the message. This is the time based UUID. Receipt handle also includes the shard number.
     * @throws PersistenceException
     */
    public void deleteMessage(String queueUrl, String receiptHandle) throws PersistenceException;
    
    /**
     * Receive the next set of messages from the Queue. Receives from random shard and random partition.
     * @param queue The queue which contains the messages.
     * @param receiveAttributes The set of attributes for the message, like the new visibility timeout and others.
     * @throws PersistenceException
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     * @throws InterruptedException 
     * @return list of messages
     * @throws JSONException 
     */
    public List<CQSMessage> receiveMessage(CQSQueue queue, Map<String, String> receiveAttributes) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException, JSONException;

    /**
     * Change the message visibility timeout of a simple message
     * @param queue The queue which contains the messages.
     * @param receiptHandle The receipt handle of the message
     * @param visibilityTO the visibility timeout of the message
     * @throws PersistenceException
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     * @throws InterruptedException
     * @return true if succesful, false otherwise 
     */
    public boolean changeMessageVisibility(CQSQueue queue, String receiptHandle, int visibilityTO) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException;

    /**
     * Peek the queue with the given Queue URL for the next set of messages
     * @param queueUrl The URL of the Queue
     * @param previousReceiptHandle The receipt handle of the last item in the previous page
     * @param nextReceiptHandle The receipt handle of the first item in the next page
     * @throws PersistenceException
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     * @return list of messages between previousReceiptHandle & nextReceiptHandle
     * @throws JSONException 
     */
    public List<CQSMessage> peekQueue(String queueUrl, int shard, String previousReceiptHandle, String nextReceiptHandle, int length) throws PersistenceException, IOException, NoSuchAlgorithmException, JSONException;
    
    /**
     * Peek the queue with the given Queue URL for the next set of messages chosen at random from the queue
     * @param queueUrl The URL of the Queue
     * @param shard The shard to be used
     * @param length the number of messages to return 
     * @throws PersistenceException
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     * @return list of messages
     * @throws JSONException 
     */
    public List<CQSMessage> peekQueueRandom(String queueUrl, int shard, int length) throws PersistenceException, IOException, NoSuchAlgorithmException, JSONException;

    /**
     * Clear a specific shard of a queue
     * @param queueUrl The URL of the queue
     * @param shard The shard to be used
     * @throws PersistenceException
     * @throws UnsupportedEncodingException 
     * @throws NoSuchAlgorithmException 
     */
    public void clearQueue(String queueUrl, int shard) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException;
    
    /**
     * Get message payload for a list of message-ids 
     * @param ids The list of message-ids
     * @return The list of messages with those ids in any order
     * @throws UnsupportedEncodingException 
     * @throws NoSuchAlgorithmException 
     * @throws IOException 
     * @throws JSONException 
     */
	Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException, IOException, JSONException;
	
    /**
     * @param queueUrl
     * @param num number of message-ids to return
     * @return message ids from the head of the queue. If data not available, empty list is returned
     */
    public List<String> getIdsFromHead(String queueUrl, int shard, int num) throws PersistenceException;
    
    /**
     * @param queueUrl
     * @return number of messages in queue
     */
    public long getQueueMessageCount(String queueUrl);
    
    /**
     * 
     * @param queueUrl
     * @param processRevisibilitySet if true, run re-visibility processing.
     * @return number of mem-ids in Redis Queue
     * @throws Exception 
     */    
    public long getQueueMessageCount(String queueUrl, boolean processHiddenIds) throws Exception;
    
    /**
     * 
     * @param queueUrl
     * @param visibilityProcessFlag if true, run visibility processing.
     * @return number of mem-ids in Redis list
     * @throws Exception 
     */
    public long getQueueNotVisibleMessageCount(String queueUrl, boolean visibilityProcessFlag) throws Exception;
    
    /**
     * 
     * @param queueUrl
     * @param visibilityProcessFlag if true, run visibility processing.
     * @return number of mem-ids in Redis set for delayed messages
     * @throws Exception 
     */
    public long getQueueDelayedMessageCount(String queueUrl, boolean visibilityProcessFlag) throws Exception;
    /**
     * Check if the queue is in the cache and in ok state. Else kick off initialization
     * and return false. 
     * @param queueUrl
     * @param trueOnFiller returns true if the current state is Filling.
     * @return true if the cache is good for use. false if it is unavailable
     */
    public boolean checkCacheConsistency(String queueUrl, int shard, boolean trueOnFiller) ;
    
    public int getNumConnections() ;
    
    /**
     * 
     * @param queueUrl
     * @return number of mem-ids in Redis Queue. If Redis queue is empty, do not load from Cassandra.
     * @throws Exception 
     */
    public long getCacheQueueMessageCount(String queueUrl) throws Exception ;
}
