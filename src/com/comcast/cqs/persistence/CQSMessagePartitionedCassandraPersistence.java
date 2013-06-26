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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.SuperCfTemplate;
import me.prettyprint.cassandra.service.template.ThriftSuperCfTemplate;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.controller.CQSCache;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
import com.comcast.cqs.util.RandomNumberCollection;
import com.comcast.cqs.util.Util;
import com.eaio.uuid.UUIDGen;
/**
 * Cassandra persistence for CQS Message
 * @author aseem, vvenkatraman, bwolf
 *
 */
public class CQSMessagePartitionedCassandraPersistence extends CassandraPersistence implements ICQSMessagePersistence {
	
	private static final String COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES = "CQSPartitionedQueueMessages";
	private static final Random rand = new Random();

	private static Logger logger = Logger.getLogger(CQSMessagePartitionedCassandraPersistence.class);

	private SuperCfTemplate<String, Composite, String> initQueueMessagesTemplate() {
		
		return new ThriftSuperCfTemplate<String, Composite, String>(
				keyspaces.get(HConsistencyLevel.QUORUM),
				COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, StringSerializer.get(),
				new CompositeSerializer(), StringSerializer.get());
	}

	public CQSMessagePartitionedCassandraPersistence() {
		super(CMBProperties.getInstance().getCQSKeyspace());
	}

	@Override
	public String sendMessage(CQSQueue queue, int shard, CQSMessage message) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException {
		
		if (queue == null) {
			throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue does not exist");
		}
		
		if (message == null) {
			throw new PersistenceException(CQSErrorCodes.InvalidMessageContents, "The supplied message is invalid");
		}
		
		int delaySeconds = 0;
		
		if (message.getAttributes().containsKey(CQSConstants.DELAY_SECONDS)) {
			delaySeconds = Integer.parseInt(message.getAttributes().get(CQSConstants.DELAY_SECONDS));
		}
		
		long ts = System.currentTimeMillis() + delaySeconds*1000;
		final Composite superColumnName = new Composite(newTime(ts, false),	UUIDGen.getClockSeqAndNode());
		int ttl = queue.getMsgRetentionPeriod();
		int partition = rand.nextInt(queue.getNumberOfPartitions());
		String key = Util.hashQueueUrl(queue.getRelativeUrl()) + "_" + shard + "_" + partition;
		
		// String compressedMessage = Util.compress(message);

		message.setMessageId(key + ":" + superColumnName.get(0) + ":" + superColumnName.get(1));

		logger.debug("event=send_message ttl=" + ttl + " delay_sec=" + delaySeconds + " msg_id=" + message.getMessageId() + " key=" + key + " col=" + superColumnName);
		
		insertSuperColumn(COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key,
				StringSerializer.get(), superColumnName, ttl,
				new CompositeSerializer(), Util.buildMessageMap(message),
				StringSerializer.get(), StringSerializer.get(),
				HConsistencyLevel.QUORUM);

		return message.getMessageId();
	}

	@Override
	public Map<String, String> sendMessageBatch(CQSQueue queue,	int shard, List<CQSMessage> messages) throws PersistenceException,	IOException, InterruptedException, NoSuchAlgorithmException {

		if (queue == null) {
			throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue doesn't exist");
		}

		if (messages == null || messages.size() == 0) {
			throw new PersistenceException(CQSErrorCodes.InvalidQueryParameter,	"No messages are supplied.");
		}
		
		Map<Composite, Map<String, String>> messageDataMap = new HashMap<Composite, Map<String, String>>();
		Map<String, String> ret = new HashMap<String, String>();
		int ttl = queue.getMsgRetentionPeriod();
		String key = Util.hashQueueUrl(queue.getRelativeUrl()) + "_" + shard + "_" + rand.nextInt(queue.getNumberOfPartitions());
		
		for (CQSMessage message : messages) {

			if (message == null) {
				throw new PersistenceException(CQSErrorCodes.InvalidMessageContents, "The supplied message is invalid");
			}
			
			int delaySeconds = 0;
			
			if (message.getAttributes().containsKey(CQSConstants.DELAY_SECONDS)) {
				delaySeconds = Integer.parseInt(message.getAttributes().get(CQSConstants.DELAY_SECONDS));
			}
			
			long ts = System.currentTimeMillis() + delaySeconds*1000;
			final Composite superColumnName = new Composite(Arrays.asList(newTime(ts, false), UUIDGen.getClockSeqAndNode()));
			message.setMessageId(key + ":" + superColumnName.get(0) + ":" + superColumnName.get(1));
			
			logger.debug("event=send_message_batch msg_id=" + message.getMessageId() + " ttl=" + ttl + " delay_sec=" + delaySeconds + " key=" + key + " col=" + superColumnName);
			
			Map<String, String> currentMessageDataMap = Util.buildMessageMap(message);
			messageDataMap.put(superColumnName, currentMessageDataMap);
			ret.put(message.getSuppliedMessageId(), message.getMessageId());
		}

		// String compressedMessage = Util.compress(message);

		insertSuperColumns(COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key,
				StringSerializer.get(), messageDataMap, ttl,
				new CompositeSerializer(), StringSerializer.get(),
				StringSerializer.get(), HConsistencyLevel.QUORUM);
		
		return ret;
	}

	@Override
	public void deleteMessage(String queueUrl, String receiptHandle) throws PersistenceException {
		
		if (receiptHandle == null) {
			logger.error("event=delete_message event=no_receipt_handle queue_url=" + queueUrl);
			return;
		}
		
		String[] receiptHandleParts = receiptHandle.split(":");
		
		if (receiptHandleParts.length != 3) {
			logger.error("event=delete_message event=invalid_receipt_handle queue_url=" + queueUrl + " receipt_handle=" + receiptHandle);
			return;
		}
		
		SuperCfTemplate<String, Composite, String> queueMessagesTemplate = initQueueMessagesTemplate();
		Composite superColumnName = new Composite(Arrays.asList(Long.parseLong(receiptHandleParts[1]), Long.parseLong(receiptHandleParts[2])));
		
		if (superColumnName != null) {
			logger.debug("event=delete_message receipt_handle=" + receiptHandle + " col=" + superColumnName + " key=" + receiptHandleParts[0]);
			deleteSuperColumn(queueMessagesTemplate, receiptHandleParts[0], superColumnName);
		}
	}

	@Override
	public List<CQSMessage> receiveMessage(CQSQueue queue, Map<String, String> receiveAttributes) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException {
		throw new UnsupportedOperationException("ReceiveMessage is not supported, please call getMessages instead");
	}

	@Override
	public boolean changeMessageVisibility(CQSQueue queue, String receiptHandle, int visibilityTO) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException {
		throw new UnsupportedOperationException("ChangeMessageVisibility is not supported");
	}

	@Override
	public List<CQSMessage> peekQueue(String queueUrl, int shard, String previousReceiptHandle, String nextReceiptHandle, int length) throws PersistenceException, IOException, NoSuchAlgorithmException {
		
		String queueHash = Util.hashQueueUrl(queueUrl);
		String key =  queueHash + "_" + shard + "_0";
		String handle = null;
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		Composite previousHandle = null;
		Composite nextHandle = null;
		
		int numberPartitions = getNumberOfPartitions(queueUrl);
		int numberShards = getNumberOfShards(queueUrl);
		
		logger.debug("event=peek_queue queue_url=" + queueUrl + " prev_receipt_handle=" + previousReceiptHandle + " next_receipt_handle=" + nextReceiptHandle + " length=" + length + " num_partitions=" + numberPartitions);
		
		if (previousReceiptHandle != null) {
			
			handle = previousReceiptHandle;
			String[] handleParts = handle.split(":");
			
			if (handleParts.length != 3) {
				logger.error("event=peek_queue error_code=corrupt_receipt_handle receipt_handle=" + handle);
				throw new IllegalArgumentException("Corrupt receipt handle " + handle);
			}
			
			key = handleParts[0];
			previousHandle = new Composite(Arrays.asList(Long.parseLong(handleParts[1]), Long.parseLong(handleParts[2])));
		
		} else if (nextReceiptHandle != null) {
			
			handle = nextReceiptHandle;
			String[] handleParts = handle.split(":");
			
			if (handleParts.length != 3) {
				logger.error("action=peek_queue error_code=corrupt_receipt_handle receipt_handle=" + handle);
				throw new IllegalArgumentException("Corrupt receipt handle " + handle);
			}
			
			key = handleParts[0];
			nextHandle = new Composite(Arrays.asList(Long.parseLong(handleParts[1]), Long.parseLong(handleParts[2])));
		}
		
		String[] queueParts = key.split("_");
		
		if (queueParts.length != 3) {
			logger.error("event=peek_queue error_code=invalid_queue_key key=" + key);
			throw new IllegalArgumentException("Invalid queue key " + key);
		}
		
		int shardNumber = Integer.parseInt(queueParts[1]);
		int partitionNumber = Integer.parseInt(queueParts[2]);
		
		if (partitionNumber < 0 || partitionNumber > numberPartitions-1) {
			logger.error("event=peek_queue error_code=invalid_partition_number partition_number=" + partitionNumber);
			throw new IllegalArgumentException("Invalid queue partition number " + partitionNumber);			
		}
		
		if (shardNumber < 0 || shardNumber > numberShards-1) {
			logger.error("event=peek_queue error_code=invalid_shard_number shard_number=" + shardNumber);
			throw new IllegalArgumentException("Invalid queue shard number " + shardNumber);			
		}

		while (messageList.size() < length && -1 < partitionNumber && partitionNumber < numberPartitions) {
			
			key = queueHash + "_" + shardNumber + "_" + partitionNumber;
			
			SuperSlice<Composite, String, String> superSlice = readRowFromSuperColumnFamily(
					COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key, previousHandle,
					nextHandle, length-messageList.size()+1, StringSerializer.get(),
					new CompositeSerializer(), StringSerializer.get(),
					StringSerializer.get(), HConsistencyLevel.QUORUM);
			
			messageList.addAll(Util.readMessagesFromSuperColumns(length-messageList.size(), previousHandle, nextHandle, superSlice, true));
			
			if (messageList.size() < length && -1 < partitionNumber && partitionNumber < numberPartitions) {
				
				if (previousHandle != null) {
					
					partitionNumber++;
					
					if (partitionNumber > -1) {
						previousHandle = new Composite(Arrays.asList(newTime(System.currentTimeMillis()-1209600000, false), UUIDGen.getClockSeqAndNode()));
					}
				
				} else if (nextHandle != null) {
					
					partitionNumber--;
					
					if (partitionNumber < numberPartitions) {
						nextHandle = new Composite(Arrays.asList(newTime(System.currentTimeMillis()+1209600000, false), UUIDGen.getClockSeqAndNode()));
					}
					
				} else {
					partitionNumber++;
				}
			}
		}
		
		return messageList;
	}

	@Override
	public void clearQueue(String queueUrl, int shard) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
		
		SuperCfTemplate<String, Composite, String> queueMessagesTemplate = initQueueMessagesTemplate();

		int numberPartitions = getNumberOfPartitions(queueUrl);
		
		logger.debug("event=clear_queue queue_url=" + queueUrl + " num_partitions=" + numberPartitions);
		
		for (int i=0; i<numberPartitions; i++) {
			String key = Util.hashQueueUrl(queueUrl) + "_" + shard + "_" + i;
			deleteSuperColumn(queueMessagesTemplate, key, null);
		}
	}

	@Override
	public Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
		
		Map<String, CQSMessage> messageMap = new HashMap<String, CQSMessage>();
		
		logger.debug("event=get_messages ids=" + ids);
		
		if (ids == null || ids.size() == 0) {
			return messageMap;
		} else if (ids.size() > 100) {
			return getMessagesBulk(queueUrl, ids);
		}
		
		for (String id: ids) {
			
			String[] idParts = id.split(":");
			
			if (idParts.length != 3) {
				logger.error("event=get_messages error_code=invalid_message_id id=" + id);
				throw new IllegalArgumentException("Invalid message id " + id);
			}
			
			Composite superColumnName = new Composite(Arrays.asList(Long.parseLong(idParts[1]), Long.parseLong(idParts[2])));
			
			HSuperColumn<Composite, String, String> superColumn = readColumnFromSuperColumnFamily(COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, 
					idParts[0], superColumnName, StringSerializer.get(), 
					new CompositeSerializer(), StringSerializer.get(),
					StringSerializer.get(), HConsistencyLevel.QUORUM);
			
			CQSMessage message = null;
			
			if (superColumn != null) {
				message = Util.extractMessageFromSuperColumn(superColumn);
			}
			
			messageMap.put(id, message);
		}
		
		return messageMap;
	}
	
	private Map<String, CQSMessage> getMessagesBulk(String queueUrl, List<String> ids) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		logger.debug("event=get_message_bulk ids=" + ids);
		
		Map<String, CQSMessage> messageMap = new HashMap<String, CQSMessage>();
		Set<String> messageIdSet = new HashSet<String>();
		Map<String, Map<String, String>> firstLastIdsForEachPartition = getFirstAndLastIdsForEachPartition(ids, messageIdSet);
		
		if (firstLastIdsForEachPartition.size() == 0) {
			return messageMap;
		}
		
		for (String queuePartition: firstLastIdsForEachPartition.keySet()) {
			
			int messageCount = 200;
			Map<String, String> firstLastForPartition = firstLastIdsForEachPartition.get(queuePartition);
			String firstParts[] = firstLastForPartition.get("First").split(":");
			String lastParts[] = firstLastForPartition.get("Last").split(":");
			
			SuperSlice<Composite, String, String> superSlice = readRowFromSuperColumnFamily(
					COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, queuePartition, new Composite(Arrays.asList(Long.parseLong(firstParts[0]), Long.parseLong(firstParts[1]))),
					new Composite(Arrays.asList(Long.parseLong(lastParts[0]), Long.parseLong(lastParts[1]))), messageCount, StringSerializer.get(),
					new CompositeSerializer(), StringSerializer.get(),
					StringSerializer.get(), HConsistencyLevel.QUORUM);
			
			List<CQSMessage> messageList = Util.readMessagesFromSuperColumns(messageCount, null, null, superSlice, false);
			
			for (CQSMessage message: messageList) {
				
				if (messageIdSet.contains(message.getMessageId())) {
					messageMap.put(message.getMessageId(), message);
					messageIdSet.remove(message.getMessageId());
				}
			}
		}

		for (String messageId : messageIdSet) {
	        messageMap.put(messageId, null);
	    }
		
		return messageMap;
	}
	
	private Map<String, Map<String, String>> getFirstAndLastIdsForEachPartition(List<String> ids, Set<String> messageIdSet) {
		
		Map<String, Map<String, String>> firstLastForEachPartitionMap = new HashMap<String, Map<String, String>>();
		
		if (ids == null || ids.size() == 0) {
			return firstLastForEachPartitionMap;
		}
		
		for (String id: ids) {			
			
			messageIdSet.add(id);
			String[] idParts = id.split(":");
			
			if (idParts.length != 3) {
				logger.error("action=get_messages_bulk error_code=corrupt_receipt_handle receipt_handle=" + id);
				throw new IllegalArgumentException("Corrupt receipt handle " + id);
			}
			
			String queuePartition = idParts[0];
			final String messageId = idParts[1] + ":" + idParts[2];
			
			if (!firstLastForEachPartitionMap.containsKey(queuePartition)) {
				firstLastForEachPartitionMap.put(queuePartition, new HashMap<String, String>() {{put("First", messageId); put("Last", messageId);}});
			} else {
				Map<String, String> firstLastForPartition = firstLastForEachPartitionMap.get(queuePartition);
				if (firstLastForPartition.get("First").compareTo(messageId) > 0) {
					firstLastForPartition.put("First", messageId);
				} 
				else if (firstLastForPartition.get("Last").compareTo(messageId) < 0) {
					firstLastForPartition.put("Last", messageId);
				}
			}
		}
		
		return firstLastForEachPartitionMap;
	}
	
	private int getNumberOfPartitions(String queueUrl) {

		int numberPartitions = CMBProperties.getInstance().getCQSNumberOfQueuePartitions();
		
		try {
			
			CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
			
			if (queue != null) {
				numberPartitions = queue.getNumberOfPartitions();
			}
			
		} catch (Exception ex) {
			logger.warn("event=queue_cache_failure queue_url=" + queueUrl, ex);
		}

		return numberPartitions;
	}

	private int getNumberOfShards(String queueUrl) {

		int numberShards = 1;
		
		try {
			
			CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
			
			if (queue != null) {
				numberShards = queue.getNumberOfShards();
			}
			
		} catch (Exception ex) {
			logger.warn("event=queue_cache_failure queue_url=" + queueUrl, ex);
		}

		return numberShards;
	}

	@Override
    public List<CQSMessage> peekQueueRandom(String queueUrl, int shard, int length) throws PersistenceException, IOException, NoSuchAlgorithmException {
        
    	String queueHash = Util.hashQueueUrl(queueUrl);

    	int numberPartitions = getNumberOfPartitions(queueUrl);

    	logger.debug("event=peek_queue_random queue_url=" + queueUrl + " shard=" + shard + " queue_hash=" + queueHash + " num_partitions=" + numberPartitions);
    	
    	List<CQSMessage> messageList = new ArrayList<CQSMessage>();
        
        if (length > numberPartitions) {
            
        	// no randomness, get from all rows, subsequent calls will return the same result
            
        	return peekQueue(queueUrl, shard, null, null, length);
        
        } else {
            
        	// get from random set of rows
            // note: as a simplification we may return less messages than length if not all rows contain messages
            
        	RandomNumberCollection rc = new RandomNumberCollection(numberPartitions);
            int numFound = 0;
            
            for (int i = 0; i < numberPartitions && numFound < length; i++) {
                
            	int partition = rc.getNext();
                String key = queueHash + "_" + shard + "_" + partition;
                
                SuperSlice<Composite, String, String> superSlice = readRowFromSuperColumnFamily(
                        COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key, null, null, 1, 
                        StringSerializer.get(),
                        new CompositeSerializer(), StringSerializer.get(),
                        StringSerializer.get(), HConsistencyLevel.QUORUM);
               
                List<CQSMessage> messages = Util.readMessagesFromSuperColumns(1, null, null, superSlice, false);
                numFound += messages.size();
                messageList.addAll(messages);
            }
            
            return messageList;
        }        
    }
}
