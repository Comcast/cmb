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
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
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
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;

import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbColumn;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbColumnSlice;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbComposite;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
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
public class CQSMessagePartitionedCassandraPersistence implements ICQSMessagePersistence {
	
	private static final String COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES = "CQSPartitionedQueueMessages";
	private static final Random rand = new Random();

	private static Logger logger = Logger.getLogger(CQSMessagePartitionedCassandraPersistence.class);
	
	private static final AbstractDurablePersistence cassandraHandler = DurablePersistenceFactory.getInstance();

	public CQSMessagePartitionedCassandraPersistence() {
	}

	@Override
	public String sendMessage(CQSQueue queue, int shard, CQSMessage message) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException, JSONException {
		
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
		CmbComposite columnName = cassandraHandler.getCmbComposite(AbstractDurablePersistence.newTime(ts, false), UUIDGen.getClockSeqAndNode());
		int ttl = queue.getMsgRetentionPeriod();
		int partition = rand.nextInt(queue.getNumberOfPartitions());
		String key = Util.hashQueueUrl(queue.getRelativeUrl()) + "_" + shard + "_" + partition;
		
		if (queue.isCompressed()) {
			message.setBody(Util.compress(message.getBody()));
		}

		message.setMessageId(key + ":" + columnName.get(0) + ":" + columnName.get(1));

		logger.debug("event=send_message ttl=" + ttl + " delay_sec=" + delaySeconds + " msg_id=" + message.getMessageId() + " key=" + key + " col=" + columnName);
		
		cassandraHandler.update(AbstractDurablePersistence.CQS_KEYSPACE, COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key, columnName, getMessageJSON(message),
				CMB_SERIALIZER.STRING_SERIALIZER,
				CMB_SERIALIZER.COMPOSITE_SERIALIZER,
				CMB_SERIALIZER.STRING_SERIALIZER, ttl);

		return message.getMessageId();
	}
	
	public List<CQSMessage> extractMessagesFromColumnSlice(String queueUrl, int length, CmbComposite previousHandle, CmbComposite nextHandle, CmbColumnSlice<CmbComposite, String> columnSlice, boolean ignoreFirstLastColumn) throws PersistenceException, NoSuchAlgorithmException, IOException, JSONException  {
		
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();

		if (columnSlice != null && columnSlice.getColumns() != null) {
			
			boolean noMatch = true;
			
			for (CmbColumn<CmbComposite, String> column : columnSlice.getColumns()) {
				
				CmbComposite columnName = column.getName();
				
				if (ignoreFirstLastColumn && (previousHandle != null && columnName.compareTo(previousHandle) == 0) || (nextHandle != null && columnName.compareTo(nextHandle) == 0)) {
					noMatch = false;
					continue;
				} else if (column.getValue() == null || column.getValue().length() == 0) {
					continue;
				}
				
				CQSMessage message = extractMessageFromJSON(queueUrl, column);
				messageList.add(message);
			}
			
			if (noMatch && messageList.size() > length) {
				messageList.remove(messageList.size() - 1);
			}
		}
		
		return messageList;
	}
	
	private CQSMessage extractMessageFromJSON(String queueUrl, CmbColumn column) throws JSONException, PersistenceException, IOException {
		
		CQSQueue queue = null;
		CQSMessage m = new CQSMessage();
		
		try {
			queue = CQSCache.getCachedQueue(queueUrl);
		} catch (Exception ex) {
			throw new PersistenceException(ex);
		}
		
		if (queue == null) {
			throw new PersistenceException(CMBErrorCodes.InternalError, "Unknown queue " + queueUrl);
		}
		
		JSONObject json = new JSONObject((String)column.getValue());

		m.setMessageId(json.getString("MessageId"));
		m.setReceiptHandle(json.getString("MessageId"));
		m.setMD5OfBody(json.getString("MD5OfBody"));
		m.setBody(json.getString("Body"));
		
		if (m.getAttributes() == null) {
			m.setAttributes(new HashMap<String, String>());
		}
		
		if (json.has(CQSConstants.SENT_TIMESTAMP)) {
			m.getAttributes().put(CQSConstants.SENT_TIMESTAMP, json.getString(CQSConstants.SENT_TIMESTAMP));
		}
		
		if (json.has(CQSConstants.APPROXIMATE_RECEIVE_COUNT)) {
			m.getAttributes().put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, json.getString(CQSConstants.APPROXIMATE_RECEIVE_COUNT));
		}

		if (json.has(CQSConstants.SENDER_ID)) {
			m.getAttributes().put(CQSConstants.SENDER_ID, json.getString(CQSConstants.SENDER_ID));
		}
		
		m.setTimebasedId(column.getName());
		
		if (queue.isCompressed()) {
			m.setBody(Util.decompress(m.getBody()));
		}
		
	    return m;
	}
	
	private String getMessageJSON(CQSMessage message) throws JSONException {
		
		Writer writer = new StringWriter(); 
    	JSONWriter jw = new JSONWriter(writer);
    	jw = jw.object();
		
    	jw.key("MessageId").value(message.getMessageId());
    	
    	jw.key("MD5OfBody").value(message.getMD5OfBody());
    	jw.key("Body").value(message.getBody());
		
		if (message.getAttributes() == null) {
			message.setAttributes(new HashMap<String, String>());
		}
		
		if (!message.getAttributes().containsKey(CQSConstants.SENT_TIMESTAMP)) {
			message.getAttributes().put(CQSConstants.SENT_TIMESTAMP, "" + System.currentTimeMillis());
		}
		
		if (!message.getAttributes().containsKey(CQSConstants.APPROXIMATE_RECEIVE_COUNT)) {
			message.getAttributes().put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, "0");
		}
		
		if (message.getAttributes() != null) {
			
			for (String key : message.getAttributes().keySet()) {
				
				String value = message.getAttributes().get(key);
				
				if (value == null || value.isEmpty()) {
					value = "";
				}
				
				jw.key(key).value(value);
			}
		}
		
		jw.endObject();
		
		return writer.toString();
	}

	@Override
	public Map<String, String> sendMessageBatch(CQSQueue queue,	int shard, List<CQSMessage> messages) throws PersistenceException,	IOException, InterruptedException, NoSuchAlgorithmException, JSONException {

		if (queue == null) {
			throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue doesn't exist");
		}

		if (messages == null || messages.size() == 0) {
			throw new PersistenceException(CQSErrorCodes.InvalidQueryParameter,	"No messages are supplied.");
		}
		
		Map<CmbComposite, String> messageDataMap = new HashMap<CmbComposite, String>();
		Map<String, String> ret = new HashMap<String, String>();
		int ttl = queue.getMsgRetentionPeriod();
		String key = Util.hashQueueUrl(queue.getRelativeUrl()) + "_" + shard + "_" + rand.nextInt(queue.getNumberOfPartitions());
		
		for (CQSMessage message : messages) {

			if (message == null) {
				throw new PersistenceException(CQSErrorCodes.InvalidMessageContents, "The supplied message is invalid");
			}
			
			if (queue.isCompressed()) {
				message.setBody(Util.compress(message.getBody()));
			}
			
			int delaySeconds = 0;
			
			if (message.getAttributes().containsKey(CQSConstants.DELAY_SECONDS)) {
				delaySeconds = Integer.parseInt(message.getAttributes().get(CQSConstants.DELAY_SECONDS));
			}
			
			long ts = System.currentTimeMillis() + delaySeconds*1000;
			CmbComposite columnName = cassandraHandler.getCmbComposite(AbstractDurablePersistence.newTime(ts, false), UUIDGen.getClockSeqAndNode());

			message.setMessageId(key + ":" + columnName.get(0) + ":" + columnName.get(1));
			
			logger.debug("event=send_message_batch msg_id=" + message.getMessageId() + " ttl=" + ttl + " delay_sec=" + delaySeconds + " key=" + key + " col=" + columnName);
			
			String messageJson = getMessageJSON(message);
			messageDataMap.put(columnName, messageJson);
			ret.put(message.getSuppliedMessageId(), message.getMessageId());
		}

		cassandraHandler.insertRow(AbstractDurablePersistence.CQS_KEYSPACE, key, COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, messageDataMap,
				CMB_SERIALIZER.STRING_SERIALIZER,
				CMB_SERIALIZER.COMPOSITE_SERIALIZER,
				CMB_SERIALIZER.STRING_SERIALIZER, ttl);
		
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
		
		CmbComposite columnName = cassandraHandler.getCmbComposite(Arrays.asList(Long.parseLong(receiptHandleParts[1]), Long.parseLong(receiptHandleParts[2])));
		
		if (columnName != null) {
			logger.debug("event=delete_message receipt_handle=" + receiptHandle + " col=" + columnName + " key=" + receiptHandleParts[0]);
			cassandraHandler.delete(AbstractDurablePersistence.CQS_KEYSPACE, COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, receiptHandleParts[0], columnName, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.COMPOSITE_SERIALIZER);
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
	public List<CQSMessage> peekQueue(String queueUrl, int shard, String previousReceiptHandle, String nextReceiptHandle, int length) throws PersistenceException, IOException, NoSuchAlgorithmException, JSONException {
		
		String queueHash = Util.hashQueueUrl(queueUrl);
		String key =  queueHash + "_" + shard + "_0";
		String handle = null;
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		CmbComposite previousHandle = null;
		CmbComposite nextHandle = null;
		
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
			previousHandle = cassandraHandler.getCmbComposite(Arrays.asList(Long.parseLong(handleParts[1]), Long.parseLong(handleParts[2])));
		
		} else if (nextReceiptHandle != null) {
			
			handle = nextReceiptHandle;
			String[] handleParts = handle.split(":");
			
			if (handleParts.length != 3) {
				logger.error("action=peek_queue error_code=corrupt_receipt_handle receipt_handle=" + handle);
				throw new IllegalArgumentException("Corrupt receipt handle " + handle);
			}
			
			key = handleParts[0];
			nextHandle = cassandraHandler.getCmbComposite(Arrays.asList(Long.parseLong(handleParts[1]), Long.parseLong(handleParts[2])));
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
			
			CmbColumnSlice<CmbComposite, String> columnSlice = cassandraHandler.readColumnSlice(
					AbstractDurablePersistence.CQS_KEYSPACE, COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key, previousHandle,
					nextHandle, length-messageList.size()+1, CMB_SERIALIZER.STRING_SERIALIZER,
					CMB_SERIALIZER.COMPOSITE_SERIALIZER,
					CMB_SERIALIZER.STRING_SERIALIZER);
			
			messageList.addAll(extractMessagesFromColumnSlice(queueUrl, length-messageList.size(), previousHandle, nextHandle, columnSlice, true));
			
			if (messageList.size() < length && -1 < partitionNumber && partitionNumber < numberPartitions) {
				
				if (previousHandle != null) {
					
					partitionNumber++;
					
					if (partitionNumber > -1) {
						previousHandle = cassandraHandler.getCmbComposite(Arrays.asList(AbstractDurablePersistence.newTime(System.currentTimeMillis()-1209600000, false), UUIDGen.getClockSeqAndNode()));
					}
				
				} else if (nextHandle != null) {
					
					partitionNumber--;
					
					if (partitionNumber < numberPartitions) {
						nextHandle = cassandraHandler.getCmbComposite(Arrays.asList(AbstractDurablePersistence.newTime(System.currentTimeMillis()+1209600000, false), UUIDGen.getClockSeqAndNode()));
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
		
		int numberPartitions = getNumberOfPartitions(queueUrl);
		
		logger.debug("event=clear_queue queue_url=" + queueUrl + " num_partitions=" + numberPartitions);
		
		for (int i=0; i<numberPartitions; i++) {
			String key = Util.hashQueueUrl(queueUrl) + "_" + shard + "_" + i;
			//cassandraHandler.deleteSuperColumn(COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key, null, CMB_SERIALIZER.STRING_SERIALIZER, CompositeSerializer.get());
			cassandraHandler.delete(AbstractDurablePersistence.CQS_KEYSPACE, COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key, null, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		}
	}

	@Override
	public Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids) throws PersistenceException, NoSuchAlgorithmException, IOException, JSONException {
		
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
			
			CmbComposite columnName = cassandraHandler.getCmbComposite(Arrays.asList(Long.parseLong(idParts[1]), Long.parseLong(idParts[2])));
			
			CmbColumn<CmbComposite, String> column = cassandraHandler.readColumn(AbstractDurablePersistence.CQS_KEYSPACE, COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, 
					idParts[0], columnName, CMB_SERIALIZER.STRING_SERIALIZER, 
					CMB_SERIALIZER.COMPOSITE_SERIALIZER,
					CMB_SERIALIZER.STRING_SERIALIZER);
			
			CQSMessage message = null;
			
			if (column != null) {
				message = extractMessageFromJSON(queueUrl, column);
			}
			
			messageMap.put(id, message);
		}
		
		return messageMap;
	}
	
	private Map<String, CQSMessage> getMessagesBulk(String queueUrl, List<String> ids) throws NoSuchAlgorithmException, PersistenceException, IOException, JSONException {
		
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
			
			CmbColumnSlice<CmbComposite, String> columnSlice = cassandraHandler.readColumnSlice(
					AbstractDurablePersistence.CQS_KEYSPACE, COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, queuePartition, cassandraHandler.getCmbComposite(Arrays.asList(Long.parseLong(firstParts[0]), Long.parseLong(firstParts[1]))),
					cassandraHandler.getCmbComposite(Arrays.asList(Long.parseLong(lastParts[0]), Long.parseLong(lastParts[1]))), messageCount, CMB_SERIALIZER.STRING_SERIALIZER,
					CMB_SERIALIZER.COMPOSITE_SERIALIZER,
					CMB_SERIALIZER.STRING_SERIALIZER);
			
			List<CQSMessage> messageList = extractMessagesFromColumnSlice(queueUrl, messageCount, null, null, columnSlice, false);
			
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
    public List<CQSMessage> peekQueueRandom(String queueUrl, int shard, int length) throws PersistenceException, IOException, NoSuchAlgorithmException, JSONException {
        
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
                
                CmbColumnSlice<CmbComposite, String> columnSlice = cassandraHandler.readColumnSlice(
                		AbstractDurablePersistence.CQS_KEYSPACE, COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key, null, null, 1, 
                        CMB_SERIALIZER.STRING_SERIALIZER,
                        CMB_SERIALIZER.COMPOSITE_SERIALIZER,
                        CMB_SERIALIZER.STRING_SERIALIZER);
               
                List<CQSMessage> messages = extractMessagesFromColumnSlice(queueUrl, 1, null, null, columnSlice, false);
                numFound += messages.size();
                messageList.addAll(messages);
            }
            
            return messageList;
        }        
    }

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
	public boolean checkCacheConsistency(String queueUrl, int shard,
			boolean trueOnFiller) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getQueueMessageCount(String queueUrl, boolean processHiddenIds)
			throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumConnections() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCacheQueueMessageCount(String queueUrl) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getQueueNotVisibleMessageCount(String queueUrl,
			boolean visibilityProcessFlag) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getQueueDelayedMessageCount(String queueUrl,
			boolean visibilityProcessFlag) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void flushAll() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isAlive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getNumberOfRedisShards() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Map<String, String>> getInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getMemQueueMessageCreatedTS(String memId) {
		// TODO Auto-generated method stub
		return 0;
	}
}
