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

import com.comcast.plaxo.cmb.common.persistence.CassandraPersistence;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cqs.model.CQSMessage;
import com.comcast.plaxo.cqs.model.CQSQueue;
import com.comcast.plaxo.cqs.util.CQSErrorCodes;
import com.comcast.plaxo.cqs.util.RandomNumberCollection;
import com.comcast.plaxo.cqs.util.Util;
import com.eaio.uuid.UUIDGen;
/**
 * Cassandra persistence for CQS Message
 * @author aseem, vvenkatraman, bwolf
 *
 */
public class CQSMessagePartitionedCassandraPersistence extends CassandraPersistence implements ICQSMessagePersistence {
	
	private static final String COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES = "CQSPartitionedQueueMessages";
	private static Random randomPartition = new Random();

	private static Logger logger = Logger.getLogger(CQSMessagePartitionedCassandraPersistence.class);

	private SuperCfTemplate<String, Composite, String> initqueueMessagesTemplate() {
		
		return new ThriftSuperCfTemplate<String, Composite, String>(
				keyspaces.get(HConsistencyLevel.QUORUM),
				COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, StringSerializer.get(),
				new CompositeSerializer(), StringSerializer.get());
	}

	public CQSMessagePartitionedCassandraPersistence() {
		super(CMBProperties.getInstance().getCMBCQSKeyspace());
	}

	@Override
	public String sendMessage(CQSQueue queue, CQSMessage message) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException {
		
		if (queue == null) {
			throw new PersistenceException(
					CQSErrorCodes.NonExistentQueue,
					"The supplied queue doesn't exist");
		}
		
		if (message == null) {
			throw new PersistenceException(
					CQSErrorCodes.InvalidMessageContents,
					"The supplied message is invalid");
		}
		
		final Composite superColumnName = new Composite(
				newTime(System.currentTimeMillis(), false),
				UUIDGen.getClockSeqAndNode());
		int ttl = queue.getMsgRetentionPeriod();
		
		String key = Util.hashQueueUrl(queue.getRelativeUrl()) + "_" + randomPartition.nextInt(CMBProperties.getInstance().getCqsNumberOfQueuePartitions());

		// String compressedMessage = Util.compress(message);

		message.setMessageId(key + ":" + superColumnName.get(0) + ":" + superColumnName.get(1));
		insertSuperColumn(COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, key,
				StringSerializer.get(), superColumnName, ttl,
				new CompositeSerializer(), Util.buildMessageMap(message),
				StringSerializer.get(), StringSerializer.get(),
				HConsistencyLevel.QUORUM);

		return message.getMessageId();
	}


	@Override
	public Map<String, String> sendMessageBatch(CQSQueue queue,
			List<CQSMessage> messages) throws PersistenceException,
			IOException, InterruptedException, NoSuchAlgorithmException {

		if (queue == null) {
			throw new PersistenceException(
					CQSErrorCodes.NonExistentQueue,
					"The supplied queue doesn't exist");
		}

		if (messages == null || messages.size() == 0) {
			throw new PersistenceException(CQSErrorCodes.InvalidQueryParameter,
					"No messages are supplied.");
		}
		
		Map<Composite, Map<String, String>> messageDataMap = new HashMap<Composite, Map<String, String>>();
		Map<String, String> ret = new HashMap<String, String>();
		int ttl = queue.getMsgRetentionPeriod();
		String key = Util.hashQueueUrl(queue.getRelativeUrl()) + "_" + randomPartition.nextInt(CMBProperties.getInstance().getCqsNumberOfQueuePartitions());
		
		for (CQSMessage message : messages) {

			if (message == null) {
				throw new PersistenceException(
						CQSErrorCodes.InvalidMessageContents,
						"The supplied message is invalid");
			}
			
			final Composite superColumnName = new Composite(Arrays.asList(
					newTime(System.currentTimeMillis(), false),
					UUIDGen.getClockSeqAndNode()));
			
			message.setMessageId(key + ":" + superColumnName.get(0) + ":" + superColumnName.get(1));
			
			logger.debug("Action=sendMessage body=" + message.getBody()
					+ " id=" + message.getMessageId());
			
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
		
		// this is with the assumption that the receiptHandle is the message id
		
		if (receiptHandle == null) {
			logger.error("action=delete_message event=no_receipt_handle queue_url=" + queueUrl);
			return;
		}
		
		String[] receiptHandleParts = receiptHandle.split(":");
		
		if (receiptHandleParts.length != 3) {
			logger.error("action=delete_message event=invalid_receipt_handle queue_url=" + queueUrl + " receipt_handle=" + receiptHandle);
			return;
		}
		
		SuperCfTemplate<String, Composite, String> queueMessagesTemplate = initqueueMessagesTemplate();
		Composite superColumnName = new Composite(Arrays.asList(Long.parseLong(receiptHandleParts[1]), Long.parseLong(receiptHandleParts[2])));
		
		if (superColumnName != null) {
			logger.debug("action=delete_message receipt_handle=" + receiptHandle + " super_column_name=" + superColumnName);
			deleteSuperColumn(queueMessagesTemplate, receiptHandleParts[0], superColumnName);
		}
	}

	@Override
	public List<CQSMessage> receiveMessage(CQSQueue queue,
			Map<String, String> receiveAttributes) throws PersistenceException,
			IOException, NoSuchAlgorithmException, InterruptedException {
		throw new UnsupportedOperationException("This operation is not supported.  Please call getMessages");
	}

	@Override
	public boolean changeMessageVisibility(CQSQueue queue,
			String receiptHandle, int visibilityTO)
			throws PersistenceException, IOException, NoSuchAlgorithmException,
			InterruptedException {
		throw new UnsupportedOperationException("This operation is not supported.  Please call getMessages");
	}

	@Override
	public List<CQSMessage> peekQueue(String queueUrl,
			String previousReceiptHandle, String nextReceiptHandle, int length)
			throws PersistenceException, IOException, NoSuchAlgorithmException {
		String queueHash = Util.hashQueueUrl(queueUrl);
		String queuePartition =  queueHash + "_0";
		String handle = null;
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		Composite previousHandle = null;
		Composite nextHandle = null;
		if (previousReceiptHandle != null) {
			handle = previousReceiptHandle;
			String[] handleParts = handle.split(":");
			if (handleParts.length != 3) {
				logger.error("action=peek_queue status=error message=corrupt_receipt_handle receipt_handle" + handle);
				throw new IllegalArgumentException("Corrupt receipt handle " + handle);
			}
			queuePartition = handleParts[0];
			previousHandle = new Composite(Arrays.asList(Long.parseLong(handleParts[1]), Long.parseLong(handleParts[2])));
		} 
		else if (nextReceiptHandle != null) {
			handle = nextReceiptHandle;
			String[] handleParts = handle.split(":");
			if (handleParts.length != 3) {
				logger.error("action=peek_queue status=error message=corrupt_receipt_handle receipt_handle=" + handle);
				throw new IllegalArgumentException("Corrupt receipt handle " + handle);
			}
			queuePartition = handleParts[0];
			nextHandle = new Composite(Arrays.asList(Long.parseLong(handleParts[1]), Long.parseLong(handleParts[2])));
		}
		
		String[] queueParts = queuePartition.split("_");
		if (queueParts.length != 2) {
			logger.error("action=peek_queue status=error message=invalid_queue_partition partition=" + queuePartition);
			throw new IllegalArgumentException("Invalid queue partition " + queuePartition);
		}
		int partitionNumber = Integer.parseInt(queueParts[1]);
		if (partitionNumber < 0 || partitionNumber > CMBProperties.getInstance().getCqsNumberOfQueuePartitions()-1) {
			String message = "action=peek_queue status=error message=invalid_queue_partition partition=" + queuePartition;
			logger.error(message);
			throw new IllegalArgumentException("Invalid queue partition " + queuePartition);			
		}
		
		while (messageList.size() < length && -1 < partitionNumber && partitionNumber < CMBProperties.getInstance().getCqsNumberOfQueuePartitions()) {
			queuePartition = queueHash + "_" + partitionNumber;
			SuperSlice<Composite, String, String> superSlice = readRowFromSuperColumnFamily(
					COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, queuePartition, previousHandle,
					nextHandle, length-messageList.size()+1, StringSerializer.get(),
					new CompositeSerializer(), StringSerializer.get(),
					StringSerializer.get(), HConsistencyLevel.QUORUM);
			messageList.addAll(Util.readMessagesFromSuperColumns(length-messageList.size(), previousHandle, nextHandle, superSlice, true));
			if (messageList.size() < length && -1 < partitionNumber && partitionNumber < CMBProperties.getInstance().getCqsNumberOfQueuePartitions()) {
				if (previousHandle != null) {
					partitionNumber++;
					if (partitionNumber > -1) {
						previousHandle = new Composite(Arrays.asList(newTime(System.currentTimeMillis()-1209600000, false), UUIDGen.getClockSeqAndNode()));
					}
				} 
				else if (nextHandle != null) {
					partitionNumber--;
					if (partitionNumber < CMBProperties.getInstance().getCqsNumberOfQueuePartitions()) {
						nextHandle = new Composite(Arrays.asList(newTime(System.currentTimeMillis()+1209600000, false), UUIDGen.getClockSeqAndNode()));
					}
				}
				else {
					partitionNumber++;
				}
			}
		}
		return messageList;
	}

	@Override
	public void clearQueue(String queueUrl) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
		SuperCfTemplate<String, Composite, String> queueMessagesTemplate = initqueueMessagesTemplate();
		for (int i=0; i<CMBProperties.getInstance().getCqsNumberOfQueuePartitions(); i++) {
			String key = Util.hashQueueUrl(queueUrl) + "_" + i;
			deleteSuperColumn(queueMessagesTemplate, key, null);
		}
	}

	@Override
	public Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids)
			throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
		Map<String, CQSMessage> messageMap = new HashMap<String, CQSMessage>();
		if (ids == null || ids.size() == 0) {
			return messageMap;
		} else if (ids.size() > 100) {
			return getMessagesBulk(queueUrl, ids);
		}
		
		for (String id: ids) {
			String[] idParts = id.split(":");
			if (idParts.length != 3) {
				logger.error("action=get_messages status=error messsage=invalid_id id=" + id);
				throw new IllegalArgumentException("Invalid id " + id);
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
				logger.error("action=peek_queue status=error message=corrupt_receipt_handle receipt_handle=" + id);
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

    @Override
    public List<CQSMessage> peekQueueRandom(String queueUrl, int length) throws PersistenceException, IOException, NoSuchAlgorithmException {
        
        String queueHash = Util.hashQueueUrl(queueUrl);
        List<CQSMessage> messageList = new ArrayList<CQSMessage>();
        
        if (length > CMBProperties.getInstance().getCqsNumberOfQueuePartitions()) {
            //no randomness. Get from all rows. Subsequent calls will return the same result
            return peekQueue(queueUrl, null, null, length);
        } else {
            //get from random set of rows
            //Note: as a simplification we may return less messages than length if not all rows
            //contain messages.
            RandomNumberCollection rc = new RandomNumberCollection(CMBProperties.getInstance().getCqsNumberOfQueuePartitions());
            int numFound = 0;
            for (int i = 0; i < CMBProperties.getInstance().getCqsNumberOfQueuePartitions() && numFound < length; i++) {
                int partition = rc.getNext();
                logger.debug("event=pick_random_partition partition=" + partition);
                String queuePartition = queueHash + "_" + partition;
                SuperSlice<Composite, String, String> superSlice = readRowFromSuperColumnFamily(
                        COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES, queuePartition, null, null, 1, 
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
