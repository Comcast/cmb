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
package com.comcast.cqs.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.controller.CQSCache;
import com.comcast.cqs.controller.CQSLongPollSender;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSMessageAttribute;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;

/**
 * Wrapper class for all CQS API calls as a layer to call CQS APIs within the same JVM. Eventually all calls coming in through Jetty 
 * should go through this layer.
 * @author boris
 *
 */
public class CQSAPI {
	
    protected static Logger logger = Logger.getLogger(CQSAPI.class);
	private static Random rand = new Random();
	
	//TODO: add missing APIs
	//TODO: improve logging
	
	private static void emitLogLine(String userId, String action, String queueUrl, List<String> receiptHandles, long rt) {

		StringBuffer logLine = new StringBuffer("");

		logLine.append("event=req status=ok client=inline Action=").append(action);
		
		if (receiptHandles != null) {
			for (String receiptHandle : receiptHandles) {
				logLine.append(" ReceiptHandle=").append(receiptHandle);
			}
		}
		
		logLine.append(" resp_ms=").append(rt);
		//logLine.append(" cass_ms=").append(CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime));
		//logLine.append(" cass_num_rd=").append(CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraRead));
		//logLine.append(" cass_num_wr=").append(CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraWrite));
		//ogLine.append(" redis_ms=").append(CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.RedisTime));
		//logLine.append(" async_pool_queue=").append(CMBControllerServlet.workerPool.getQueue().size()).
		//append(" async_pool_size=").append(CMBControllerServlet.workerPool.getActiveCount()).
		//append(" cqs_pool_size=").append(CMB.cqsServer.getThreadPool().getThreads()).
		//append(" cns_pool_size=").append(CMB.cnsServer.getThreadPool().getThreads());
		
		logger.info(logLine.toString());
	}
	
	public static String sendMessage(String userId, String relativeQueueUrl, String messageBody, Integer delaySeconds, Map<String, CQSMessageAttribute> messageAttributes) throws Exception {
		
		long ts1 = System.currentTimeMillis();

	    CQSQueue queue = CQSCache.getCachedQueue(relativeQueueUrl);

	    if (queue == null) {
	    	throw new CMBException(CMBErrorCodes.InternalError, "Unknown queue " + relativeQueueUrl);
	    }
        
		if (messageBody == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "MessageBody not found");
        }

        if (messageBody.length() > CMBProperties.getInstance().getCQSMaxMessageSize()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Value for parameter MessageBody is invalid. Reason: Message body must be shorter than " + CMBProperties.getInstance().getCQSMaxMessageSize() + " bytes");
        }
        
        if (!Util.isValidUnicode(messageBody)) {
            throw new CMBException(CMBErrorCodes.InvalidMessageContents, "The message contains characters outside the allowed set.");
        }
        
        HashMap<String, String> attributes = new HashMap<String, String>();

        if (delaySeconds != null) { 
	        if (delaySeconds < 0 || delaySeconds > CMBProperties.getInstance().getCQSMaxMessageDelaySeconds()) {
	            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "DelaySeconds should be from 0 to " + CMBProperties.getInstance().getCQSMaxMessageDelaySeconds());
	        } else {
	            attributes.put(CQSConstants.DELAY_SECONDS, "" + delaySeconds);
	        }
        }

        attributes.put(CQSConstants.SENDER_ID, userId);
        attributes.put(CQSConstants.SENT_TIMESTAMP, "" + System.currentTimeMillis());
        attributes.put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, "0");
        attributes.put(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, "");
        
        CQSMessage message = new CQSMessage(messageBody, attributes, messageAttributes);
        
		int shard = rand.nextInt(queue.getNumberOfShards());

        String receiptHandle = PersistenceFactory.getCQSMessagePersistence().sendMessage(queue, shard, message);

        if (receiptHandle == null || receiptHandle.isEmpty()) {
            throw new CMBException(CMBErrorCodes.InternalError, "Failed to add message to queue");
        }
        
        try {
        	CQSLongPollSender.send(queue.getArn());
        } catch (Exception ex) {
        	logger.warn("event=failed_to_send_longpoll_notification", ex);
        }
        
		long ts2 = System.currentTimeMillis();
		emitLogLine(userId, "SendMessage", relativeQueueUrl, new ArrayList<String>(Arrays.asList(receiptHandle)), ts2-ts1);
        
        return receiptHandle;
	}
	
	public static List<CQSMessage> receiveMessages(String userId, String relativeQueueUrl, Integer maxNumberOfMessages, Integer visibilityTimeout) throws Exception {
		
		//TODO: longpoll currently not supported
		
		long ts1 = System.currentTimeMillis();
		
		List<CQSMessage> messages = null;

		CQSQueue queue = CQSCache.getCachedQueue(relativeQueueUrl);

	    if (queue == null) {
	    	throw new CMBException(CMBErrorCodes.InternalError, "Unknown queue " + relativeQueueUrl);
	    }
        
		HashMap<String, String> msgParam = new HashMap<String, String>();
		
		if (maxNumberOfMessages != null) {

			if (maxNumberOfMessages < 1 || maxNumberOfMessages > CMBProperties.getInstance().getCQSMaxReceiveMessageCount()) {
				throw new CMBException(CMBErrorCodes.InvalidParameterValue, "The value for MaxNumberOfMessages is not valid (must be from 1 to " + CMBProperties.getInstance().getCQSMaxReceiveMessageCount() + ").");
			}
    	
			msgParam.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "" + maxNumberOfMessages);
		}
		
		if (visibilityTimeout != null) {
		
			if (visibilityTimeout < 0 || visibilityTimeout > CMBProperties.getInstance().getCQSMaxVisibilityTimeOut()) {
	            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "The value for VisibilityTimeout is not valid (must be from 0 to " + CMBProperties.getInstance().getCQSMaxVisibilityTimeOut() + ").");
	    	}
			
        	msgParam.put(CQSConstants.VISIBILITY_TIMEOUT, "" + visibilityTimeout);
		}

		messages = PersistenceFactory.getCQSMessagePersistence().receiveMessage(queue, msgParam);
		List<String> receiptHandles = new ArrayList<String>();
		
		for (CQSMessage m : messages) {
			receiptHandles.add(m.getReceiptHandle());
		}

		long ts2 = System.currentTimeMillis();
		emitLogLine(userId, "ReceiveMessage", relativeQueueUrl, receiptHandles, ts2-ts1);

		return messages;
	}
	
	public static void deleteMessage(String userId, String relativeQueueUrl, String receiptHandle) throws Exception {
		long ts1 = System.currentTimeMillis();
		PersistenceFactory.getCQSMessagePersistence().deleteMessage(relativeQueueUrl, receiptHandle);
		long ts2 = System.currentTimeMillis();
		emitLogLine(userId, "DeleteMessage", relativeQueueUrl, new ArrayList<String>(Arrays.asList(receiptHandle)), ts2-ts1);
	}
	
	public static CQSQueue createQueue(String userId, String queueName, Integer visibilityTimeout, Integer messageRetentionPeriod, Integer delaySeconds, Integer receiveMessageWaitTimeSeconds, Integer numberOfPartitions, Integer numberOfShards, Boolean isCompressed, String policy) throws Exception {
		
		long ts1 = System.currentTimeMillis();
		
        if (queueName == null || queueName.length() == 0) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "New queue must have a name");
        }
        
        queueName = queueName.trim();

        if (queueName.length() > CMBProperties.getInstance().getCQSMaxNameLength()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "QueueName " + queueName + " is too long. Maximum is " + CMBProperties.getInstance().getCQSMaxNameLength());
        }

        Pattern p = Pattern.compile("[a-zA-Z0-9-_]+");
        Matcher m = p.matcher(queueName);
        
        if (!m.matches()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "QueueName " + queueName + " is invalid. Only alphanumeric and hyphen and underscore allowed.");
        }

        String relativeQueueUrl = new CQSQueue(queueName, userId).getRelativeUrl();
        CQSQueue queue = PersistenceFactory.getQueuePersistence().getQueue(relativeQueueUrl);
        
        if (queue != null) {
        	return queue;
        }
        
        queue = new CQSQueue(queueName, userId);
        
        if (visibilityTimeout != null) {
    		if (visibilityTimeout < 0 || visibilityTimeout > CMBProperties.getInstance().getCQSMaxVisibilityTimeOut()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.VISIBILITY_TIMEOUT + " should be between 0 and " + CMBProperties.getInstance().getCQSMaxVisibilityTimeOut());
    		}
    		queue.setVisibilityTO(visibilityTimeout);
        }
        
        if (policy != null) {
        	//TODO: validate policy
        	queue.setPolicy(policy);
        }
        
        if (messageRetentionPeriod != null) {
    		if (messageRetentionPeriod < 0 || messageRetentionPeriod > CMBProperties.getInstance().getCQSMaxMessageRetentionPeriod()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.MESSAGE_RETENTION_PERIOD + " should be between 0 and " + CMBProperties.getInstance().getCQSMaxMessageRetentionPeriod());
    		}
    		queue.setMsgRetentionPeriod(messageRetentionPeriod);
        }
        
        if (delaySeconds != null) {
    		if (delaySeconds < 0 || delaySeconds > CMBProperties.getInstance().getCQSMaxMessageDelaySeconds()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.DELAY_SECONDS + " should be between 0 and " + CMBProperties.getInstance().getCQSMaxMessageDelaySeconds());
    		}
    		queue.setDelaySeconds(delaySeconds);
        }
        
        if (receiveMessageWaitTimeSeconds != null) {
    		if (receiveMessageWaitTimeSeconds < 0 || receiveMessageWaitTimeSeconds > CMBProperties.getInstance().getCMBRequestTimeoutSec()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.RECEIVE_MESSAGE_WAIT_TIME_SECONDS + " should be between 0 and " + CMBProperties.getInstance().getCQSMaxMessageDelaySeconds());
    		}
    		queue.setReceiveMessageWaitTimeSeconds(receiveMessageWaitTimeSeconds);
        }
        
        if (numberOfPartitions != null) {
    		if (numberOfPartitions < 1) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.NUMBER_OF_PARTITIONS + " should be at least 1");
    		}
        	queue.setNumberOfPartitions(numberOfPartitions);
        }
        
        if (numberOfShards != null) {
    		if (numberOfShards < 1 || numberOfShards > 100) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.NUMBER_OF_SHARDS + " should be between 1 and 100");
    		}
            queue.setNumberOfShards(numberOfShards);
        }
        
        if (isCompressed != null) {
        	queue.setCompressed(isCompressed);
        }
		
		PersistenceFactory.getQueuePersistence().createQueue(queue);
		for (int shard = 0; shard < numberOfShards; shard++) {
			PersistenceFactory.getCQSMessagePersistence().checkCacheConsistency(queue.getRelativeUrl(), shard, false);
		}

		long ts2 = System.currentTimeMillis();
		
		emitLogLine(userId, "CreateQueue", relativeQueueUrl, null, ts2-ts1);

		return queue;
	}
	
	public static void deleteQueue(String userId, String relativeQueueUrl) throws Exception {
		
		long ts1 = System.currentTimeMillis();
		
    	CQSQueue queue = CQSCache.getCachedQueue(relativeQueueUrl);
    	
	    if (queue == null) {
	    	throw new CMBException(CMBErrorCodes.InternalError, "Unknown queue " + relativeQueueUrl);
	    }
	    
    	int numberOfShards = queue.getNumberOfShards();
    	PersistenceFactory.getQueuePersistence().deleteQueue(queue.getRelativeUrl());
    	
        for (int shard=0; shard<numberOfShards; shard++) {
        	PersistenceFactory.getCQSMessagePersistence().clearQueue(queue.getRelativeUrl(), shard);
        }

        long ts2 = System.currentTimeMillis();
        
		emitLogLine(userId, "DeleteQueue", relativeQueueUrl, null, ts2-ts1);
	}
	
	public static CQSQueue getQueueUrl(String userId, String queueName) throws Exception {
		
		long ts1 = System.currentTimeMillis();
		
        if (queueName == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "Missing parameter QueueName");
        }

        CQSQueue queue = PersistenceFactory.getQueuePersistence().getQueue(userId, queueName);

        if (queue == null) {
            throw new CMBException(CQSErrorCodes.NonExistentQueue, "Queue not found with name " + queueName + " for user " + userId);
        }
        
		long ts2 = System.currentTimeMillis();
		
		emitLogLine(userId, "GetQueueUrl", queue.getRelativeUrl(), null, ts2-ts1);
        
        return queue;
	}
	
	public static void changeMessageVisibility(String userId, String relativeQueueUrl, String receiptHandle, Integer visibilityTimeout) throws Exception {

		long ts1 = System.currentTimeMillis();
		
    	CQSQueue queue = CQSCache.getCachedQueue(relativeQueueUrl);
    	
	    if (queue == null) {
	    	throw new CMBException(CMBErrorCodes.InternalError, "Unknown queue " + relativeQueueUrl);
	    }
	    
        if (receiptHandle == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "ReceiptHandle not found");
        }

        if (visibilityTimeout == null) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "VisibilityTimeout not found");
        }

        if (visibilityTimeout < 0 || visibilityTimeout > CMBProperties.getInstance().getCQSMaxVisibilityTimeOut()) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, "VisibilityTimeout is limited from 0 to " + CMBProperties.getInstance().getCQSMaxVisibilityTimeOut() + " seconds");
        }
        
        PersistenceFactory.getCQSMessagePersistence().changeMessageVisibility(queue, receiptHandle, visibilityTimeout);
	    
		long ts2 = System.currentTimeMillis();
		
		emitLogLine(userId, "ChangeMessageVisibility", queue.getRelativeUrl(), null, ts2-ts1);
	}
}
