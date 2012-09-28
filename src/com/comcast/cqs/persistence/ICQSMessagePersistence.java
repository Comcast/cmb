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

import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;

/**
 * Interface to persist cqs message
 * @author baosen, aseem, vvenkatraman, bwolf
 */
public interface ICQSMessagePersistence {
    /**
     * Create a message
     * @param queue The CQS queue to post the message
     * @param message An instance of CQSMessage
     * @throws PersistenceException
     * @throws IOException 
     * @throws InterruptedException 
     * @return the message-=id
     * @throws NoSuchAlgorithmException 
     */
    public String sendMessage(CQSQueue queue, CQSMessage message) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException;

    /**
     * Create a batch of message
     * Note: THe provided messages are modified with a new messageId
     * @param queue The CQS queue to post the messages
     * @param messages A list of CQSMessage instances
     * @throws PersistenceException
     * @throws IOException 
     * @throws InterruptedException
     * @return mapping from client-provided-message-id to internal message-id 
     * @throws NoSuchAlgorithmException 
     */
    public Map<String, String> sendMessageBatch(CQSQueue queue, List<CQSMessage> messages) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException;

    /**
     * Delete a message given the receiptHandle
     * @param receiptHandle The receipt handle for the message.  This is the time based UUID.
     * @throws PersistenceException
     */
    public void deleteMessage(String queueUrl, String receiptHandle) throws PersistenceException;
    
    /**
     * Receive the next set of messages from the Queue
     * @param queue The queue which contains the messages.
     * @param receiveAttributes The set of attributes for the message, like the new visibility timeout and others.
     * @throws PersistenceException
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     * @throws InterruptedException 
     * @return list of messages
     */
    public List<CQSMessage> receiveMessage(CQSQueue queue, Map<String, String> receiveAttributes) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException;

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
     */
    public List<CQSMessage> peekQueue(String queueUrl,  String previousReceiptHandle, String nextReceiptHandle,
			int length) throws PersistenceException, IOException, NoSuchAlgorithmException;
    
    /**
     * Peek the queue with the given Queue URL for the next set of messages chosen at random from the queue
     * @param queueUrl The URL of the Queue
     * @param length the number of messages to return 
     * @throws PersistenceException
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     * @return list of messages
     */
    public List<CQSMessage> peekQueueRandom(String queueUrl, int length) throws PersistenceException, IOException, NoSuchAlgorithmException;
    

    /**
     * Peek the queue with the given Queue URL for the next set of messages
     * @param queueUrl The URL of the Queue
     * @param previousReceiptHandle The receipt handle of the last item in the previous page
     * @param nextReceiptHandle The receipt handle of the first item in the next page
     * @throws PersistenceException
     * @throws UnsupportedEncodingException 
     * @throws NoSuchAlgorithmException 
     */
    public void clearQueue(String queueUrl) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException;
    
    /**
     * 
     * @param ids The list of message-ids
     * @return The list of messages with those ids in any order
     * @throws UnsupportedEncodingException 
     * @throws NoSuchAlgorithmException 
     */
	Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids)
			throws PersistenceException, NoSuchAlgorithmException,
			UnsupportedEncodingException;
}
