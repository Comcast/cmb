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

public class RedisSortedSetPersistence implements ICQSMessagePersistence, ICQSMessagePersistenceIdSequence {
	
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
