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
