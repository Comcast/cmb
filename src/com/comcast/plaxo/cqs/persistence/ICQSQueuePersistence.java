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

import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cqs.model.CQSQueue;

import java.util.List;
import java.util.Map;

/**
 * Interface to persist queues
 * @author baosen, vvenkatraman, jorge
 */
public interface ICQSQueuePersistence {

	/**
	 * Create a queue
	 * 
	 * @param queue
	 * @throws PersistenceException
	 */
	public void createQueue(CQSQueue queue) throws PersistenceException;

	/**
	 * Delete a queue given the queueUrl
	 * 
	 * @param queueUrl
	 * @throws PersistenceException
	 */
	public void deleteQueue(String queueUrl) throws PersistenceException;

	/**
	 * List all the queue for a given user. All queues are returned if prefix is
	 * null
	 * 
	 * @param userId
	 * @param queueName_prefix
	 * @return
	 * @throws PersistenceException
	 */
	public List<CQSQueue> listQueues(String userId, String queueName_prefix)
			throws PersistenceException;

	/**
	 * Update the queue
	 * 
	 * @param queue
	 */
	public void updateQueueAttribute(String queueURL, Map<String, String> queueData)
			throws PersistenceException;

	/**
	 * Get a queue given user ID and queueName
	 * 
	 * @param userId
	 * @param queueName
	 * @return
	 */
	public CQSQueue getQueue(String userId, String queueName);

	/**
	 * Get a queue given queue url
	 * 
	 * @param queueUrl
	 *            The URL of the queue to get.
	 * @return
	 */
	public CQSQueue getQueue(String queueUrl);

	/**
	 * Update the policy for a given queue
	 * 
	 * @param queueUrl
	 *            The URL of the queue to get.
	 * @param policy
	 *            The updated policy for the queue
	 * @return
	 */
	public boolean updatePolicy(String queueUrl, String policy);
}
