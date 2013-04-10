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
package com.comcast.cns.persistence;

import java.util.List;

import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.model.CNSTopic;

/**
 * Interface representing persistence of topic
 * @author bwolf, tina
 *
 */
public interface ICNSTopicPersistence {
	
	/**
	 * Create a new topic.
	 * @param name
	 * @param displayName
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public CNSTopic createTopic(String name, String displayName, String userId) throws Exception;
	
	/**
	 * Delete an existing topic along with all subscriptions.
	 * @param arn
	 * @throws Exception
	 */
	public void deleteTopic(String arn) throws Exception;
	
	/**
	 * Get a topic object by topic arn. Not part of official AWS API.
	 * @param arn topic arn
	 * @return topic
	 * @throws Exception
	 */
	public CNSTopic getTopic(String arn) throws Exception;

	/**
	 * List all topics for a user. Pagination for more than 100 subscriptions.
	 * @param userId user id 
	 * @param nextToken initially null, on subsequent calls arn of last result from prior call
	 * @return topic
	 * @throws Exception
	 */
	public List<CNSTopic> listTopics(String userId, String nextToken) throws Exception;
	
	/**
	 * List all topics across all users. Pagination for more than 100 subscriptions. Not part of official AWS API.
	 * @param nextToken
	 * @return
	 * @throws Exception
	 */
	public List<CNSTopic> listAllTopics(String nextToken) throws Exception;
	
	/**
	 * Update topic display name
	 * @param arn
	 * @param displayName
	 * @throws Exception
	 */
	public void updateTopicDisplayName(String arn, String displayName) throws Exception;

	/**
	 * Get number of topics by user 
	 * @param userId
	 * @return
	 * @throws PersistenceException
	 */
	public long getNumberOfTopicsByUser(String userId) throws PersistenceException;
}
