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
package com.comcast.cmb.common.persistence;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.PersistenceException;

import java.util.List;

/**
 * interface represents the persistence functionality for a User object 
 * @author bwolf, vvenkatraman, michael
 */
public interface IUserPersistence {

    /**
     * Create a user given username and password
     * @param userName
     * @param password
     * @return User object if successful
     */
    public User createUser(String userName, String password) throws PersistenceException;

    /**
     * Delete a user given an user name
     * @param userName
     */
    public void deleteUser(String userName) throws PersistenceException;

    /**
     * Get all users
     * @return all users in a list
     * @throws PersistenceException 
     */
    public List<User> getAllUsers() throws PersistenceException;
    
    public User getUserById(String userId) throws PersistenceException;
    public User getUserByName(String userName) throws PersistenceException;
    public User getUserByAccessKey(String accessKey) throws PersistenceException;

	/**
	 * Gte number of queues this user owns
	 * @param userId
	 * @return
	 * @throws PersistenceException 
	 */
    public long getNumUserQueues(String userId) throws PersistenceException;

	/**
	 * Gte number of topics this user owns
	 * @param userId
	 * @return
	 * @throws PersistenceException 
	 */
    public long getNumUserTopics(String userId) throws PersistenceException;
}
