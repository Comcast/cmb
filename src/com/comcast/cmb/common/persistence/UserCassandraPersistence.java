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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.AuthUtil;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.util.CQSErrorCodes;

import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.query.QueryResult;


import org.apache.log4j.Logger;


/**
 * Represents Cassandra persistence functionality of User Objects
 * @author bwolf, vvenkatraman, baosen, michael, aseem
 */
public class UserCassandraPersistence extends CassandraPersistence implements IUserPersistence {

	private static final String ACCESS_SECRET = "accessSecret";
	private static final String USER_ID = "userId";
	private static final String HASH_PASSWORD = "hashPassword";
	private static final String USER_NAME = "userName";
	private final ColumnFamilyTemplate<String, String> usersTemplate;
	private static final String COLUMN_FAMILY_USERS = "Users";
	private static final Logger logger = Logger.getLogger(UserCassandraPersistence.class);
	
	public UserCassandraPersistence() {
		super(CMBProperties.getInstance().getCMBKeyspace());		
		usersTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(HConsistencyLevel.QUORUM), COLUMN_FAMILY_USERS, StringSerializer.get(), StringSerializer.get());
	}

	@Override
	public User createUser(String userName, String password) throws PersistenceException {
		
		User user = null;
		
		if (userName == null || userName.length() < 0 || userName.length() > 25) {
			logger.error("event=create_user error_code=invalid_user_name user_name=" + userName);
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, "Invalid user name " + userName);
		}
		
		if (password == null || password.length() < 0 || password.length() > 25) {
			logger.error("event=create_user error_code=invalid_password");
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, "Invalid password");
		}

		if (getUserByName(userName) != null) {
			logger.error("event=create_user error_code=user_already_exists user_name=" + userName);
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, "User with user name " + userName + " already exists");
		}
		
		try {

			String userId = Long.toString(System.currentTimeMillis()).substring(1);
            
            String hashedPassword = AuthUtil.hashPassword(password);
            String accessSecret = AuthUtil.generateRandomAccessSecret();
            String accessKey = AuthUtil.generateRandomAccessKey();

			user = new User(userId, userName, hashedPassword, accessKey, accessSecret);
			
			Map<String, String> userDataMap = new HashMap<String, String>();
			
			userDataMap.put(USER_ID, user.getUserId());
			userDataMap.put(USER_NAME, user.getUserName());
			userDataMap.put(HASH_PASSWORD, user.getHashPassword());
			userDataMap.put(ACCESS_SECRET, user.getAccessSecret());		
			
			insertOrUpdateRow(user.getAccessKey(), COLUMN_FAMILY_USERS, userDataMap, HConsistencyLevel.QUORUM);
			
		} catch (Exception e) {
			logger.error("event=create_user", e);
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, e.getMessage());
		}
		
		return user;
	}
	
	@Override
	public void deleteUser(String userName) throws PersistenceException {
		
		User user = getUserByName(userName);
		
		if (user == null) {
			logger.error("event=delete_user error_code=user_does_not_exist user_name=" + userName);
			throw new PersistenceException (CQSErrorCodes.InvalidRequest, "No user with the user name " + userName + " exists");
		}
		
		super.delete(usersTemplate, user.getAccessKey(), null);
	}
	
	@Override
	public long getNumUserQueues(String userId) throws PersistenceException {
		return PersistenceFactory.getQueuePersistence().getNumberOfQueuesByUser(userId);
	}
		
	@Override
	public long getNumUserTopics(String userId) throws PersistenceException {
		return PersistenceFactory.getTopicPersistence().getNumberOfTopicsByUser(userId);
	}

	@Override
	public List<User> getAllUsers() throws PersistenceException {
		String query = "SELECT * from Users";
		return getUsers(query);
	}

	@Override
	public User getUserById(String userId) throws PersistenceException {
		String query = "SELECT * from Users where " + USER_ID + "='" + userId +"'";
		return getUser(query);
	}

	@Override
	public User getUserByName(String userName) throws PersistenceException {
		String query = "SELECT * from Users where " + USER_NAME + "='" + userName +"'";
		return getUser(query);
	}

	private User getUser(String query) throws PersistenceException {

		QueryResult<CqlRows<String,String,String>> res = super.readRows(query, HConsistencyLevel.QUORUM);
		
		if (res == null) {
			return null;
		}
		
		CqlRows<String,String,String> rows = res.get();
		
		if (rows == null || rows.getCount() == 0) {
			return null;
		} 
		
		/*else if (rows.getCount() > 1) {
			logger.error("event=read_user query=" + query);
			throw new PersistenceException(CQSErrorCodes.InvalidQueryParameter, "Failed to read user");
		}*/

		me.prettyprint.hector.api.beans.Row<String, String, String> row = rows.getList().get(0);

		return fillUserFromCqlRow(row);
	}
	
	private List<User> getUsers(String query) throws PersistenceException {
		
		List<User> userList = new ArrayList<User>();
		QueryResult<CqlRows<String,String,String>> res = super.readRows(query, HConsistencyLevel.QUORUM);
		
		if (res == null) {
			return userList;
		}
		
		CqlRows<String,String,String> rows = res.get();

		if (rows == null || rows.getCount() == 0) {
			return userList;
		}
		
		for (me.prettyprint.hector.api.beans.Row<String, String, String> row : rows) {
		
			User user = fillUserFromCqlRow(row);
			
			if (user != null) {
				userList.add(user);
			}
		}
		
		return userList;
	}

	private User fillUserFromCqlRow(me.prettyprint.hector.api.beans.Row<String, String, String> row) {
		
		String accessKey = row.getKey();
		ColumnSlice<String, String> columnSlice = row.getColumnSlice();
		
		if (columnSlice == null || columnSlice.getColumns() == null || columnSlice.getColumns().size() <= 1) {
			return null;
		}
		
		String userId = columnSlice.getColumnByName(USER_ID).getValue();
		String userName = columnSlice.getColumnByName(USER_NAME).getValue();
		String hashPassword = columnSlice.getColumnByName(HASH_PASSWORD).getValue();
		String accessSecret = columnSlice.getColumnByName(ACCESS_SECRET).getValue();

		return new User(userId, userName, hashPassword, accessKey, accessSecret);
	}

	@Override
	public User getUserByAccessKey(String accessKey) throws PersistenceException {

		Row<String, String, String> result = super.readRow(COLUMN_FAMILY_USERS, accessKey, 10, StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
		
		if (result == null) {
			return null;
		}
		
		return fillUserFromCqlRow(result);
	}
}

