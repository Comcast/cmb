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
package com.comcast.cmb.test.unit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.Util;

import org.apache.log4j.Logger;
import org.junit.* ;


import static org.junit.Assert.*;

public class UserPersistenceTest {

    protected static Logger logger = Logger.getLogger(UserPersistenceTest.class);


    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
    }

	@Test
	public void testCreateDeleteUser() {

		try {
			IUserPersistence persistence = new UserCassandraPersistence();	
			User user = createUser(persistence, "vvenkatraman1234", "venu1234");
			
			persistence.deleteUser(user.getUserName());
			verifyUserCleanup((new ArrayList<User>(Arrays.asList(user))), persistence);
            logger.info("testCreateDeleteUser succeeded");

		} catch (Exception ex) {
            logger.error("test failed", ex);
			fail("Test failed: " + ex.toString());
		}		   
	}
	
	@Test
	public void testGetAllUsers() {
		try {
			IUserPersistence persistence = new UserCassandraPersistence();	
			
            List<User> users = new LinkedList<User>();
            User user = createUser(persistence, "vvenkatraman1234", "venu1234");
            users.add(user);
            user = createUser(persistence, "bcheng1234", "baosen1234");
            users.add(user);
			user = createUser(persistence, "mchiang1234", "mchiang1234");
            users.add(user);
			
			List<User> userList = persistence.getAllUsers();
			assertTrue(userList != null && userList.size() >= 3);
			
			for (User user1 : users) {
				persistence.deleteUser(user1.getUserName());
			}
			
			userList = persistence.getAllUsers();
			verifyUserCleanup(users, persistence);

            logger.info("testGetAllUsers succeeded");
			

		} catch (Exception ex) {
            logger.error("test failed", ex);
			fail("Test failed: " + ex.toString());
		}		   
	}

	public User createUser(IUserPersistence persistence, String userName, String password)
			throws PersistenceException, InterruptedException {
		User user = persistence.getUserByName(userName);
		if (user != null) {
			persistence.deleteUser(userName);
		}
		user = persistence.createUser(userName, password);
		assertUser(userName, user);
		Thread.sleep(10);
		return user;
	}
	
	@Test
	public void testCreateAdmin(){
		try{
			String ADMIN_NAME = "cns_admin";
			String ADMIN_PASSWORD = "cns_admin";
			IUserPersistence persistence = new UserCassandraPersistence();	
			User user = persistence.getUserByName(ADMIN_NAME);
			if (user != null) {
				persistence.deleteUser(ADMIN_NAME);
			}
			user = persistence.createUser(ADMIN_NAME, ADMIN_PASSWORD, true);
			assertAdmin(ADMIN_NAME, user);
			persistence.deleteUser(ADMIN_NAME);
		}catch(Exception ex){
			logger.error("test failed", ex);
			fail("Test failed: " + ex.toString());
		}
	}

	private void assertUser(String userName, User user) {
		assertNotNull(user);
		assertTrue(user.getUserName().equals(userName));
		assertNotNull(user.getAccessKey());
		assertNotNull(user.getAccessSecret());
		assertNotNull(user.getHashPassword());
		assertNotNull(user.getUserId());
	}
	
	private void assertAdmin(String userName, User user) {
		assertUser(userName, user);
		assertTrue(user.getIsAdmin());
	}

	private void verifyUserCleanup(List<User> users, IUserPersistence persistence) throws PersistenceException {
		List<User> userList = persistence.getAllUsers();
		for (User user1 : users) {
			if (checkIfUserExists(userList, user1)) {
				fail("Test failed: User with user name " + user1.getUserName() + " still exists in the system");
			}
		}
	}

	private boolean checkIfUserExists(List<User> userList, User user1) {
		for (User userListUser : userList) {
			if (user1.getUserName().equals(userListUser.getUserName())) {
				// match found
				return true;
			}
		}
		
		return false;
	}
	
	@Test
	public void testgetUserByIdNameAndKey() throws Exception {
		IUserPersistence persistence = new UserCassandraPersistence();			
		User user;
		try {
		    List<User> users = new LinkedList<User>();
			user = createUser(persistence, "vvenkatraman1234", "venu1234");
			users.add(user);
			user = createUser(persistence, "bcheng1234", "boasen1234");
			users.add(user);
			String accessKey = user.getAccessKey();
			user = persistence.createUser("mchiang1234", "mchiang1234");
			users.add(user);
			String userId = user.getUserId();
			
			String userName = "vvenkatraman1234";
			user = persistence.getUserByName(userName);
			assertUser(userName, user);
			
			user = persistence.getUserById(userId);
			assertUser("mchiang1234", user);
			assertTrue(user.getUserId().equals(userId));
			
			user = persistence.getUserByAccessKey(accessKey);
			assertUser("bcheng1234", user);
			assertTrue(user.getAccessKey().equals(accessKey));
			
			
			for (User u : users) {
			    persistence.deleteUser(u.getUserName());
			}
			
			verifyUserCleanup(users, persistence);

            logger.info("testgetUserByIdNameAndKey succeeded");
		} catch (PersistenceException ex) {
            logger.error("test failed", ex);
			fail("Test failed: " + ex.toString());
		}
		
	}
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }

}