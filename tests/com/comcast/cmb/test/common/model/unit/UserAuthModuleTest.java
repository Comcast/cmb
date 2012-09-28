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
package com.comcast.cmb.test.common.model.unit;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.IAuthModule;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.model.UserAuthModule;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.AuthenticationException;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.Util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: michael
 * Date: 3/6/12
 * Time: 4:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserAuthModuleTest {
	
    IAuthModule authModule;
    
    @Before
    public void setUp() throws Exception {
        Util.initLog4jTest();

        IUserPersistence userHandler = new UserCassandraPersistence();
        
        User user = userHandler.getUserByName("cmb_unit_test");

        if (user == null) {
            user = userHandler.createUser("cmb_unit_test", "cmb_unit_test");
        }

        authModule = new UserAuthModule();
        authModule.setUserPersistence(userHandler);
        CMBControllerServlet.valueAccumulator.initializeAllCounters();

    }

    @Test
    public void testAuthModule() throws Exception {
    	
    	User user = authModule.authenticateByPassword("cmb_unit_test", "cmb_unit_test");
        assertNotNull(user);

        try {
            user = authModule.authenticateByPassword("cmb_unit_test", "blahblah");
            fail("wrong password ... should throw an exception");
        } catch (AuthenticationException ex) {
            assertEquals(ex.getCMBCode(), CMBErrorCodes.AuthFailure.getCMBCode());
        }
    }
    
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
