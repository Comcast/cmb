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
package com.comcast.cmb.test.common.util.unit;

import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.AuthUtil;
import com.comcast.cmb.common.util.AuthenticationException;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.Util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: michael
 * Date: 2/29/12
 * Time: 6:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class AuthUtilTest {
    
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
    }
    
    @Test
    public void testPasswords() throws Exception {
        String hashedPassword = AuthUtil.hashPassword("Blah");
        assertTrue(AuthUtil.verifyPassword("Blah", hashedPassword));
        assertFalse(AuthUtil.verifyPassword("Blur", hashedPassword));
    }

    @Test
    public void testKeyAndSecret() throws Exception {
        String key = AuthUtil.generateRandomAccessKey();
        String secret = AuthUtil.generateRandomAccessSecret();
        
        assertEquals(key.length(), 20);
        assertEquals(secret.length(), 40);
    }
    
    @Test
    public void testCheckTimeStamp() throws Exception {
        
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        
        Date now = new Date();
        Date tooOld = new Date(now.getTime() - 1000000);
        Date tooNew = new Date(now.getTime() + 1000000);
        Date inRange = new Date(now.getTime() + 100000);
        
        
        AuthUtil.checkTimeStamp(df.format(now));
        AuthUtil.checkTimeStamp(df.format(inRange));
        
        try {
            AuthUtil.checkTimeStamp(df.format(tooOld));
            fail("Does not throw an exception for out of range timestamp");
        } catch (AuthenticationException ex) {
            assertEquals(ex.getCMBCode(), CMBErrorCodes.RequestExpired.getCMBCode());
        }

        try {
            AuthUtil.checkTimeStamp(df.format(tooNew));
            fail("Does not throw an exception for out of range timestamp");
        } catch (AuthenticationException ex) {
            assertEquals(ex.getCMBCode(), CMBErrorCodes.RequestExpired.getCMBCode());
        }
        
        Date expired = new Date(now.getTime() - 1000);
        Date notExpired = new Date(now.getTime() + 1000);

        AuthUtil.checkExpiration(df.format(notExpired));
        try {
            AuthUtil.checkExpiration(df.format(expired));
            fail("Does not throw an exception for expired timestamp");
        } catch (AuthenticationException ex) {
            assertEquals(ex.getCMBCode(), CMBErrorCodes.RequestExpired.getCMBCode());
        }
    }
}
