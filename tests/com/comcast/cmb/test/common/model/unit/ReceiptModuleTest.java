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
import com.comcast.cmb.common.model.ReceiptModule;
import com.comcast.cmb.common.util.Util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: michael
 * Date: 2/8/12
 * Time: 3:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReceiptModuleTest {

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();

    }
    
    @Test
    public void testGetReceiptId() throws Exception {
        String id1 = ReceiptModule.getReceiptId();
        String id2 = ReceiptModule.getReceiptId();
        
        assertEquals(id1, id2);
        
        ReceiptModule.init();
        final String id3 = ReceiptModule.getReceiptId();
        
        assertNotSame(id1, id3);
        
        Thread t = new Thread(new Runnable() {
            public void run() {
                String id4 = ReceiptModule.getReceiptId();
                assertNotSame(id3, id4);
            }
        });
        t.start();
        
        t.join();
    }
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }

}
