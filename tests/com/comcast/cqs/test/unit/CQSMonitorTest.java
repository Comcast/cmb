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
package com.comcast.cqs.test.unit;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.controller.CQSMonitor;
import com.comcast.cqs.controller.CQSMonitor.CacheType;

public class CQSMonitorTest {

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        CQSMonitor.Inst.clearAllState();
    }

    @Test
    public void testMessageCount() {
        CQSMonitor.Inst.addNumberOfMessagesReturned("test", 10);
        CQSMonitor.Inst.addNumberOfMessagesReturned("test", 10);
        if (CQSMonitor.Inst.getNumberOfMessagesReturned("test") != 20) {
            fail("Expected 20 messages. Got=" + CQSMonitor.Inst.getNumberOfMessagesReturned("test"));
        }
    }
    
    @Test
    public void testCacheHit() {
        CQSMonitor.Inst.registerCacheHit("test", 5, 10, CacheType.QCache);
        CQSMonitor.Inst.registerCacheHit("test", 5, 10, CacheType.QCache);
        //should be 50% hit
        if (CQSMonitor.Inst.getCacheHitPercent("test", CacheType.QCache) != 50) {
            fail("Expected 50% Got=" + CQSMonitor.Inst.getCacheHitPercent("test", CacheType.QCache));
        }
    }
    
    @Test
    public void testNumMessages() {
        CQSMonitor.Inst.addNumberOfMessagesReceived("test", 1);
        if (CQSMonitor.Inst.getNumberOfMessagesInQueue("test") != 1) {
            fail("Expected 1. Got:" + CQSMonitor.Inst.getNumberOfMessagesInQueue("test"));
        }
        CQSMonitor.Inst.addNumberOfMessagesReturned("test", 1);
        if (CQSMonitor.Inst.getNumberOfMessagesInQueue("test") != 0) {
            fail("Expected 0. Got:" + CQSMonitor.Inst.getNumberOfMessagesInQueue("test"));
        }
        
        if (CQSMonitor.Inst.getNumberOfMessagesDeleted("test") != 1) {
            fail("Expected 1. Got:" + CQSMonitor.Inst.getNumberOfMessagesDeleted("test"));
        }
    }
    
}
