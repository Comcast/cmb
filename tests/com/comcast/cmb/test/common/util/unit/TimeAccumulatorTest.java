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

import static org.junit.Assert.*;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.common.util.ValueAccumulator;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;

public class TimeAccumulatorTest {

    private static Logger log = Logger.getLogger(TimeAccumulatorTest.class);
    
    
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();

    }

    @Test
    public void testAccumulator() {
        ValueAccumulator ac = new ValueAccumulator();
        ac.initializeAllCounters();
        ac.addToCounter(AccumulatorName.UnitTestTime, 10L);
        long val = ac.getCounter(AccumulatorName.UnitTestTime);
        if (val != 10L) {
            fail("should have gotten 10 back. Got:" + val);
        }
    }

    volatile boolean fail = false;
    Random r = new Random();
    @Test
    public void testAccumulatorConcurrent() throws Exception {
        final ValueAccumulator ac = new ValueAccumulator();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        Future []jobs = new Future[10];
        for (int i = 0; i <10; i++) {
            jobs[i] = executor.submit(new Runnable() {                
                @Override
                public void run() {
                    ac.initializeAllCounters();
                    long val = 0;
                    while (!Thread.interrupted()) {
                        ac.addToCounter(AccumulatorName.UnitTestTime, 1);
                        long newVal = ac.getCounter(AccumulatorName.UnitTestTime);
                        if (newVal != val + 1) {
                            log.error("Should have incremented by 1. Expoected val:" + (val + 1) + " instead got:" + newVal);
                            fail = true;
                            fail("Should have incremented by 1. Expoected val:" + (val + 1) + " instead got:" + newVal);
                        }
                        val = newVal;
                        try {
                            Thread.sleep(r.nextInt(100));
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                    log.info("done");
                }
            });            
        }
        Thread.sleep(3000);
        executor.shutdown();
        log.info("cancelling");
        for(int i = 0; i < jobs.length; i++) {
            if (!jobs[i].cancel(true)) {
                log.info("Could not cancel");
            }
        }
        executor.awaitTermination(10, TimeUnit.SECONDS);
        if (fail) {
            fail("failed");
        }
    }
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }

}
