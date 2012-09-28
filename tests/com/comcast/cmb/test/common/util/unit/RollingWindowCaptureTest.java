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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.RollingWindowCapture;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.common.util.RollingWindowCapture.PayLoad;
import com.comcast.cmb.common.util.RollingWindowCapture.Visitor;

public class RollingWindowCaptureTest {
    static Logger logger = Logger.getLogger(RollingWindowCaptureTest.class);

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
    }

    class SimplePayload extends PayLoad {
    }
    
    class CountingVisitor implements Visitor<SimplePayload> {
        volatile int count=0;
        @Override
        public void processNode(SimplePayload n) {
            count++;
        }
        
    }
    @Test
    public void test() throws InterruptedException {
        RollingWindowCapture<SimplePayload> c = new RollingWindowCapture<SimplePayload>(1,10);
        c.addNow(new SimplePayload());
        CountingVisitor countingV = new CountingVisitor();
        c.visitAllNodes(countingV);
        if (countingV.count != 1) {
            fail("Expected to find count 1. Got:" + countingV.count);
        }
        
        Thread.sleep(1001);
        countingV = new CountingVisitor();
        c.visitAllNodes(countingV);
        if (countingV.count != 0) {
            fail("Expected to find count 0. Got:" + countingV.count);
        }
        for (int i = 0; i < 100; i++) {
            c.addNow(new SimplePayload());
        }
        countingV = new CountingVisitor();
        c.visitAllNodes(countingV);
        if (countingV.count != 100) {
            fail("Expected to find count 0. Got:" + countingV.count);
        }
    }
    

    @Test
    public void testRWThroughput() throws Exception {
        ExecutorService e = Executors.newFixedThreadPool(5);
        final AtomicInteger in = new AtomicInteger();
        final RollingWindowCapture<SimplePayload> c = new RollingWindowCapture<SimplePayload>(10,1000); //10 second capture
        final Random r = new Random();
        
        Future []jobs = new Future[5];
        for (int i = 0; i < 5; i++) {
            jobs[i] = e.submit(new Runnable() {
                
                @Override
                public void run() {
                    while(!Thread.interrupted()) {
                        c.addNow(new SimplePayload());
                        in.incrementAndGet();
                        if (r.nextInt() % 97 == 0) {
                            c.visitAllNodes(new CountingVisitor());
                        }
                    }
                }
            });
        }
        
        Thread.sleep(10000);
        e.shutdown();
        for (int i = 0; i < jobs.length; i++) {
            jobs[i].cancel(true);
        }
        logger.info("cancelled");
        e.awaitTermination(2, TimeUnit.SECONDS);
        logger.info("throughput=" + in.get()/10 + " additions per second");        
    }
}
