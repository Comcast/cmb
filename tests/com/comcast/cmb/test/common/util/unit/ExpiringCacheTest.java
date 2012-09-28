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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.ExpiringCache;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.common.util.ExpiringCache.CacheFullException;

public class ExpiringCacheTest {

    private static Logger log = Logger.getLogger(ExpiringCacheTest.class);
    
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();

    }

    static class TestCallable implements Callable<Integer> {
       
    	volatile int numTimesCalled = 0;
        
        @Override
        public Integer call() throws Exception {
            log.debug("getting called");
            numTimesCalled++;
            return 1;
        }        
    }
    
    @Test
    public void testSetGetCache() throws Exception {
    	
        ExpiringCache<String, Integer> cache = new ExpiringCache<String, Integer>(1000);
        TestCallable c = new TestCallable();
        
        Integer i = cache.getAndSetIfNotPresent("test", c, 1000);
        if (i != 1) {
            fail("Expected 1 got:" + i);
        }
        if (c.numTimesCalled != 1) {
            fail("Expected to have called the callable once. got num=" + c.numTimesCalled);
        }
        
        i = cache.getAndSetIfNotPresent("test", c, 1000);
        if (c.numTimesCalled != 1) {
            fail("Expected to have called the callable once. got num=" + c.numTimesCalled);
        }
        
        //check expiration
        Thread.sleep(1001);
        i = cache.getAndSetIfNotPresent("test", c, 1000);
        if (c.numTimesCalled != 2) {
            fail("Expected to have called the callable 2. got num=" + c.numTimesCalled);
        }        
        
    }
    
    class TestRunnable implements Runnable {
    	
        final ExpiringCache<Integer, Integer> cache;
        final TestCallable c;
        final boolean sleep;
        public volatile boolean done = false;
        public volatile int count = 0;
        
        public TestRunnable(ExpiringCache<Integer, Integer> cache, TestCallable c, boolean sleep) {
            this.cache = cache;
            this.c = c;
            this.sleep = sleep;
        }
        
        @Override
        public void run() {
        	
        	log.info("checking thread-state");
        	
            while(!Thread.interrupted() && !done) {

                try {
                    cache.getAndSetIfNotPresent(1, c, 1000);
                    count++;
                } catch (CacheFullException e1) {
                    log.error("Cache too small");
                }

                if (sleep) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        log.error("Could not sleep thread");
                    }
                }
            }
        }        
    }
    
    @Test
    public void testConcurrentCacheGetsSets() throws Exception {
    	
        ExecutorService e = Executors.newFixedThreadPool(10);
        final TestCallable c = new TestCallable();
        final ExpiringCache<Integer, Integer> cache = new ExpiringCache<Integer, Integer>(1000);

        TestRunnable arr[] = new TestRunnable[10];

        for (int i = 0; i < 10; i++) {
            arr[i] = new TestRunnable(cache, c, true);
        	e.execute(arr[i]);
        }
        
        Thread.sleep(10000); 
        
        for (TestRunnable r : arr) {
        	r.done = true;
        }
        
        e.shutdown();
        e.awaitTermination(2, TimeUnit.SECONDS);
        
        //check that there were only 10 calls to TestCallable in 10 seconds.
        if (c.numTimesCalled > 12) {
            fail("Expected to only have 10 calls in 10 seconds. Got:" + c.numTimesCalled);
        }
    }       
    
    @Test
    public void testContains() throws Exception {
    	
        final ExpiringCache<Integer, Integer> cache = new ExpiringCache<Integer, Integer>(1000);
        final TestCallable c = new TestCallable();
        cache.getAndSetIfNotPresent(1, c, 100);
        
        if (!cache.containsKey(1)) {
            fail("Expected to find key 1 in cache. DIdn't get it");
        }
        
        Thread.sleep(101);
        
        if (cache.containsKey(1)) {
            fail("Expected to Not find key 1 in cache after 100 ms. DId get it");
        }
    }
    
    @Test
    public void test() throws Exception {
        ConcurrentHashMap<String, String> c = new ConcurrentHashMap<String, String>();
        c.put("a", "a");
        c.replace("a", "a", "b");
    }
    
    @Test
    public void testThroughput() throws Exception {
    	
        ExecutorService e = Executors.newFixedThreadPool(10);
        final TestCallable c = new TestCallable();
        final ExpiringCache<Integer, Integer> cache = new ExpiringCache<Integer, Integer>(1000);
        long ts1 = System.currentTimeMillis();
        TestRunnable arr[] = new TestRunnable[10];
        
        for (int i = 0; i < 10; i++) {
            arr[i] = new TestRunnable(cache, c, false);
            e.execute(arr[i]);
        }
        
        Thread.sleep(10000); 
        
        for (TestRunnable r : arr) {
        	r.done = true;
        }
        
        e.shutdown();
        e.awaitTermination(2, TimeUnit.SECONDS);
        long ts2 = System.currentTimeMillis();
        long numCalled = 0;

        for (int i = 0; i < 10; i++) {
            numCalled += arr[i].count;
        }
        
        System.out.println("throughput=" + ((numCalled / (ts2-ts1))*1000) + "gets/sec"  ) ;
    }
    
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
