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
package com.comcast.cns.test.unit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import com.comcast.plaxo.cmb.common.util.Util;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.persistence.CassandraPersistence;
import com.comcast.plaxo.cmb.common.util.CMBProperties;

public class CassandraTest {

    private static Logger log = Logger.getLogger(CassandraTest.class);
    private static AtomicInteger counter = new AtomicInteger(0);

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
    }

	@Test	
	public void testCassandraCounters() {
		
		try {
			
			CassandraPersistence p = new CassandraPersistence(CMBProperties.getInstance().getCMBCNSKeyspace());
			
			long i = p.getCounter("CNSTopicStats", "bla", "foo", StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
			
			while (i > 0) {
				p.decrementCounter("CNSTopicStats", "bla", "foo", 1, StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
				i = p.getCounter("CNSTopicStats", "bla", "foo", StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
			}
			
			p.incrementCounter("CNSTopicStats", "bla", "foo", 1, StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
			p.incrementCounter("CNSTopicStats", "bla", "foo", 1, StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
			p.incrementCounter("CNSTopicStats", "bla", "foo", 1, StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
			
			i = p.getCounter("CNSTopicStats", "bla", "foo", StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
			
			assertTrue(i == 3);

		} catch (Exception ex) {
            fail("Excetpion="+ex);
			log.error("Excetpion="+ex, ex);
		}
	}

    private class TimeUUIDGenerator implements Runnable {
    	
    	CassandraPersistence p;
    	
    	public TimeUUIDGenerator(CassandraPersistence p) {
    		this.p = p;
    	}
 
		@Override
		public void run() {
			while (true) {
				try {
					p.getTimeUUID(System.currentTimeMillis());
				} catch (InterruptedException ex) {
					break;
				}
				counter.incrementAndGet();
			}
		}
    }	

    private class LongUUIDGenerator implements Runnable {
    	
    	CassandraPersistence p;
    	
    	public LongUUIDGenerator(CassandraPersistence p) {
    		this.p = p;
    	}
 
		@Override
		public void run() {
			while (!Thread.interrupted()) {
				try {
					p.getTimeLong(System.currentTimeMillis());
				} catch (InterruptedException ex) {
					break;
				}
				counter.incrementAndGet();
			}
		}
    }	

    /*@Test
	public void testTimeUUIDGenerationThroughput() throws InterruptedException {
    	
		CassandraPersistence p = new CassandraPersistence(CMBProperties.getInstance().getCMBCNSKeyspace());

		counter.set(0);
		ExecutorService executorService = Executors.newFixedThreadPool(30);
		
		for (int i=0; i<30; i ++) {
			executorService.execute(new TimeUUIDGenerator(p));
		}
    	
    	Thread.currentThread().sleep(2000);
    	executorService.shutdown();

    	log.info("time uuid message count: " + counter.get());
    	
		counter.set(0);
		executorService = Executors.newFixedThreadPool(30);
		
		for (int i=0; i<30; i ++) {
			executorService.execute(new LongUUIDGenerator(p));
		}
    	
    	Thread.currentThread().sleep(2000);
    	executorService.shutdown();

    	log.info("long uuid message count: " + counter.get());
    }*/
    
    @Test
    public void testValidJSON() {
    	
    	String valid1 = "{}";
    	String valid2 = "{ \"Version\":\"2008-10-17\", \"Id\":\"cd3ad3d9-2776-4ef1-a904-4c229d1642ee\", \"Statement\" : [ { \"Sid\":\"1\", \"Effect\":\"Allow\", \"Principal\" : { \"aws\": \"111122223333\" }, \"Action\":[\"cqs:SendMessage\",\"cqs:ReceiveMessage\"], \"Resource\": \"arn:aws:cqs:us-east-1:444455556666:queue2\", \"Condition\" : { \"IpAddress\" : { \"aws:SourceIp\":\"10.52.176.0/24\" }, \"DateLessThan\" : { \"aws:CurrentTime\":\"2009-06-30T12:00Z\" }}   } ]}";

		JSONObject o;

		try {
			o = new JSONObject(valid1);
			o = new JSONObject(valid2);
		} catch (Exception ex) {
			fail();
    	}

    	String invalid1 = "{ \"Version\":\"2008-10-17, \"Id\":\"cd3ad3d9-2776-4ef1-a904-4c229d1642ee\", \"Statement\" : [ { \"Sid\":\"1\", \"Effect\":\"Allow\", \"Principal\" : { \"aws\": \"111122223333\" }, \"Action\":[\"cqs:SendMessage\",\"cqs:ReceiveMessage\"], \"Resource\": \"arn:aws:cqs:us-east-1:444455556666:queue2\", \"Condition\" : { \"IpAddress\" : { \"aws:SourceIp\":\"10.52.176.0/24\" }, \"DateLessThan\" : { \"aws:CurrentTime\":\"2009-06-30T12:00Z\" }}   } ]}";
    	String invalid2 = "{ \"Version\":\"2008-10-17\", \"Id\":\"cd3ad3d9-2776-4ef1-a904-4c229d1642ee\", \"Statement\" : [  \"Sid\":\"1\", \"Effect\":\"Allow\", \"Principal\" : { \"aws\": \"111122223333\" }, \"Action\":[\"cqs:SendMessage\",\"cqs:ReceiveMessage\"], \"Resource\": \"arn:aws:cqs:us-east-1:444455556666:queue2\", \"Condition\" : { \"IpAddress\" : { \"aws:SourceIp\":\"10.52.176.0/24\" }, \"DateLessThan\" : { \"aws:CurrentTime\":\"2009-06-30T12:00Z\" }}   } ]}";
    	String invalid3 = "{ \"Version\":\"2008-10-17\", \"Id\":\"cd3ad3d9-2776-4ef1-a904-4c229d1642ee\", \"Statement\" :  { \"Sid\":\"1\", \"Effect\":\"Allow\", \"Principal\" : { \"aws\": \"111122223333\" }, \"Action\":[\"cqs:SendMessage\",\"cqs:ReceiveMessage\"], \"Resource\": \"arn:aws:cqs:us-east-1:444455556666:queue2\", \"Condition\" : { \"IpAddress\" : { \"aws:SourceIp\":\"10.52.176.0/24\" }, \"DateLessThan\" : { \"aws:CurrentTime\":\"2009-06-30T12:00Z\" }}   } ]}";

    	try {
			o = new JSONObject(invalid1);
			fail();
		} catch (Exception ex) {
    	}

    	try {
			o = new JSONObject(invalid2);
			fail();
		} catch (Exception ex) {
    	}
    
		try {
			o = new JSONObject(invalid3);
			fail();
		} catch (Exception ex) {
		}
    }
	
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
