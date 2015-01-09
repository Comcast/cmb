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

import static org.junit.Assert.assertTrue;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.Util;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraTest {

    private static Logger log = Logger.getLogger(CassandraTest.class);

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
    }

	@Test	
	public void testCassandraCounters() throws PersistenceException {
		
		log.info("Testing Cassandra counters");
		
		AbstractDurablePersistence cassandraHandler = DurablePersistenceFactory.getInstance();
		
		long i = cassandraHandler.getCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", "bla2", "foo2", CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		
		while (i > 0) {
			cassandraHandler.decrementCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", "bla2", "foo2", 1, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
			i = cassandraHandler.getCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", "bla2", "foo2", CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		}
		
		cassandraHandler.incrementCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", "bla2", "foo2", 1, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		cassandraHandler.incrementCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", "bla2", "foo2", 1, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		cassandraHandler.incrementCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", "bla2", "foo2", 1, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		
		i = cassandraHandler.getCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", "bla2", "foo2", CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		
		assertTrue("Expected counter to be 3, instead found " + i, i == 3);
	}
	
	@Test	
	/*public void testCassandraCounterBug() throws PersistenceException {
		// this test is expected to fail on C* > 2.0.6
		String rowKey = "arn:cmb:cns:csv:420754986205:foo2";
		AbstractDurablePersistence cassandraHandler = DurablePersistenceFactory.getInstance();
		cassandraHandler.deleteCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", rowKey, "subscriptionConfirmed", CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		cassandraHandler.incrementCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", rowKey, "subscriptionConfirmed", 1, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		long i = cassandraHandler.getCounter(CMBProperties.getInstance().getCNSKeyspace(), "CNSTopicStats", rowKey, "subscriptionConfirmed", CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		assertTrue("Expected counter to be 1, instead found " + i, i == 1);
	}*/	
	
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
