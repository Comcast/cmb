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
import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.HConsistencyLevel;

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
	public void testCassandraCounters() {
		
		log.info("Testing Cassandra counters");
		
		CassandraPersistence p = new CassandraPersistence(CMBProperties.getInstance().getCNSKeyspace());
		
		long i = p.getCounter("CNSTopicStats", "bla", "foo", StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getConsistencyLevel());
		
		while (i > 0) {
			p.decrementCounter("CNSTopicStats", "bla", "foo", 1, StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getConsistencyLevel());
			i = p.getCounter("CNSTopicStats", "bla", "foo", StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getConsistencyLevel());
		}
		
		p.incrementCounter("CNSTopicStats", "bla", "foo", 1, StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getConsistencyLevel());
		p.incrementCounter("CNSTopicStats", "bla", "foo", 1, StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getConsistencyLevel());
		p.incrementCounter("CNSTopicStats", "bla", "foo", 1, StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getConsistencyLevel());
		
		i = p.getCounter("CNSTopicStats", "bla", "foo", StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getConsistencyLevel());
		
		assertTrue("Expected counter to be 3, instead found " + i, i == 3);
	}
	
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
