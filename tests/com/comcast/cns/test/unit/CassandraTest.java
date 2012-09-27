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

import com.comcast.plaxo.cmb.common.util.Util;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.persistence.CassandraPersistence;
import com.comcast.plaxo.cmb.common.util.CMBProperties;

public class CassandraTest {

    private static Logger log = Logger.getLogger(CassandraTest.class);

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
	
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
