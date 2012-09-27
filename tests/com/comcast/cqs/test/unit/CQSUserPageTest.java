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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.comcast.cmb.test.tools.SimpleHttpServletRequest;
import com.comcast.cmb.test.tools.SimpleHttpServletResponse;
import com.comcast.plaxo.cmb.common.util.Util;
import org.apache.log4j.Logger;
import org.junit.*;

import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.plaxo.cqs.controller.CQSUserPageServlet;
import com.comcast.plaxo.cqs.model.CQSQueue;
import com.comcast.plaxo.cqs.persistence.CQSQueueCassandraPersistence;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CQSUserPageTest {

    protected static Logger logger = Logger.getLogger(CQSUserPageTest.class);
    
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
    }
    
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
	
	@Test
	public void testCQSQueuesPage() {

		try {
		
			CQSUserPageServlet cqsServlet = new CQSUserPageServlet();
			UserCassandraPersistence userHandler = new UserCassandraPersistence();
			User user = userHandler.getUserByName("cqs_unit_test");
			
			if (user == null) {
				user = userHandler.createUser("cqs_unit_test", "cqs_unit_test");
			}
			
	    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();

	    	Resp res1 = createQueue(cqsServlet, queueName, user.getUserId());
			assertTrue(res1.httpCode == 200 || res1.httpCode == 0);
			CQSQueueCassandraPersistence queueHandler = new CQSQueueCassandraPersistence();
			List<CQSQueue> queues = queueHandler.listQueues(user.getUserId(), null);
			
			if (queues != null && queues.size() > 0) {
			
				for (int i = 0; i < queues.size(); i++) {
				
					CQSQueue queue1 = queues.get(i);
					
					if (queue1.getName().equals(queueName)) {
						Resp res2 = deleteQueue(cqsServlet, queue1.getRelativeUrl(), user.getUserId());
						assertTrue(res2.httpCode == 200 || res2.httpCode == 0);
					}
				}
			}
			
		} catch (Exception ex) {
            logger.error("Exception", ex);
            fail("Test failed: " + ex.toString());
		}
	}
	
	private Resp createQueue(CQSUserPageServlet cqsServlet, String queueName, String userId) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "queueName", queueName);
		addParam(params, "userId", userId);
		addParam(params, "Create", "Create");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		cqsServlet.doGet(request, response);
		response.getWriter().flush();
		
		return new Resp(response.getStatus(), out.toString());
	}
	
	private Resp deleteQueue(CQSUserPageServlet cqsServlet, String qUrl, String userId) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "qUrl", qUrl);
		addParam(params, "userId", userId);
		addParam(params, "Delete", "Delete");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		cqsServlet.doGet(request, response);
		response.getWriter().flush();
		
		return new Resp(response.getStatus(), out.toString());
	}
	
	private void addParam(Map<String, String[]> params, String name, String val) {        
		
        String[] paramVals = new String[1];
        paramVals[0] = val;
        params.put(name, paramVals);        
    }
	
	class Resp {

		Resp(int code, String res) {
            this.httpCode = code;
            this.resp = res;
        }
        
        int httpCode;
        String resp;
    };
}
