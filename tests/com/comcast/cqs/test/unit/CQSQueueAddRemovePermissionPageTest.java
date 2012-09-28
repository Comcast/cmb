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

import org.apache.log4j.Logger;
import org.junit.*;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.CMBStatement;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.SimpleHttpServletRequest;
import com.comcast.cmb.test.tools.SimpleHttpServletResponse;
import com.comcast.cqs.controller.CQSAddQueuePermissionPage;
import com.comcast.cqs.controller.CQSQueuePermissionsPage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.CQSQueueCassandraPersistence;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CQSQueueAddRemovePermissionPageTest {
	
	private static Logger log = Logger.getLogger(CQSQueueAddRemovePermissionPageTest.class);
	
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 
	
	@Before
	public void setup() throws Exception {
		Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
	}
	
	@Test
	public void testCQSQueueAddRemovePermission() {
		
		try {
			
			CQSAddQueuePermissionPage addPermissionServlet = new CQSAddQueuePermissionPage();
			UserCassandraPersistence usrPS = new UserCassandraPersistence();
			User user = usrPS.getUserByName("cqs_unit_test");
			
			if (user == null) {
				user = usrPS.createUser("cqs_unit_test", "cqs_unit_test");
			}
			
	    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();

	    	CQSQueueCassandraPersistence queueHandler = new CQSQueueCassandraPersistence();
			CQSQueue queue = queueHandler.getQueue(user.getUserId(), queueName);
			
			if (queue == null) {
				queue = new CQSQueue(queueName, user.getUserId());
				queueHandler.createQueue(queue);
			}
			
			Resp res1 = addPermission(addPermissionServlet, queue.getName(), "on", "on", user.getUserId());
			assertTrue(res1.httpCode == 200 || res1.httpCode == 0);
			queue = queueHandler.getQueue(queue.getRelativeUrl());
			CQSQueuePermissionsPage queuePermissionServlet = new CQSQueuePermissionsPage();
			
			CMBPolicy policy = new CMBPolicy(queue.getPolicy());
			
			if (policy != null && !policy.getStatements().isEmpty()) {
				
				List<CMBStatement> stmt = policy.getStatements();
				
				for (int i = 0; stmt != null && i < stmt.size(); i++) {
					
					CMBStatement cmbStats = stmt.get(i);
					Resp res2 = removePermission(queuePermissionServlet, user.getUserId(), queue.getName(), cmbStats.getSid());
					assertTrue(res2.httpCode == 200 || res2.httpCode == 0);
				}
			}
			
			queueHandler.deleteQueue(queue.getRelativeUrl());
			
		} catch (Exception ex) {
			log.error("exception=" + ex, ex);
            fail("test failed ex=" + ex);
		}
	}
	
	private Resp addPermission(CQSAddQueuePermissionPage addPermissionServlet, String queueName, String everybody, String allActions, String userId) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "queueName", queueName);
		addParam(params, "userId", userId);
		addParam(params, "everybody", everybody);
		addParam(params, "allActions", allActions);
		addParam(params, "allow", "allow");
		addParam(params, "Add", "Add");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		addPermissionServlet.doGet(request,  response);
		response.getWriter().flush();
		
		return new Resp(response.getStatus(), out.toString());
	}
	
	private Resp removePermission(CQSQueuePermissionsPage queuePermissionServlet, String userId, String queueName, String sid) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "queueName", queueName);
		addParam(params, "userId", userId);
		addParam(params, "sid", sid);
		addParam(params, "Remove", "Remove");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		queuePermissionServlet.doGet(request,  response);
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

    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
