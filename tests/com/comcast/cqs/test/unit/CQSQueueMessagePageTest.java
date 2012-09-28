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

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.SimpleHttpServletRequest;
import com.comcast.cmb.test.tools.SimpleHttpServletResponse;
import com.comcast.cqs.controller.CQSQueueMessagesPageServlet;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.CQSMessagePartitionedCassandraPersistence;
import com.comcast.cqs.persistence.CQSQueueCassandraPersistence;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.persistence.ICQSQueuePersistence;

import org.apache.log4j.Logger;
import org.junit.*;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CQSQueueMessagePageTest {

    protected static Logger logger = Logger.getLogger(CQSQueueMessagePageTest.class);
    
    private User user;
    private CQSQueue queue;
    
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 

    @Before
    public void setup() throws Exception {
    	
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();

		UserCassandraPersistence userHandler = new UserCassandraPersistence();
		user = userHandler.getUserByName("cqs_unit_test");
		
		if (user == null) {
			user = userHandler.createUser("cqs_unit_test", "cqs_unit_test");
		}
		
		CQSQueueCassandraPersistence queueHandler = new CQSQueueCassandraPersistence();

    	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
        queue = queueHandler.getQueue(user.getUserId(), queueName);
        
        if (queue == null) {
            queue = new CQSQueue(queueName, user.getUserId());
            queueHandler.createQueue(queue);
        }
    }
    
    @After    
    public void tearDown() {

    	if (queue != null) {
    		try {
	    		ICQSQueuePersistence queueHandler = new CQSQueueCassandraPersistence();
	            queueHandler.deleteQueue(queue.getRelativeUrl());
    		} catch (Exception ex) {
    			logger.error("Tear down exception", ex);
    		}
    	}
    	
    	CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
	
	@Test
	public void testCQSQueueMessagePage() {
		
		try {
			
			CQSQueueMessagesPageServlet msgServlet = new CQSQueueMessagesPageServlet();
			Resp res1 = send(msgServlet, user.getUserId(), queue.getName(), "Test sending message1");
			assertTrue(res1.httpCode == 200 || res1.httpCode == 0);
			
			ICQSMessagePersistence messageHandler = new CQSMessagePartitionedCassandraPersistence();

			List<CQSMessage> messages = messageHandler.peekQueue(queue.getRelativeUrl(), null, null, 10);
			
			if (messages != null && messages.size() > 0) {
				
				CQSMessage cqsMsg = messages.get(0);
				Resp res2 = delete(msgServlet, queue.getName(), cqsMsg.getReceiptHandle(), user.getUserId());
				assertTrue(res2.httpCode == 200 || res2.httpCode == 0);
			}
			
		} catch (Exception ex) {
            logger.error("Exception", ex);
            fail("Test failed: " + ex.toString());
        }
	}
	
	private Resp send(CQSQueueMessagesPageServlet msgServlet, String userId, String queueName, String msg) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "userId", userId);
		addParam(params, "queueName", queueName);
		addParam(params, "message", msg);
		addParam(params, "Send", "Send");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		msgServlet.doGet(request, response);
		response.getWriter().flush();
		
		return new Resp(response.getStatus(), out.toString());
	}
	
	private Resp delete(CQSQueueMessagesPageServlet msgServlet, String queueName, String receiptHandle, String userId) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "queueName", queueName);
		addParam(params, "receiptHandle", receiptHandle);
		addParam(params, "Delete", "Delete");
		addParam(params, "userId", userId);
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		msgServlet.doGet(request, response);
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
    }
}
