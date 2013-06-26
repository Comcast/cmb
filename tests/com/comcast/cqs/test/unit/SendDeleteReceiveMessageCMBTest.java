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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.SimpleHttpServletRequest;
import com.comcast.cmb.test.tools.SimpleHttpServletResponse;
import com.comcast.cqs.controller.CQSControllerServlet;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.CQSMessagePartitionedCassandraPersistence;
import com.comcast.cqs.persistence.CQSQueueCassandraPersistence;
import com.comcast.cqs.persistence.ICQSMessagePersistence;
import com.comcast.cqs.persistence.ICQSQueuePersistence;

public class SendDeleteReceiveMessageCMBTest {

	private static Logger logger = Logger.getLogger(CreateDeleteListQueueCMBTest.class);
    
    private User user;
    private CQSQueue queue;
    
    private Random randomGenerator = new Random();
    private final static String QUEUE_PREFIX = "TSTQ_"; 
    
    @Before
    public void setup() {
    	
        try {
        	
            Util.initLog4jTest();
            CMBControllerServlet.valueAccumulator.initializeAllCounters();
            PersistenceFactory.reset();
    
            IUserPersistence userHandler = new UserCassandraPersistence();

            user = userHandler.getUserByName("cqs_unit_test");
    
            if (user == null) {
                user = userHandler.createUser("cqs_unit_test", "cqs_unit_test");
            }
            
            ICQSQueuePersistence queueHandler = new CQSQueueCassandraPersistence();
        	String queueName = QUEUE_PREFIX + randomGenerator.nextLong();
            queue = queueHandler.getQueue(user.getUserId(), queueName);
            
            if (queue == null) {
                queue = new CQSQueue(queueName, user.getUserId());
                queueHandler.createQueue(queue);
            }
            
        } catch (Exception ex) {
            logger.error("setup failed", ex);
            assertFalse(true);
        }
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
        
        public String getValueByTag(String tag) {
            String[] result = resp.split("</{0,1}" + tag + ">");

            // Pattern regex = Pattern.compile(".*<"+tag+">(.*?)</"+tag+">.*");
            // String[] result = Pattern.matches("<"+tag+">(.*?)</"+tag+">", resp);
            return result[1];
        }

        int httpCode;
        String resp;
    }

    private Resp sendMessage(CQSControllerServlet cqs, User user, CQSQueue queue, String msg) throws Exception {

        SimpleHttpServletRequest request = new SimpleHttpServletRequest();

        request.setRequestUrl(CMBProperties.getInstance().getCQSServiceUrl() + user.getUserId() + "/" + queue.getName());
        Map<String, String[]> params = new HashMap<String, String[]>();

        addParam(params, "Action", "SendMessage");
        addParam(params, "AWSAccessKeyId", user.getAccessKey());
        addParam(params, "MessageBody", msg);

        request.setParameterMap(params);
        SimpleHttpServletResponse response = new SimpleHttpServletResponse();

        OutputStream out = new ByteArrayOutputStream();

        response.setOutputStream(out);
              
        cqs.doGet(request, response);
        response.getWriter().flush();
        
        return new Resp(response.getStatus(), out.toString());
    }
    
    private Resp deleteMessage(CQSControllerServlet cqs, User user, CQSQueue queue, String receiptHandle) throws Exception {

        SimpleHttpServletRequest request = new SimpleHttpServletRequest();

        request.setRequestUrl(CMBProperties.getInstance().getCQSServiceUrl() + user.getUserId() + "/" + queue.getName());
        Map<String, String[]> params = new HashMap<String, String[]>();

        addParam(params, "Action", "DeleteMessage");
        addParam(params, "AWSAccessKeyId", user.getAccessKey());
        addParam(params, "ReceiptHandle", receiptHandle);

        request.setParameterMap(params);
        SimpleHttpServletResponse response = new SimpleHttpServletResponse();

        OutputStream out = new ByteArrayOutputStream();

        response.setOutputStream(out);
              
        cqs.doGet(request, response);
        response.getWriter().flush();
        
        return new Resp(response.getStatus(), out.toString());
    }
    
    private Resp receiveMessage(CQSControllerServlet cqs, User user, CQSQueue queue, String numberOfMessages) throws Exception {

        SimpleHttpServletRequest request = new SimpleHttpServletRequest();

        request.setRequestUrl(CMBProperties.getInstance().getCQSServiceUrl() + user.getUserId() + "/" + queue.getName());
        Map<String, String[]> params = new HashMap<String, String[]>();

        addParam(params, "Action", "ReceiveMessage");
        addParam(params, "AWSAccessKeyId", user.getAccessKey());
        addParam(params, "MaxNumberOfMessages", numberOfMessages);

        request.setParameterMap(params);
        SimpleHttpServletResponse response = new SimpleHttpServletResponse();

        OutputStream out = new ByteArrayOutputStream();

        response.setOutputStream(out);
              
        cqs.doGet(request, response);
        response.getWriter().flush();
        
        return new Resp(response.getStatus(), out.toString());
    }
    
    private Resp receiveMessageBody(CQSControllerServlet cqs, User user, CQSQueue queue, String numberOfMessages) throws Exception {

        SimpleHttpServletRequest request = new SimpleHttpServletRequest();

        request.setRequestUrl(CMBProperties.getInstance().getCQSServiceUrl() + user.getUserId() + "/" + queue.getName());
        Map<String, String[]> params = new HashMap<String, String[]>();

        addParam(params, "Action", "ReceiveMessageBody");
        addParam(params, "AWSAccessKeyId", user.getAccessKey());
        addParam(params, "MaxNumberOfMessages", numberOfMessages);

        request.setParameterMap(params);
        SimpleHttpServletResponse response = new SimpleHttpServletResponse();

        OutputStream out = new ByteArrayOutputStream();

        response.setOutputStream(out);
              
        cqs.doGet(request, response);
        response.getWriter().flush();
        
        return new Resp(response.getStatus(), out.toString());
    }
    
    @Test
    public void testSendDeleteReceiveMessageServlet() {

        try {
            Random rand = new Random();
            
            CQSControllerServlet cqs = new CQSControllerServlet();

            cqs.initPersistence();
            List<String> filterAttributes = new ArrayList<String>();
            filterAttributes.add("SenderId");
            filterAttributes.add("SentTimestamp");
            filterAttributes.add("All");
                        
            String msg1 = "Test Message1: " + rand.nextInt();
            String msg2 = "Test Message2: " + rand.nextInt();
            String msg3 = "Test Message3: " + rand.nextInt();

            // Testing add to Queue
            Resp res1 = sendMessage(cqs, user, queue, msg1);

            assertTrue(res1.httpCode >= 200 && res1.httpCode < 300);
            logger.info("res1=" + res1.resp);

            Resp res2 = sendMessage(cqs, user, queue, msg2);
            assertTrue(res2.httpCode >= 200 && res2.httpCode < 300);
            logger.info("res2=" + res2.resp);
            
            sendMessage(cqs, user, queue, msg3);

            Resp resp1 = receiveMessage(cqs, user, queue, "2");
            assertTrue(resp1.httpCode >= 200 && resp1.httpCode < 300);
            logger.info("resp1=" + resp1.resp);
            
            ICQSMessagePersistence persistence = new CQSMessagePartitionedCassandraPersistence();
            List<CQSMessage> messageList = persistence.peekQueue(queue.getRelativeUrl(), 0, null, null, 25);
            
            for (CQSMessage message : messageList) {
            	
            	String msg = CQSMessagePopulator.serializeMessage(message, filterAttributes);
                String[] results = msg.split("</{0,1}ReceiptHandle>");
                assertTrue(message.getReceiptHandle().equals(results[1]));
                Resp res3 = deleteMessage(cqs, user, queue, results[1]);

                assertTrue(res3.httpCode >= 200 && res3.httpCode < 300);
                logger.info("res3=" + res3.resp);            	
            }

        } catch (Exception ex) {
            logger.error("testCreateDeleteListTopicServlet failed", ex);
            assertFalse(true);
        }
    }
    
    @Test
    public void testSendDeleteReceiveMessageBodyServlet() {

        try {
            Random rand = new Random();
            
            CQSControllerServlet sqs = new CQSControllerServlet();

            sqs.initPersistence();
            List<String> filterAttributes = new ArrayList<String>();
            filterAttributes.add("SenderId");
            filterAttributes.add("SentTimestamp");
            filterAttributes.add("All");
                        
            String msg1 = "Test Message1: " + rand.nextInt();

            // Testing add to Queue
            Resp res1 = sendMessage(sqs, user, queue, msg1);

            assertTrue(res1.httpCode >= 200 && res1.httpCode < 300);
            logger.info("res1=" + res1.resp);
            
            Resp resp1 = receiveMessageBody(sqs, user, queue, "2");
            assertTrue(resp1.httpCode >= 200 && resp1.httpCode < 300);
            logger.info("resp1=" + resp1.resp);
            
            ICQSMessagePersistence persistence = new CQSMessagePartitionedCassandraPersistence();
            List<CQSMessage> messageList = persistence.peekQueue(queue.getRelativeUrl(), 0, null, null, 25);
            
            for (CQSMessage message : messageList) {
                Resp res3 = deleteMessage(sqs, user, queue, message.getReceiptHandle());

                assertTrue(res3.httpCode >= 200 && res3.httpCode < 300);
                logger.info("res3=" + res3.resp);            	
            }

        } catch (Exception ex) {
            logger.error("testCreateDeleteListTopicServlet failed", ex);
            assertFalse(true);
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
}
