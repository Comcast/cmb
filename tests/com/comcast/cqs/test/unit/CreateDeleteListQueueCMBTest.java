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

import com.comcast.cmb.test.tools.SimpleHttpServletRequest;
import com.comcast.cmb.test.tools.SimpleHttpServletResponse;
import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cmb.common.util.Util;
import com.comcast.plaxo.cqs.controller.CQSControllerServlet;

import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.OutputStream;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: michael
 * Date: 2/6/12
 * Time: 2:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class CreateDeleteListQueueCMBTest {
	
    private static Logger logger = Logger.getLogger(CreateDeleteListQueueCMBTest.class);
    
    private User user;
    
    @Before
    public void setup() {
    	
        try {
        	
            Util.initLog4jTest();
            CMBControllerServlet.valueAccumulator.initializeAllCounters();
            PersistenceFactory.reset();

            IUserPersistence userHandler = new UserCassandraPersistence();
            user = userHandler.getUserByName("cqs_unit_test");
    
            if (user == null) {
                user =  userHandler.createUser("cqs_unit_test", "cqs_unit_test");
            }

        } catch (Exception ex) {
            logger.error("setup failed", ex);
            assertFalse(true);
        }
    }

    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
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

    private Resp addQueue(CQSControllerServlet cqs, User user, String qName) throws Exception {

        SimpleHttpServletRequest request = new SimpleHttpServletRequest();
        Map<String, String[]> params = new HashMap<String, String[]>();
        request.setRequestUrl(CMBProperties.getInstance().getCQSServerUrl());


        addParam(params, "Action", "CreateQueue");
        addParam(params, "AWSAccessKeyId", user.getAccessKey());
        addParam(params, "QueueName", qName);
        
        addParam(params, "Attribute.1.Name", "VisibilityTimeout");
        addParam(params, "Attribute.1.Value", "30");
        addParam(params, "Attribute.2.Name", "Policy");
        addParam(params, "Attribute.2.Value", "");
        addParam(params, "Attribute.3.Name", "MaximumMessageSize");
        addParam(params, "Attribute.3.Value", "65536");
        addParam(params, "Attribute.4.Name", "MessageRetentionPeriod");
        addParam(params, "Attribute.4.Value", "172800");
        addParam(params, "Attribute.5.Name", "DelaySeconds");
        addParam(params, "Attribute.5.Value", "10");

        request.setParameterMap(params);
        SimpleHttpServletResponse response = new SimpleHttpServletResponse();

        OutputStream out = new ByteArrayOutputStream();
        response.setOutputStream(out);
              
        cqs.doGet(request, response);
        response.getWriter().flush();
        
        return new Resp(response.getStatus(), out.toString());
    }
    
    private Resp listQueues(CQSControllerServlet sqs, User user, String qName_prefix) throws Exception {
        SimpleHttpServletRequest request = new SimpleHttpServletRequest();
        request.setRequestUrl(CMBProperties.getInstance().getCQSServerUrl());

        Map<String, String[]> params = new HashMap<String, String[]>();
        addParam(params, "Action", "ListQueues");
        addParam(params, "AWSAccessKeyId", user.getAccessKey());
        addParam(params, "QueueNamePrefix", qName_prefix);
        request.setParameterMap(params);

        SimpleHttpServletResponse response = new SimpleHttpServletResponse();

        OutputStream out = new ByteArrayOutputStream();
        response.setOutputStream(out);

        sqs.doGet(request, response);
        response.getWriter().flush();

        return new Resp(response.getStatus(), out.toString());
    }
    
    private Resp getQueueUrl(CQSControllerServlet sqs, User user, String qName) throws Exception {
        SimpleHttpServletRequest request = new SimpleHttpServletRequest();
        request.setRequestUrl(CMBProperties.getInstance().getCQSServerUrl());

        Map<String, String[]> params = new HashMap<String, String[]>();
        addParam(params, "Action", "GetQueueUrl");
        addParam(params, "AWSAccessKeyId", user.getAccessKey());
        addParam(params, "QueueName", qName);
        request.setParameterMap(params);

        SimpleHttpServletResponse response = new SimpleHttpServletResponse();
        OutputStream out = new ByteArrayOutputStream();
        response.setOutputStream(out);

        sqs.doGet(request, response);
        
        response.getWriter().flush();

        return new Resp(response.getStatus(), out.toString());
    }

    private Resp delQueue(CQSControllerServlet sqs, User user, String qUrl) throws Exception {
        SimpleHttpServletRequest request = new SimpleHttpServletRequest();
        request.setRequestUrl(qUrl);

        Map<String, String[]> params = new HashMap<String, String[]>();
        addParam(params, "Action", "DeleteQueue");
        addParam(params, "AWSAccessKeyId", user.getAccessKey());
        request.setParameterMap(params);

        SimpleHttpServletResponse response = new SimpleHttpServletResponse();
        OutputStream out = new ByteArrayOutputStream();
        response.setOutputStream(out);

        sqs.doGet(request, response);

        response.getWriter().flush();

        return new Resp(response.getStatus(), out.toString());
    }

    private class ResponseParser  extends org.xml.sax.helpers.DefaultHandler {
        private List<String> qUrls = new ArrayList<String>();
        private String errCode = null;
        private CharArrayWriter content = new CharArrayWriter();

        public void endElement (String uri, String localName, String qName) {
            if (qName.equals("QueueUrl")) {
                String qUrl = content.toString().trim();
                qUrls.add(qUrl);
            } else if (qName.equals("Code")) {
                errCode = content.toString().trim();
            }
            content.reset();
        }

        public void characters( char[] ch, int start, int length ) {
            content.write( ch, start, length );
        }
    }

    private ResponseParser getParsedResponseFromString(String res) {

        javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
        javax.xml.parsers.SAXParser saxParser;

        ResponseParser pl = new ResponseParser();

        try {
            saxParser = fac.newSAXParser();
            saxParser.parse(new ByteArrayInputStream(res.getBytes()), pl);

        } catch (Exception ex) {

            logger.error("Exception parsing xml", ex);
            assertFalse(true);
        }
        return pl;
    }
    
    @Test
    public void testInvalidAction() {
        try {
            CQSControllerServlet cqs = new CQSControllerServlet();
            cqs.initPersistence();

            SimpleHttpServletRequest request = new SimpleHttpServletRequest();

            Map<String, String[]> params = new HashMap<String, String[]>();
            addParam(params, "Action", "InvalidAction");

            request.setParameterMap(params);
            SimpleHttpServletResponse response = new SimpleHttpServletResponse();

            OutputStream out = new ByteArrayOutputStream();
            response.setOutputStream(out);

            cqs.doGet(request, response);
            response.getWriter().flush();
            
            logger.info(out.toString());

            assertEquals(response.getStatus(), CMBErrorCodes.InvalidAction.getHttpCode());

            String code = getParsedResponseFromString(out.toString()).errCode;

            assertEquals(code, CMBErrorCodes.InvalidAction.getCMBCode());
            
        } catch (Exception ex) {
            logger.error("testInvalidAction failed", ex);
            assertFalse(true);            
        }
    }

    @Test
    public void testCreateDeleteListTopicServlet() {

        try {
            Random rand = new Random();
            
            CQSControllerServlet cqs = new CQSControllerServlet();
            cqs.initPersistence();
                        
            String qName = "TestQueue1-"+rand.nextInt();
            String qName2 = "TestQueue2-"+rand.nextInt();

            //Testing add to Queue
            Resp res = addQueue(cqs, user, qName);
            assertTrue(res.httpCode >= 200 && res.httpCode < 300);
            logger.info("res1="+res.resp);

            Resp res2 = addQueue(cqs, user, qName2);
            assertTrue(res2.httpCode >= 200 && res2.httpCode < 300);
            logger.info("res2="+res2.resp);            
            
            //Testing listQueue
            Resp res3 = listQueues(cqs, user, "TestQueue");
            assertTrue(res3.httpCode >= 200 && res3.httpCode < 300);
            logger.info("res3="+res3.resp);
            
            List<String> qUrls = getParsedResponseFromString(res3.resp).qUrls;
            
            Resp res4 = listQueues(cqs, user, "TestQueue1");
            assertTrue(res4.httpCode >= 200 && res4.httpCode < 300);
            logger.info("res4="+res4.resp);
            
            Resp res5 = getQueueUrl(cqs, user, qName);
            assertTrue(res5.httpCode >= 200 && res5.httpCode < 300);
            logger.info("resp5="+res5.resp);
            
            //Testing delete Queues
            for(String qUrl : qUrls) {
                Resp res6 = delQueue(cqs, user, qUrl);
                assertTrue(res6.httpCode >= 200 && res6.httpCode < 300);
                logger.info("res6="+res6.resp);
            }

        } catch (Exception ex) {
            logger.error("testCreateDeleteListTopicServlet failed", ex);
            assertFalse(true);
        }
    }
    
    @Test
    public void testConsistencies() throws Exception {
        Cluster cluster = HFactory.getOrCreateCluster("test11-cluster", "test11:9160");
        Keyspace keyspace = HFactory.createKeyspace("CMB", cluster, new ConsistencyLevelPolicy() {
            
            @Override
            public HConsistencyLevel get(OperationType arg0, String arg1) {
                return HConsistencyLevel.ALL;
            }
            
            @Override
            public HConsistencyLevel get(OperationType arg0) {
                return HConsistencyLevel.ALL;            
            }
        });
        
        HFactory.createKeyspace("CMB", cluster, new ConsistencyLevelPolicy() {
            
            @Override
            public HConsistencyLevel get(OperationType arg0, String arg1) {
                return HConsistencyLevel.ALL;
            }
            
            @Override
            public HConsistencyLevel get(OperationType arg0) {
                return HConsistencyLevel.ALL;            
            }
        });
        
        logger.info("ks=" + keyspace);
    }
}
