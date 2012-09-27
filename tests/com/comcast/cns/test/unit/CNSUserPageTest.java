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
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cns.controller.CNSUserPageServlet;
import com.comcast.plaxo.cns.model.CNSTopic;
import com.comcast.plaxo.cns.persistence.CNSTopicCassandraPersistence;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CNSUserPageTest {

    private static Logger logger = Logger.getLogger(CNSUserPageTest.class);
    private Random random = new Random();
    private User user;

    @Before
    public void setup() throws Exception {
    	
    	Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();

        IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
        String userName = "cns_unit_test";
        user = userHandler.getUserByName(userName);

        if (user == null) {	          
            user =  userHandler.createUser(userName, userName);
        }
    }
	
	@Test
	public void testCNSTopicsPage() {

		try {
		
			CNSUserPageServlet cnsServlet = new CNSUserPageServlet();
			
			String topicName = "T" + random.nextLong();
			Response res1 = createTopic(cnsServlet, topicName, topicName, user.getUserId());
			assertTrue(res1.httpCode == 200 || res1.httpCode == 0);
				
			CNSTopicCassandraPersistence topicPS = new CNSTopicCassandraPersistence();
			List<CNSTopic> topics = topicPS.listTopics(user.getUserId(), null);
			
			if (topics != null && topics.size() > 0) {
				
				for (int i=0; i<topics.size(); i++) {
					
					CNSTopic topic = topics.get(i);
					
					if (topic.getName().equals(topicName)) {
						Response res2 = deleteTopic(cnsServlet, topic.getArn(), user.getUserId());
						assertTrue(res2.httpCode == 200 || res2.httpCode == 0);
					}
				}
			}
			
		} catch (Exception ex) {
            logger.error("exception=" + ex, ex);
            fail("test failed ex=" + ex);
		}
	}
	
	private Response createTopic(CNSUserPageServlet cnsServlet, String topicName, String displayName, String userId) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "topic", topicName);
		addParam(params, "display", displayName);
		addParam(params, "userId", userId);
		addParam(params, "Create", "Create");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		cnsServlet.doGet(request, response);
		response.getWriter().flush();
		
		return new Response(response.getStatus(), out.toString());	
	}
	
	private Response deleteTopic(CNSUserPageServlet cnsServlet, String arn, String userId) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "arn", arn);
		addParam(params, "userId", userId);
		addParam(params, "Delete", "Delete");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		cnsServlet.doGet(request, response);
		response.getWriter().flush();
		
		return new Response(response.getStatus(), out.toString());
	}
	
	private void addParam(Map<String, String[]> params, String name, String val) {        
        String[] paramVals = new String[1];
        paramVals[0] = val;
        params.put(name, paramVals);        
    }
	
	class Response {
		
        Response(int code, String res) {
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
