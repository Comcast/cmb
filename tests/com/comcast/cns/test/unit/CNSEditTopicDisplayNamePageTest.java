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
import com.comcast.plaxo.cns.controller.CNSEditTopicDisplayNamePage;
import com.comcast.plaxo.cns.model.CNSTopic;
import com.comcast.plaxo.cns.persistence.CNSTopicCassandraPersistence;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CNSEditTopicDisplayNamePageTest {

    private static Logger log = Logger.getLogger(CNSEditTopicDisplayNamePageTest.class);
    private Random random = new Random();
    private User user;
    private final String TOPIC_PREFIX = "TSTT";

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
	public void testUpdateTopicDisplayName() {
		
		CNSTopicCassandraPersistence topicHandler = new CNSTopicCassandraPersistence();
		String topicArn = null;

		try {
			
			CNSEditTopicDisplayNamePage topicPage = new CNSEditTopicDisplayNamePage();

			String topicName = TOPIC_PREFIX + random.nextLong();
			CNSTopic topic = topicHandler.createTopic(topicName, topicName, user.getUserId());
			topicArn = topic.getArn();

			Response res1 = updateDisplayName(topicPage, topic.getArn(), "TOPIC1 DISPLAY NAME JUNIT", user.getUserId());
			assertTrue(res1.httpCode == 200 || res1.httpCode == 0);

		} catch (Exception e) {
            log.error("exception=" + e, e);
            fail("test failed ex=" + e);
		} finally {
			try {
				if (topicArn != null) {
					topicHandler.deleteTopic(topicArn);
				}
			} catch (Exception e) {	}
		}
	}
	
	private Response updateDisplayName(CNSEditTopicDisplayNamePage editDisplayNameServlet, String topicArn, String displayName, String userId) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "topicArn", topicArn);
		addParam(params, "displayName", displayName);
		addParam(params, "userId", userId);
		addParam(params, "Edit", "Edit");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		editDisplayNameServlet.doGet(request, response);
		response.getWriter().flush();
		
		return new Response(response.getStatus(), out.toString());
	}
	
	private void addParam(Map<String, String[]> params, String name, String val) {        
        String[] paramVals = new String[1];
        paramVals[0] = val;
        params.put(name, paramVals);        
    }
	
	private class Response {

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
