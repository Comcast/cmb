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

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.SimpleHttpServletRequest;
import com.comcast.cmb.test.tools.SimpleHttpServletResponse;
import com.comcast.cns.controller.CNSPublishToTopicPageServlet;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.persistence.CNSTopicCassandraPersistence;

import org.apache.log4j.Logger;
import org.junit.*;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CNSPublishToTopicPageTest {

    private static Logger log = Logger.getLogger(CNSPublishToTopicPageTest.class);
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
	public void testPublishToTopicPage() {
		
		CNSTopicCassandraPersistence topicHandler = new CNSTopicCassandraPersistence();
		String topicArn = null;

		try {
			
			CNSPublishToTopicPageServlet publishPage = new CNSPublishToTopicPageServlet();
			String topicName = TOPIC_PREFIX + random.nextLong();
			CNSTopic topic = topicHandler.createTopic(topicName, topicName, user.getUserId());
			topicArn = topic.getArn();

			Response response = publish(publishPage, topic.getArn(), user.getUserId(), "test subject1", "publish a testing message to topic", "same");
			assertTrue(response.httpCode == 200 || response.httpCode == 0);
			
		} catch (Exception ex) {
            log.error("exception=" + ex, ex);
            fail("test failed ex=" + ex);		
		} finally {
			try {
				if (topicArn != null) {
					topicHandler.deleteTopic(topicArn);
				}
			} catch (Exception e) {	}
		}
	}
	
	private Response publish(CNSPublishToTopicPageServlet pubServlet, String topicArn, String userId, String subject, String msg, String same) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "userId", userId);
		addParam(params, "topicArn", topicArn);
		addParam(params, "subject", subject);
		addParam(params, "message", msg);
		addParam(params, "isSame", same);
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		pubServlet.doGet(request, response);
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
