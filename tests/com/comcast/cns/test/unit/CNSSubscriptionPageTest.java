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
import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.Util;
import com.comcast.plaxo.cns.controller.CNSSubscriptionPageServlet;
import com.comcast.plaxo.cns.model.CNSSubscription;
import com.comcast.plaxo.cns.model.CNSTopic;
import com.comcast.plaxo.cns.persistence.CNSSubscriptionCassandraPersistence;
import com.comcast.plaxo.cns.persistence.CNSTopicCassandraPersistence;

import org.apache.log4j.Logger;
import org.junit.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CNSSubscriptionPageTest {

    private static Logger log = Logger.getLogger(CNSSubscriptionPageTest.class);
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
	public void testCNSSubscriptionPage() {

		CNSTopicCassandraPersistence topicHandler = new CNSTopicCassandraPersistence();
		String topicArn = null;

		try {
		
			CNSSubscriptionPageServlet subscripePage = new CNSSubscriptionPageServlet();

			String topicName = TOPIC_PREFIX + random.nextLong();
			CNSTopic topic = topicHandler.createTopic(topicName, topicName, user.getUserId());
			topicArn = topic.getArn();

			Response res1 = subscribe(subscripePage, "tina_2@plaxo.com", "EMAIL", topic.getArn(), user.getUserId());
			assertTrue(res1.httpCode == 200 || res1.httpCode == 0);
			
			CNSSubscriptionCassandraPersistence subscribePS = new CNSSubscriptionCassandraPersistence();
			List<CNSSubscription> subscriptions = subscribePS.listSubscriptionsByTopic(null, topic.getArn(), null, 10, false);
			
			if (subscriptions != null && subscriptions.size() > 0) {
				CNSSubscription subscription = (CNSSubscription)subscriptions.get(0);
				Response res2 = unsubscribe(subscripePage, subscription.getArn(), subscription.getTopicArn(), user.getUserId());
				assertTrue(res2.httpCode == 200 || res2.httpCode == 0);
			}
			
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
	
	private Response subscribe(CNSSubscriptionPageServlet subscripeServlet, String endPoint, String protocol, String topicArn, String userId) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "userId", userId);
		addParam(params, "topicArn", topicArn);
		addParam(params, "endPoint", endPoint);
		addParam(params, "protocol", protocol);
		addParam(params, "Subscribe", "Subscribe");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		subscripeServlet.doGet(request, response);
		response.getWriter().flush();
		
		return new Response(response.getStatus(), out.toString());	
	}
	
	private Response unsubscribe(CNSSubscriptionPageServlet subscripeServlet, String arn, String topicArn, String userId) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "userId", userId);
		addParam(params, "arn", arn);
		addParam(params, "topicArn", topicArn);
		addParam(params, "Unsubscribe", "Unsubscribe");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		subscripeServlet.doGet(request, response);
		response.getWriter().flush();
		
		return new Response(response.getStatus(), out.toString());	
	}
	
	private void addParam(Map<String, String[]> params, String name, String val) {        
        String[] paramVals = new String[1];
        paramVals[0] = val;
        params.put(name, paramVals);        
    }
	
	private class Response {
		
        Response(int code, String resp) {
            this.httpCode = code;
            this.resp = resp;
        }
        
		private int httpCode;
        private String resp;
    };
    
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
