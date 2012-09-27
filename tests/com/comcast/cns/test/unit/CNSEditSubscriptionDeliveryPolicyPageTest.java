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
import com.comcast.plaxo.cns.controller.CNSEditSubscriptionDeliveryPolicyPage;
import com.comcast.plaxo.cns.model.CNSSubscription;
import com.comcast.plaxo.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.plaxo.cns.model.CNSTopic;
import com.comcast.plaxo.cns.persistence.CNSSubscriptionCassandraPersistence;
import com.comcast.plaxo.cns.persistence.CNSTopicCassandraPersistence;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CNSEditSubscriptionDeliveryPolicyPageTest {

    private static Logger log = Logger.getLogger(CNSEditSubscriptionDeliveryPolicyPageTest.class);
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
	public void testEditSubscriptionDeliveryPolicyPage() {

		CNSTopicCassandraPersistence topicHandler = new CNSTopicCassandraPersistence();
		String topicArn = null;

		try {
			
			String topicName = TOPIC_PREFIX + random.nextLong();
			CNSTopic topic = topicHandler.createTopic(topicName, topicName, user.getUserId());
			topicArn = topic.getArn();
			
			CNSSubscriptionCassandraPersistence subscriptionHandler = new CNSSubscriptionCassandraPersistence();
			CNSSubscription subscription = subscriptionHandler.subscribe("http://www.plaxo.com", CnsSubscriptionProtocol.http, topic.getArn(), user.getUserId());
			List<CNSSubscription> l = CNSTestingUtils.confirmPendingSubscriptionsByTopic(topic.getArn(), user.getUserId(), CnsSubscriptionProtocol.http);
			
			assertTrue(l.size() == 1);
			
			subscription = l.get(0);
			
			CNSEditSubscriptionDeliveryPolicyPage eidtSubPolicy = new CNSEditSubscriptionDeliveryPolicyPage();
			
			Resp response = editSubscriptionDeliveryPolicy(eidtSubPolicy, subscription.getArn(), "77", "3", "16", "3", "18", "3", "5", "exponential", user.getUserId());
			
			assertTrue(response.httpCode == 200 || response.httpCode == 0);
		
		} catch (Exception e) {
            log.error("exception=" + e, e);
            fail("test failed ex="+e);
		} finally {
			try {
				if (topicArn != null) {
					topicHandler.deleteTopic(topicArn);
				}
			} catch (Exception e) {	}
		}
	}
	
	private Resp editSubscriptionDeliveryPolicy(CNSEditSubscriptionDeliveryPolicyPage eidtSubPolicy, String subscriptionArn, String numRetries, String retriesNoDelay, String minDelay, String minDelayRetries, String maxDelay, String maxDelayRetries, String maxReceiveRate, String backoffFunc, String userId) throws Exception {
		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "subscriptionArn", subscriptionArn);
		addParam(params, "numRetries", numRetries);
		addParam(params, "retriesNoDelay", retriesNoDelay);
		addParam(params, "minDelay", minDelay);
		addParam(params, "minDelayRetries", minDelayRetries);
		addParam(params, "maxDelay", maxDelay);
		addParam(params, "maxDelayRetries", maxDelayRetries);
		addParam(params, "maxReceiveRate", maxReceiveRate);
		addParam(params, "backoffFunc", backoffFunc);
		addParam(params, "userId", userId);
		addParam(params, "Update", "Update");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		eidtSubPolicy.doGet(request, response);
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
