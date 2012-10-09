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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.junit.* ;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.test.tools.CMBTestingConstants;
import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.persistence.CNSSubscriptionCassandraPersistence;
import com.comcast.cns.persistence.CNSTopicCassandraPersistence;
import com.comcast.cns.persistence.ICNSAttributesPersistence;
import com.comcast.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.cns.persistence.ICNSTopicPersistence;

import static org.junit.Assert.*;

public class SubscribeListUnsubscribeCassandraTest {

    private static Logger logger = Logger.getLogger(SubscribeListUnsubscribeCassandraTest.class);
    
	private User user1;
	private User user2;

	private Random rand = new Random();


    @Before
    public void setup() throws Exception {

    	com.comcast.cmb.common.util.Util.initLog4jTest();
    	CMBControllerServlet.valueAccumulator.initializeAllCounters();
		PersistenceFactory.reset();

		IUserPersistence userHandler = PersistenceFactory.getUserPersistence();

		String userName1 = "cns_unit_test_1";
		String userName2 = "cns_unit_test_2";
		
		user1 = userHandler.getUserByName(userName1);

		if (user1 == null) {
			user1 =  userHandler.createUser(userName1, userName1);
		}

		user2 = userHandler.getUserByName(userName2);

		if (user2 == null) {
			user2 =  userHandler.createUser(userName2, userName2);
		}
    }
    
    @Test
    public void testSubscribe() {

        ICNSTopicPersistence topicHandler = new CNSTopicCassandraPersistence();
        
        String topicArn = null;

        try {

            String topicName = "T" + rand.nextLong();

            String userId1 = user1.getUserId();
            String userId2 = user2.getUserId();

            CNSTopic t = topicHandler.createTopic(topicName, topicName, userId2);
            topicArn = t.getArn();

            ICNSSubscriptionPersistence subscriptionHandler = new CNSSubscriptionCassandraPersistence();
            subscriptionHandler.subscribe(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/1234", CnsSubscriptionProtocol.http, t.getArn(), userId1);
            
        } catch (Exception ex) {
            
            fail(ex.toString());

        } finally {
        
            if (topicArn != null) {
                try {
                    topicHandler.deleteTopic(topicArn);
                } catch (Exception e) { }
            }
        }
    }

	@Test
	public void testSubscribeListUnsubsribeTopic() {

		ICNSTopicPersistence topicHandler = new CNSTopicCassandraPersistence();
		ICNSAttributesPersistence attributeHandler = PersistenceFactory.getCNSAttributePersistence();
		
		String topicArn = null;

		try {

			String topicName = "T" + rand.nextLong();

			String userId1 = user1.getUserId();
			String userId2 = user2.getUserId();

			CNSTopic t = topicHandler.createTopic(topicName, topicName, userId2);
			topicArn = t.getArn();
			
			try {
                topicHandler.deleteTopic(topicArn); //delete any pre-existing state
            } catch (Exception e) { }
            
            t = topicHandler.createTopic(topicName, topicName, userId2);
            topicArn = t.getArn();

			ICNSSubscriptionPersistence subscriptionHandler = new CNSSubscriptionCassandraPersistence();
			long beforeSubscribeCount = subscriptionHandler.getCountSubscription(t.getArn(), "subscriptionPending");

			CNSSubscription s = subscriptionHandler.subscribe(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/1234", CnsSubscriptionProtocol.http, t.getArn(), userId1);

			long afterSubscribeCount = subscriptionHandler.getCountSubscription(t.getArn(), "subscriptionPending");
			
			// check default delivery policy on topic1
			
			CNSSubscriptionAttributes attributes = attributeHandler.getSubscriptionAttributes(s.getArn());
			
			assertTrue("Expected 3 retries in healthy policy, instead found " + attributes.getEffectiveDeliveryPolicy().getHealthyRetryPolicy().getNumRetries(), attributes.getEffectiveDeliveryPolicy().getHealthyRetryPolicy().getNumRetries() == 3);
			
			List<CNSSubscription> l = subscriptionHandler.listSubscriptions(null, null, userId1);

			assertTrue("Could not verify PendingConfirmation state", l.size() == 1 && l.get(0).getArn().equals("PendingConfirmation"));

			s = subscriptionHandler.confirmSubscription(false, s.getToken(), t.getArn());

			l = subscriptionHandler.listSubscriptions(null, null, userId1);

			assertTrue("Expected 1 subscription, instead found " + l.size(), l.size() == 1);

			l = subscriptionHandler.listSubscriptionsByTopic(null, t.getArn(), null);

			assertTrue("Expected 1 subscription, instead found " + l.size(), l.size() == 1);

			l = subscriptionHandler.listSubscriptionsByTopic(null, t.getArn(), CnsSubscriptionProtocol.http);

			assertTrue("Expected 1 subscription, instead found " + l.size(), l.size() == 1);
			
			l = subscriptionHandler.listSubscriptionsByTopic(null, t.getArn(), CnsSubscriptionProtocol.email);

			assertTrue("Expected 0 subscription, instead found " + l.size(), l.size() == 0);
			
			assertTrue("Wrong number of subscribers", afterSubscribeCount == beforeSubscribeCount+1);

			try {
				l = subscriptionHandler.listSubscriptionsByTopic(null, com.comcast.cns.util.Util.generateCnsTopicArn("xyz", "east", userId1), null);
			} catch (CMBException ex) {
				assertTrue(ex.getCMBCode().equals(CMBErrorCodes.NotFound.getCMBCode()));
			}
			
			CNSSubscription sdup = subscriptionHandler.getSubscription(s.getArn());
			
			assertTrue("Subscriptions are not identical: " + s + "; " + sdup, s.equals(sdup));
			
			long beforeUnsubscribeCount = subscriptionHandler.getCountSubscription(t.getArn(), "subscriptionDeleted");

			subscriptionHandler.unsubscribe(s.getArn());
			
			long afterUnsubscribeCount = subscriptionHandler.getCountSubscription(t.getArn(), "subscriptionDeleted");

			l = subscriptionHandler.listSubscriptions(null, null, userId1);

			assertTrue("Expected 0 subscription, instead found " + l.size(), l.size() == 0);
			
			assertTrue("Wrong number of subscribers", afterUnsubscribeCount == beforeUnsubscribeCount + 1);

		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					topicHandler.deleteTopic(topicArn);
				} catch (Exception e) { }
			}
		}
	}

	@Test
	public void testSubscriptionPagination() {

		ICNSTopicPersistence topicHandler = new CNSTopicCassandraPersistence();
		
		String topicArn = null;

		try {

			String userId1 = user1.getUserId();

			String topicName = "T" + rand.nextLong();
			CNSTopic t = topicHandler.createTopic(topicName, topicName, userId1);
			topicArn = t.getArn();

			ICNSSubscriptionPersistence subscriptionHandler = new CNSSubscriptionCassandraPersistence();

			for (int i=1; i<=96; i++) {
				subscriptionHandler.subscribe(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + i, CnsSubscriptionProtocol.http, t.getArn(), userId1);
			}
			
			CNSTestingUtils.confirmPendingSubscriptionsByTopic(t.getArn(), userId1, CnsSubscriptionProtocol.http);
			
			for (int i=97; i<=103; i++) {
			
				subscriptionHandler.subscribe(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + i, CnsSubscriptionProtocol.http, t.getArn(), userId1);
				CNSTestingUtils.confirmPendingSubscriptionsByTopic(t.getArn(), userId1, CnsSubscriptionProtocol.http);
				Set<String> keys = new TreeSet<String>();
				
				List<CNSSubscription> l = subscriptionHandler.listSubscriptions(null, null, userId1);
				List<CNSSubscription> a = new ArrayList<CNSSubscription>();
				
				while (l.size() > 0) {
					
					a.addAll(l);
					
					for (CNSSubscription scr : l) {
						
						if (keys.contains(scr.getArn())) {
							fail("Duplicate subscription arn " + scr);
						} else {
							keys.add(scr.getArn());
						}
					}
					
					l = subscriptionHandler.listSubscriptions(l.get(l.size()-1).getArn(), null, userId1);
				}
				
				assertTrue("Wrong number of subscriptions: " + a.size() + " versus " + i, a.size() == i);
				
				logger.info(i + " subscriptions ok");
			}
			
		} catch (Exception ex) {
			
			fail(ex.toString());

		} finally {
		
			if (topicArn != null) {
				try {
					topicHandler.deleteTopic(topicArn);
				} catch (Exception e) { }
			}
		}
	}
	
	
	/**
	 * Performance testing of listSubscriptionsByTopic
	 */	
	@Test
	public void testlistSubscriptionsByTopicPerf() throws Exception {
        
		ICNSTopicPersistence topicHandler = new CNSTopicCassandraPersistence();
        
        String topicArn = null;
        long ts1=0L, ts2=0L;
        
        try {

            String userId1 = user1.getUserId();

            String topicName = "T" + rand.nextLong();
            CNSTopic t = topicHandler.createTopic(topicName, topicName, userId1);
            topicArn = t.getArn();

            ICNSSubscriptionPersistence subscriptionHandler = new CNSSubscriptionCassandraPersistence();

            for (int i=1; i<=200; i++) {
                subscriptionHandler.subscribe(CMBTestingConstants.HTTP_ENDPOINT_BASE_URL + "recv/" + i, CnsSubscriptionProtocol.http, t.getArn(), userId1);
            }
            
            CNSTestingUtils.confirmPendingSubscriptionsByTopic(t.getArn(), userId1, CnsSubscriptionProtocol.http);
            ts1 = System.currentTimeMillis();
            List<CNSSubscription> l = subscriptionHandler.listSubscriptionsByTopic(null, t.getArn(), null, 1000);
            int totalSize = 0;
        
            while (totalSize != 200 && l.size() > 0) {
                totalSize += l.size();
                logger.info("Number of subscriptions is " + totalSize);
                l = subscriptionHandler.listSubscriptionsByTopic(l.get(l.size() - 1).getArn(), t.getArn(), null, 1000);
            }
            
            if (totalSize != 200) {
                fail("Expected 200 subscriptions, instead found " + totalSize);
            }
            
            ts2 = System.currentTimeMillis();
            
        } catch (Exception ex) {
            
            fail(ex.toString());

        } finally {
        
            if (topicArn != null) {
                try {
                    topicHandler.deleteTopic(topicArn);
                } catch (Exception e) { }
            }
            
            logger.info("List subscriptions response time is " + (ts2 - ts1) + " ms");
        }
	}
	
	@After    
    public void tearDown() throws PersistenceException {
	    CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
