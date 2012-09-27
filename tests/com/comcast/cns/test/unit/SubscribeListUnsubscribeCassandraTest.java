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

import com.comcast.cmb.test.tools.CNSTestingUtils;
import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.plaxo.cns.model.CNSSubscriptionAttributes;
import com.comcast.plaxo.cns.model.CNSTopic;
import com.comcast.plaxo.cns.model.CNSSubscription;
import com.comcast.plaxo.cns.persistence.ICNSAttributesPersistence;
import com.comcast.plaxo.cns.persistence.ICNSTopicPersistence;
import com.comcast.plaxo.cns.persistence.CNSSubscriptionCassandraPersistence;
import com.comcast.plaxo.cns.persistence.CNSTopicCassandraPersistence;
import com.comcast.plaxo.cns.persistence.ICNSSubscriptionPersistence;

import static org.junit.Assert.*;

public class SubscribeListUnsubscribeCassandraTest {

    private static Logger logger = Logger.getLogger(SubscribeListUnsubscribeCassandraTest.class);
    
	private User user1;
	private User user2;

	private Random rand = new Random();


    @Before
    public void setup() throws Exception {

    	com.comcast.plaxo.cmb.common.util.Util.initLog4jTest();
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
            subscriptionHandler.subscribe("http://www.plaxo.com/test", CnsSubscriptionProtocol.http, t.getArn(), userId1);
            
        } catch (Exception ex) {
            
            logger.error("test failed", ex);
            fail("test failed: " + ex.toString());

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

			CNSSubscription s = subscriptionHandler.subscribe("http://www.plaxo.com/test", CnsSubscriptionProtocol.http, t.getArn(), userId1);

			long afterSubscribeCount = subscriptionHandler.getCountSubscription(t.getArn(), "subscriptionPending");
			
			// check default delivery policy on topic1
			
			CNSSubscriptionAttributes attributes = attributeHandler.getSubscriptionAttributes(s.getArn());
			
			assertTrue(attributes.getEffectiveDeliveryPolicy().getHealthyRetryPolicy().getNumRetries() == 3);
			
			List<CNSSubscription> l = subscriptionHandler.listSubscriptions(null, null, userId1);

			assertTrue(l.size() == 1 && l.get(0).getArn().equals("PendingConfirmation"));

			s = subscriptionHandler.confirmSubscription(false, s.getToken(), t.getArn());

			l = subscriptionHandler.listSubscriptions(null, null, userId1);

			assertTrue(l.size() == 1);

			l = subscriptionHandler.listSubscriptionsByTopic(null, t.getArn(), null);

			assertTrue(l.size() == 1);

			l = subscriptionHandler.listSubscriptionsByTopic(null, t.getArn(), CnsSubscriptionProtocol.http);

			assertTrue(l.size() == 1);
			
			l = subscriptionHandler.listSubscriptionsByTopic(null, t.getArn(), CnsSubscriptionProtocol.email);

			assertTrue(l.size() == 0);
			
			assertTrue(afterSubscribeCount == beforeSubscribeCount+1);

			try {
				l = subscriptionHandler.listSubscriptionsByTopic(null, com.comcast.plaxo.cns.util.Util.generateCnsTopicArn("xyz", "east", userId1), null);
			} catch (CMBException ex) {
				assertTrue(ex.getCMBCode().equals(CMBErrorCodes.NotFound.getCMBCode()));
			}
			
			CNSSubscription sdup = subscriptionHandler.getSubscription(s.getArn());
			
			assertTrue(s.equals(sdup));
			
			long beforeUnsubscribeCount = subscriptionHandler.getCountSubscription(t.getArn(), "subscriptionDeleted");

			subscriptionHandler.unsubscribe(s.getArn());
			
			long afterUnsubscribeCount = subscriptionHandler.getCountSubscription(t.getArn(), "subscriptionDeleted");

			l = subscriptionHandler.listSubscriptions(null, null, userId1);

			assertTrue(l.size() == 0);
			
			assertTrue(afterUnsubscribeCount == beforeUnsubscribeCount + 1);

		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("test failed: " + ex.toString());

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
				subscriptionHandler.subscribe("http://www.plaxo.com/test/" + i, CnsSubscriptionProtocol.http, t.getArn(), userId1);
			}
			
			CNSTestingUtils.confirmPendingSubscriptionsByTopic(t.getArn(), userId1, CnsSubscriptionProtocol.http);
			
			for (int i=97; i<=103; i++) {
			
				subscriptionHandler.subscribe("http://www.plaxo.com/test/" + i, CnsSubscriptionProtocol.http, t.getArn(), userId1);
				CNSTestingUtils.confirmPendingSubscriptionsByTopic(t.getArn(), userId1, CnsSubscriptionProtocol.http);
				Set<String> keys = new TreeSet<String>();
				
				List<CNSSubscription> l = subscriptionHandler.listSubscriptions(null, null, userId1);
				List<CNSSubscription> a = new ArrayList<CNSSubscription>();
				
				while (l.size() > 0) {
					
					a.addAll(l);
					
					for (CNSSubscription scr : l) {
						
						if (keys.contains(scr.getArn())) {
							fail("Duplicate subscription arn: " + scr);
						} else {
							keys.add(scr.getArn());
						}
					}
					
					l = subscriptionHandler.listSubscriptions(l.get(l.size()-1).getArn(), null, userId1);
				}
				
				assertTrue(a.size() == i);
				
				logger.info(i + "...ok");
			}
			
		} catch (Exception ex) {
			
			logger.error("test failed", ex);
			fail("test failed: " + ex.toString());

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
                subscriptionHandler.subscribe("http://www.plaxo.com/test/" + i, CnsSubscriptionProtocol.http, t.getArn(), userId1);
            }
            
            CNSTestingUtils.confirmPendingSubscriptionsByTopic(t.getArn(), userId1, CnsSubscriptionProtocol.http);
            ts1 = System.currentTimeMillis();
            List<CNSSubscription> l = subscriptionHandler.listSubscriptionsByTopic(null, t.getArn(), null, 1000);
            int totalSize = 0;
            while (totalSize != 200 && l.size() > 0) {
                totalSize += l.size();
                logger.info("totalSize=" + totalSize);
                l = subscriptionHandler.listSubscriptionsByTopic(l.get(l.size() - 1).getArn(), t.getArn(), null, 1000);
            }
            if (totalSize != 200) {
                fail("Expected 200. Got:" + totalSize);
            }
            ts2 = System.currentTimeMillis();
            
        } catch (Exception ex) {
            
            logger.error("test failed", ex);
            fail("test failed: " + ex.toString());

        } finally {
        
            if (topicArn != null) {
                try {
                    topicHandler.deleteTopic(topicArn);
                } catch (Exception e) { }
            }
            logger.info("listSub listSub responseTIme=" + (ts2 - ts1));
        }
	}
	
	@After    
    public void tearDown() throws PersistenceException {
	    IUserPersistence userHandler = PersistenceFactory.getUserPersistence();

	    String userName1 = "cns_unit_test_1";
	    String userName2 = "cns_unit_test_2";

	    if (user1 != null) {
	        userHandler.deleteUser(userName1);
	    }

	    if (user2 != null) {
	        userHandler.deleteUser(userName2);
	    }

	    CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
