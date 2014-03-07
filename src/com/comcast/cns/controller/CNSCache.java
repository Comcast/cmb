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
package com.comcast.cns.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ExpiringCache;
import com.comcast.cmb.common.util.ExpiringCache.CacheFullException;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.persistence.ICNSAttributesPersistence;
import com.comcast.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.cns.persistence.ICNSTopicPersistence;

/**
 * Utility class that contains most of the caches the rest of the code uses
 * @author bwolf, aseem
 */
public class CNSCache {
	
    private static volatile ExpiringCache<String, CNSTopicAttributes> attributeCache = new ExpiringCache<String, CNSTopicAttributes>(CMBProperties.getInstance().getCNSCacheSizeLimit());
	private static volatile ICNSAttributesPersistence attributeHandler = PersistenceFactory.getCNSAttributePersistence();

    private static volatile ExpiringCache<String, List<CNSSubscription>> confirmedSubscriptionsCache = new ExpiringCache<String, List<CNSSubscription>>(CMBProperties.getInstance().getCNSCacheSizeLimit());
	private static volatile ICNSSubscriptionPersistence subscriptionHandler = PersistenceFactory.getSubscriptionPersistence();
	
    private static ExpiringCache<String, CNSTopic> topicCache = new ExpiringCache<String, CNSTopic>(CMBProperties.getInstance().getCNSCacheSizeLimit());
    private static ICNSTopicPersistence topicHandler = PersistenceFactory.getTopicPersistence();
    
	
    private static class CNSTopicCallable implements Callable<CNSTopic> {
    	
        String arn = null;
        
        public CNSTopicCallable(String arn) {
            this.arn = arn;
        }
        
        @Override
        public CNSTopic call() throws Exception {
            CNSTopic topic = topicHandler.getTopic(arn);
            return topic;
        }
    }

	/**
	 * 
	 * @param topicArn
	 * @return CNSTopicAttributes for the given topic or null if doesn't exist
	 * @throws Exception
	 */
    public static CNSTopic getTopic(String topicArn) throws Exception {
        
    	if (topicArn == null) {
        	return null;
        }
    	
        return topicCache.getAndSetIfNotPresent(topicArn, new CNSTopicCallable(topicArn), CMBProperties.getInstance().getCNSCacheExpiring() * 1000); 
    }

    
	private static class TopicAttributesCallable implements Callable<CNSTopicAttributes> {
        
    	String topicArn = null;
        
    	public TopicAttributesCallable(String key) {
            this.topicArn = key;
        }
        
    	@Override
        public CNSTopicAttributes call() throws Exception {
    		CNSTopicAttributes attributes = attributeHandler.getTopicAttributes(this.topicArn);
            return attributes;
        }
    }
    
	/**
	 * 
	 * @param topicArn
	 * @return CNSTopicAttributes for the given topic or null if doesn't exist
	 * @throws Exception
	 */
    public static CNSTopicAttributes getTopicAttributes(String topicArn) throws Exception {
        
    	if (topicArn == null) {
        	return null;
        }
    	
    	return attributeCache.getAndSetIfNotPresent(topicArn, new TopicAttributesCallable(topicArn), CMBProperties.getInstance().getCNSCacheExpiring() * 1000);        
    }
    
    public static void removeTopicAttributes(String topicArn) {
    	
    	if (topicArn == null) {
    		return;
    	}
    	
    	attributeCache.remove(topicArn);
    }

	private static class SubscriptionCallable implements Callable<List<CNSSubscription>> {
        
    	String topicArn = null;
        
    	public SubscriptionCallable(String key) {
            this.topicArn = key;
        }
        
    	@Override
        public List<CNSSubscription> call() throws Exception {

    		String nextToken = null;
    		int round = 0;
    		
    		List<CNSSubscription> allSubscriptions = new ArrayList<CNSSubscription>();
    		
    		while (round == 0 || nextToken != null) {
    		
    			List<CNSSubscription> subscriptions = subscriptionHandler.listSubscriptionsByTopic(nextToken, topicArn, null);
    			
    			if (subscriptions.size() > 0) {
    				nextToken = subscriptions.get(subscriptions.size()-1).getArn();
    			} else {
    				nextToken = null;
    			}

    			for (CNSSubscription s : subscriptions) {
    				if (s.isConfirmed()) {
    					allSubscriptions.add(s);
    				}
    			}
    			
    			round++;
    		}
    		
    		return allSubscriptions;
        }
    }

    public static List<CNSSubscription> getConfirmedSubscriptions(String topicArn) throws Exception {

    	List<CNSSubscription> subscriptions = null;

		try {
			subscriptions = confirmedSubscriptionsCache.getAndSetIfNotPresent(topicArn, new SubscriptionCallable(topicArn), CMBProperties.getInstance().getCNSCacheExpiring() * 1000);
        } catch (CacheFullException ex1) {
        	subscriptions = new SubscriptionCallable(topicArn).call();
        } catch (Exception ex2) {
        	subscriptions = null;
        }
        
        return subscriptions;
    }
}
