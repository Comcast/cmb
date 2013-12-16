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
package com.comcast.cns.persistence;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.apache.log4j.Logger;

import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.ExpiringCache;
import com.comcast.cmb.common.util.ExpiringCache.CacheFullException;
import com.comcast.cns.model.CNSEndpointPublishJob;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.util.Util;

/**
 * This class supports caching of the subinfo in memory
 * The static cache uses topicArn as the key for the cache. The value is a concurrent-hashmap of subArn->subInfo
 * {{topicArn, exp}->{subArn->{subInfo}}}
 * 
 * @author aseem
 * 
 * Class is thread-safe
 */
public class CNSCachedEndpointPublishJob extends CNSEndpointPublishJob {
	
    private static Logger logger = Logger.getLogger(CNSCachedEndpointPublishJob.class);
    private static final ExpiringCache<String, LinkedHashMap<String, CNSCachedEndpointSubscriptionInfo>> cache = new ExpiringCache<String, LinkedHashMap<String,CNSCachedEndpointSubscriptionInfo>>(1000);
    
    /**
     * 
     * @param topicArn
     * @return THh list of sub-infos
     * @throws Exception 
     */
    public static List<CNSCachedEndpointSubscriptionInfo> getSubInfos(String topicArn) throws Exception {
        LinkedHashMap<String, CNSCachedEndpointSubscriptionInfo> arnToSubInfo;
        try {
            arnToSubInfo = cache.getAndSetIfNotPresent(topicArn, new CachePopulator(topicArn), 60000);
        } catch (CacheFullException e) {
            arnToSubInfo = new CachePopulator(topicArn).call();
        } catch(IllegalStateException e) {
            if ((e.getCause() instanceof ExecutionException) && 
                (e.getCause().getCause() instanceof TopicNotFoundException)) {
                throw new TopicNotFoundException("Topic Not Found:" + topicArn);
            } else {
                throw e;
            }
        }
        return new LinkedList<CNSCachedEndpointSubscriptionInfo>(arnToSubInfo.values());
    }
    
    public static class CNSCachedEndpointSubscriptionInfo extends CNSEndpointSubscriptionInfo {

        public CNSCachedEndpointSubscriptionInfo(CnsSubscriptionProtocol protocol, String endpoint, String subArn, boolean rawDelivery) {
            super(protocol, endpoint, subArn, rawDelivery);
        }
        
        public CNSCachedEndpointSubscriptionInfo(CNSEndpointSubscriptionInfo info) {
            super(info.protocol, info.endpoint, info.subArn, info.rawDelivery);
        }
        
        @Override
        public String serialize() {
            return subArn;
        }

        public static CNSCachedEndpointSubscriptionInfo parseInstance(String str) {
            return new CNSCachedEndpointSubscriptionInfo(CNSEndpointSubscriptionInfo.parseInstance(str));
        }                
        /**
         * 
         * @param arr array of serialized CachedSubInfo objects (or its identifiers)
         * @param idx start index
         * @param count num to read
         * @return list of fully populated SubInfos in random order
         * @throws Exception 
         */
        public static List<CNSCachedEndpointSubscriptionInfo> parseInstances(String topicArn, String []arr, int idx, int count, boolean useCache) throws Exception {

        	if (count == 0) {
            	return Collections.emptyList();
            }
            
            List<CNSCachedEndpointSubscriptionInfo> infos = new LinkedList<CNSCachedEndpointPublishJob.CNSCachedEndpointSubscriptionInfo>();

            if (useCache) {
                HashMap<String, CNSCachedEndpointSubscriptionInfo> arnToSubInfo;
                try {
                    arnToSubInfo = cache.getAndSetIfNotPresent(topicArn, new CachePopulator(topicArn), 60000);
                } catch (CacheFullException e) {
                    arnToSubInfo = new CachePopulator(topicArn).call();
                } catch(IllegalStateException e) {
                    if ((e.getCause() instanceof ExecutionException) && 
                        (e.getCause().getCause() instanceof TopicNotFoundException)) {
                    	logger.warn("event=topic_not_found topic_arn=" + topicArn);
                        throw new TopicNotFoundException("Topic Not Found:" + topicArn);
                    } else {
                        throw e;
                    }
                }
                
                for (int i = idx; i < idx + count ; i++) {
                    String subArn = arr[i];
                    CNSCachedEndpointSubscriptionInfo subInfo = arnToSubInfo.get(subArn);
                    if (subInfo == null) {
                        logger.warn("event=subscription_info_not_found arn=" + subArn + " topic_arn=" + topicArn);
                    } else {
                        infos.add(subInfo);
                    }
                }                
            } else {
                //get from Cassandra                
                ICNSSubscriptionPersistence subscriptionPersistence = PersistenceFactory.getSubscriptionPersistence();
                for (int i = idx; i < idx + count ; i++) {
                    String subArn = arr[i];
                    CNSSubscription sub = subscriptionPersistence.getSubscription(subArn);
                	//TODO: store raw delivery flag as part of the subscription for better performance
                    if (sub == null) {
                        logger.warn("event=subscription_info_not_found arn=" + subArn + " topic_arn=" + topicArn);
                    } else {
                        infos.add(new CNSCachedEndpointSubscriptionInfo(sub.getProtocol(), sub.getEndpoint(), sub.getArn(), sub.getRawMessageDelivery()));
                    }
                }                
            }
            return infos;
        }
    }
    
    public CNSCachedEndpointPublishJob(CNSMessage message, List<? extends CNSEndpointSubscriptionInfo> subInfos) {
        super(message, subInfos);
    }
    
    /**
     * Class responsible for returning the contents of the cache if cache miss occurs
     */
    private static class CachePopulator implements Callable<LinkedHashMap<String,CNSCachedEndpointSubscriptionInfo>> {
        final String topicArn;
        public CachePopulator(String topicArn) {
            this.topicArn = topicArn;
        }
        @Override
        public LinkedHashMap<String, CNSCachedEndpointSubscriptionInfo> call() throws Exception {
            long ts1 = System.currentTimeMillis();
            ICNSSubscriptionPersistence subscriptionPersistence = PersistenceFactory.getSubscriptionPersistence();
            List<CNSSubscription> subs = null;
    		String nextToken = null;
            LinkedHashMap<String, CNSCachedEndpointSubscriptionInfo> val = new LinkedHashMap<String, CNSCachedEndpointPublishJob.CNSCachedEndpointSubscriptionInfo>();
    		while (true) {
    			//get subscription by page
    			subs = subscriptionPersistence.listSubscriptionsByTopic(nextToken, topicArn, null, 1000);
    	        
    			if (subs == null || subs.size() == 0) {
    	            break;
    	        }
    	        
                for (CNSSubscription sub : subs) {
                    if (!sub.getArn().equals("PendingConfirmation")) {
                        val.put(sub.getArn(), new CNSCachedEndpointSubscriptionInfo(sub.getProtocol(), sub.getEndpoint(), sub.getArn(), sub.getRawMessageDelivery()));
                        nextToken = sub.getArn();
                    }
                    
                } 	        
    			   			
    	    }

            long ts2 = System.currentTimeMillis();
            logger.debug("event=populated_subscription_cache topic_arn=" + topicArn + " res_ts=" + (ts2 - ts1));
            return val;
        }        
    }
    
    /**
     * Construct a CNSEndpointPublishJob given its string representation
     * @param str
     * @return
     * @throws CMBException
     */
    public static CNSEndpointPublishJob parseInstance(String str) throws CMBException {
        String arr[] = str.split("\n");
        if (arr.length < 2) {
            throw new IllegalArgumentException("Expected at least two tokens in CNSEndpointPublishJob serial representation. Expect4ed <num-subinfos>\n[<sub-info>\n<sub-info>...\n]<CNSPublishJob>. Got:" + str);
        }
        int numSubInfos = Integer.parseInt(arr[0]);   
        int idx = 1;
        
        //Note: We assume that topic-ARN is part of the sub-arn
        
        String topicArn = null;
        boolean useCache = true;
        if (numSubInfos > 0) {
            topicArn = Util.getCnsTopicArn(arr[idx]);
        }

        List<CNSCachedEndpointSubscriptionInfo> subInfos;
        try {
            subInfos = CNSCachedEndpointSubscriptionInfo.parseInstances(topicArn, arr, idx, numSubInfos, useCache);
        } catch (TopicNotFoundException e) {
            logger.error("event=parse_publish_job error_code=topic_not_found topic_arn=" + topicArn, e);
            throw e;
        } catch (Exception e) {
            logger.error("event=parse_publish_job", e);
            throw new CMBException(CMBErrorCodes.InternalError, e.getMessage());
        }

        idx += numSubInfos;
        
        StringBuffer sb = new StringBuffer();
        for (int j = idx; j < arr.length; j++) {
            if (j != idx) {
                sb.append("\n");
            }
            sb.append(arr[j]);
        }
        
        CNSMessage message = null;

        try {
            message = CNSMessage.parseInstance(sb.toString());
        } catch(Exception e) {
            logger.error("event=parse_publish_job cnsmessage_serialized=" + sb.toString(), e);
            throw new CMBException(CMBErrorCodes.InternalError, e.getMessage());
        }
        
        return new CNSCachedEndpointPublishJob(message, subInfos);
    }
}
