package com.comcast.cqs.controller;

import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ExpiringCache;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ExpiringCache.CacheFullException;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.ICQSQueuePersistence;
import com.comcast.cqs.util.CQSErrorCodes;

public class CQSCache {

    protected static ExpiringCache<String, CQSQueue> queueCache = new ExpiringCache<String, CQSQueue>(CMBProperties.getInstance().getCQSCacheSizeLimit());
    protected static volatile ICQSQueuePersistence queuePersistence = PersistenceFactory.getQueuePersistence();
    
    private static Logger logger = Logger.getLogger(CQSCache.class);

    public static class QueueCallable implements Callable<CQSQueue> {
        
    	String queueUrl = null;
        
    	public QueueCallable(String key) {
            this.queueUrl = key;
        }
        
    	@Override
        public CQSQueue call() throws Exception {
            CQSQueue queue = queuePersistence.getQueue(queueUrl);
            return queue;
        }
    }
    
    /**
     * 
     * @param relativeQueueUrl
     */
    public static void removeQueue(String relativeQueueUrl) {
    	queueCache.remove(relativeQueueUrl);
    }
    
    /**
     * 
     * @param relativeQueueUrl
     * @return Cached instance of CQSQueue given queueUrl. If none exists, we call QueueCallable and cache it
     * for config amount of time
     * @throws Exception
     */
    public static CQSQueue getCachedQueue(String relativeQueueUrl) throws Exception {
        try {
            return queueCache.getAndSetIfNotPresent(relativeQueueUrl, new QueueCallable(relativeQueueUrl), CMBProperties.getInstance().getCQSCacheExpiring() * 1000);
        } catch (CacheFullException e) {
            return new QueueCallable(relativeQueueUrl).call();
        } 
    }
    
    /**
     * Get a CQSQueue instance given the request and user. If no queue is found, Exception is thrown
     * @param user
     * @param request
     * @return
     * @throws Exception
     */
    public static CQSQueue getCachedQueue(User user, HttpServletRequest request) throws Exception {
    	
        String queueUrl = null;
        CQSQueue queue = null;

        queueUrl = request.getRequestURL().toString();

        if (queueUrl != null && !queueUrl.equals("") && !queueUrl.equals("/")) {

            if (queueUrl.startsWith("http://")) {

                queueUrl = queueUrl.substring("http://".length());

                if (queueUrl.contains("/")) {
                    queueUrl = queueUrl.substring(queueUrl.indexOf("/"));
                }

            } else if (queueUrl.startsWith("https://")) {

                queueUrl = queueUrl.substring("https://".length());

                if (queueUrl.contains("/")) {
                    queueUrl = queueUrl.substring(queueUrl.indexOf("/"));
                }
            } 

            if (queueUrl.startsWith("/")) {
                queueUrl = queueUrl.substring(1);
            }

            if (queueUrl.endsWith("/")) {
                queueUrl = queueUrl.substring(0, queueUrl.length()-1);
            }

            // auto prefix with user id if omitted in request

            if (!queueUrl.contains("/") && user != null) {
                queueUrl = user.getUserId() + "/" + queueUrl;
            }

            queue = getCachedQueue(queueUrl);

            if (queue == null) {
                throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue with url " + request.getRequestURL().toString() + " doesn't exist");
            }
            
        } else {
            throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue with url " + request.getRequestURL().toString() + " doesn't exist");
        }
        
        return queue;
    }
}
