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
package com.comcast.cmb.common.util;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

/**
 * This class is a highly concurrent cache used to cache key, value pairs with an expiration
 * on the value. 
 * The class is thread-safe
 * K - the key type
 * V - the Value type
 * 
 * @author aseem
 * Class ia thread-safe
 */
public final class ExpiringCache<K, V> {    
    
    private static final Logger logger = Logger.getLogger(ExpiringCache.class);    
    private final int cacheKeysLimit;
    /**
     * @param cacheKeysLimit The maximum number of keys in the cache
     */
    public ExpiringCache(int cacheKeysLimit) {
        this.cacheKeysLimit = cacheKeysLimit;
    }
    
    private class ValueContainer {
        final FutureTask<V> future;
        final long createdTimestamp;
        final int exp;
        public ValueContainer(FutureTask<V> val, long ts, int expP) {
            future = val; createdTimestamp = ts; exp = expP;
        }
    }
         
    private ConcurrentHashMap<K, ValueContainer> cache = new ConcurrentHashMap<K, ValueContainer>();
    
    /**
     * Clean up the expired key values.
     * This operation has complexity O(n) where n is the number of key, value pairs stored in the
     * cache.
     * Note: THis method is not synchronized with the getAndSetIfNotPresent. Its possible for a race condition
     * where we evict something from the cache that was just recently set in cache. That's an acceptable tradeoff
     * This is why we try and minimize calls to cleanup
     */
    private void cleanup() {
        for (Map.Entry<K, ValueContainer> entry : cache.entrySet()) {
            if (entry.getValue().createdTimestamp + entry.getValue().exp < System.currentTimeMillis()) {
                cache.remove(entry.getKey());
            }
        }
    }
    
    /**
     * @param key
     * @return true if cache contains key whose value has not yet expired, false otherwise
     */
    public boolean containsKey(K key) {
        ValueContainer existingValContainer = cache.get(key);
        if (existingValContainer != null) {
            if (existingValContainer.createdTimestamp + existingValContainer.exp > System.currentTimeMillis()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Thrown when cache reaches its configured limit
     */
    @SuppressWarnings("serial")
    public static class CacheFullException extends Exception {}
    
    /**
     * 
     * @param key THe key 
     * @param valueGetter The Caller that will get the V value if none is cached or if previous one expired
     * @param exp THe expiration time in milliseconds
     * @return The Value V that was cached ir that just got cached.
     * @throws InterruptedException
     * @throws ExecutionException
     * Note: method will block if we need to call valueGetter
     * Note: THere is a window where t1 could add a new future-task and t2 could be removing it
     */
    public V getAndSetIfNotPresent(K key, Callable<V> valueGetter, int exp) throws CacheFullException {
        
        ValueContainer existingValContainer = cache.get(key);
        if (existingValContainer != null) {
            if (existingValContainer.createdTimestamp + existingValContainer.exp < System.currentTimeMillis()) {
                //expired
                FutureTask<V> ft = new FutureTask<V>(valueGetter);
                ValueContainer valContainer = new ValueContainer(ft, System.currentTimeMillis(), exp);
                if (cache.replace(key, existingValContainer, valContainer)) { //was replaced
                    valContainer.future.run();
                    try {
                        return valContainer.future.get();
                    } catch (Exception e) {
                        logger.error("event=no_value_getter", e);
                        throw new IllegalStateException("Could not get value from valueGetter", e);
                    }
                } else {
                    //someone beat us to it. Lets do this again
                    return getAndSetIfNotPresent(key, valueGetter, exp);
                }
            } else {
                try {
                    return existingValContainer.future.get();
                } catch (Exception e) {
                    logger.error("event=no_value_getter_from_cache", e);
                    throw new IllegalStateException("Could not get value from existing cache entry", e);
                }
            }
        }
        
        //Below is code when no ValueGetter exists for key
        if (cache.size() >= cacheKeysLimit) {
            cleanup();
            throw new CacheFullException();
        }
        
        FutureTask<V> ft = new FutureTask<V>(valueGetter);
        ValueContainer valContainer = new ValueContainer(ft, System.currentTimeMillis(), exp);

        existingValContainer = cache.putIfAbsent(key, valContainer);
        if (existingValContainer == null) { //none previously existing
            valContainer.future.run();
            existingValContainer = valContainer;
        }
        try {
            return existingValContainer.future.get(); //will block till valueGetter returns a value
        } catch (Exception e) {
            logger.error("event=no_value_getter_from_callable", e);
            throw new IllegalStateException("Could not get value from user passed Callable:" + valueGetter, e);
        } 
    }
}
