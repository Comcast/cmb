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

import org.apache.log4j.Logger;

/**
 * This class helps accumulate long values associated with various keys per given VM process
 * This class is mostly used for accumulating db time to serve a single request.
 * @author aseem
 * Class is thread-safe
 */
public final class ValueAccumulator {

	private static Logger logger = Logger.getLogger(ValueAccumulator.class);
    
    public enum AccumulatorName {
        CassandraTime,
        CassandraWrite,
        CassandraRead,
        RedisTime,
        CQSMonitorTime,
        SendMessageArgumentCheck,
        SendMessageXMLTime,
        CMBControllerPreHandleAction,
        CQSPreDoAction,
        CNSCQSTime,
        CNSPublishSendTime,
        UnitTestTime;
        
    }
    static AccumulatorName[] accumulatorNameValues = AccumulatorName.values();

    private final ThreadLocal<long[]> keyToValue = new ThreadLocal<long[]> () {
        @Override
        protected long[] initialValue() {
            long []arr = new long[accumulatorNameValues.length];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = -1L;
            }
            return arr;
        }        
    };
    
    /**
     * Initialize the counter for key. Should be called at the begenning of servicing a request
     * @param key
     */
    public void initializeCounters(AccumulatorName ...keys) {
        for (AccumulatorName key : keys) {
            if (keyToValue.get()[key.ordinal()] != -1L) {
                //logger.warn("initializeCounter called but on an in-use counter. Please call deleteCounter()");
            }
            keyToValue.get()[key.ordinal()] = 0L;
        }
    }
    
    public void initializeAllCounters() {
        for (AccumulatorName key : accumulatorNameValues) {
            if (keyToValue.get()[key.ordinal()] != -1L) {
                //logger.warn("initializeCounter called but on an in-use counter. Please call deleteCounter()");
            }
            keyToValue.get()[key.ordinal()] = 0L;
        }
    }
    
    /**
     * Accumulate time in the key bucket
     * @param key the bucket to accumulate time in
     * @param ammount the ammount to accumulate
     */
    public void addToCounter(AccumulatorName key, long ammount){
        long val = keyToValue.get()[key.ordinal()];
        if (val == -1L) {
            //logger.warn("Must initialize Accumulator before adding to counter with key=" + key + ", not adding to the counter the new ammount=" + ammount);
            return;
        }
        keyToValue.get()[key.ordinal()] = val + ammount;
    }
    
    /**
     * Get accumulated time or -1 if doesn't exist
     * @param key
     * @return
     */
    public long getCounter(AccumulatorName key) {
        return keyToValue.get()[key.ordinal()];
    }
    
    /**
     * Clear the counter for this key. Must be called when we are done with one 
     * HTTP request
     * @param key
     */
    public void deleteCounter(AccumulatorName key) {
        keyToValue.get()[key.ordinal()] = -1L;
    }
    
    /**
     * Clear counters for all keys
     */
    public void deleteAllCounters() {
        for (AccumulatorName key : accumulatorNameValues) {
            keyToValue.get()[key.ordinal()] = -1L;
        }
    }
}
