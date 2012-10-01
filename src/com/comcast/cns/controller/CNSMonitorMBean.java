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

import java.util.Map;

/**
 * Interface for monitoring CNS
 * @author aseem
 *
 */
public interface CNSMonitorMBean {

        
    /**
     * @return total number of publishMessage() called across all topics in a rolling window
     */
    public int getNumPublishedMessages();
    
    /**
     * @return number of http connections in the pool for HTTP publisher
     */    
    public int getNumPooledHttpConnections();
    
    /**
     * 
     * @return total number of publish jobs for which at least one endpoint is not yet succesfully published
     *  but we are still trying
     */
    public int getPendingPublishJobs();
    
    /**
     * 
     * @return map of endpointURL -> failure-rate-percentage, numTries, Topic1, Topic2,..
     */
    Map<String, String> getErrorRateForEndpoints();
    
    /**
     * Clear all bad endpoint state
     */
    public void clearBadEndpointsState();
    
    /**
     * 
     * @return size of delivery handler queue
     */
    public int getDeliveryQueueSize();
    
    /**
     * 
     * @return size of redelivery handler queue
     */
    public int getRedeliveryQueueSize();
    
    /**
     * 
     * @return true if consumer is no longer taking new ep-publish-jobs off the queue
     * because its too busy. false otherwise
     */
    public boolean isConsumerOverloaded();
}
