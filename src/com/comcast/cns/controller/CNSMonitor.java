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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.RollingWindowCapture;
import com.comcast.cns.io.HTTPEndpointPublisherApache;
import com.comcast.cns.tools.CNSEndpointPublisherJobConsumer;

/**
 * The implementation of monitoring for CNS.
 * @author aseem
 * Class is thread-safe
 */

public class CNSMonitor implements CNSMonitorMBean {

	private static final CNSMonitor Inst = new CNSMonitor();
	private CNSMonitor() {}
    public static CNSMonitor getInstance() {return Inst;}
        
    public ConcurrentHashMap<String, AtomicInteger> messageIdToSendRemaining = new ConcurrentHashMap<String, AtomicInteger>();
    
    private class BadEndpointInfo {
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger tries = new AtomicInteger();
        ConcurrentHashMap<String, Boolean> topics = new ConcurrentHashMap<String, Boolean>(); 
    }
    
    private ConcurrentHashMap<String, BadEndpointInfo> badEndpoints = new ConcurrentHashMap<String, CNSMonitor.BadEndpointInfo>();

    private class GenericEvent extends RollingWindowCapture.PayLoad{
        final AtomicInteger num = new AtomicInteger(1);
        final long timeStamp = System.currentTimeMillis();
    }
    private class CountMessagesVisitor implements RollingWindowCapture.Visitor<GenericEvent> {
        int num = 0;
        public void processNode(GenericEvent n){
            num += n.num.get();
        }
    }
    
    private RollingWindowCapture<GenericEvent> publishMsgRW = new RollingWindowCapture<GenericEvent>(CMBProperties.getInstance().getRollingWindowTimeSec(), 10000);


    @Override
    public int getNumPublishedMessages() {
        CountMessagesVisitor c = new CountMessagesVisitor();
        publishMsgRW.visitAllNodes(c);
        return c.num;
    }

    @Override
    public int getNumPooledHttpConnections() {
        return HTTPEndpointPublisherApache.cm.getConnectionsInPool();
    }
    
    
    public void registerPublishMessage() {
        GenericEvent currentHead = publishMsgRW.getLatestPayload();
        if (currentHead == null || System.currentTimeMillis() - currentHead.timeStamp > 60000L) {
            publishMsgRW.addNow(new GenericEvent());
        } else {
            currentHead.num.incrementAndGet();
        }
    }


    public void registerSendsRemaining(String messageId, int remaining) {
        AtomicInteger newVal = new AtomicInteger();
        AtomicInteger oldVal = messageIdToSendRemaining.putIfAbsent(messageId, newVal);
        if (oldVal != null) {
            newVal = oldVal;
        }
        newVal.addAndGet(remaining);
    }
    
    @Override
    public int getPendingPublishJobs() {
        int numNonZero = 0;
        for (Map.Entry<String, AtomicInteger> entry : messageIdToSendRemaining.entrySet()) {
            if (entry.getValue().get() != 0) {
                numNonZero++;
            }
        }
        return numNonZero;
    }

    public void clearBadEndpointsState() {
        badEndpoints.clear();
    }
    /**
     * Only capture stats for endpoints that have previous errors.
     * @param endpoint
     * @param errors
     * @param numTries
     * @param topic
     */
    public void registerBadEndpoint(String endpoint, int errors, int numTries, String topic) {
        if (!badEndpoints.containsKey(endpoint) && errors == 0) {
            return; //no errirs for this endpoint. Its good. return
        }
        BadEndpointInfo newVal = new BadEndpointInfo();
        BadEndpointInfo oldVal = badEndpoints.putIfAbsent(endpoint, newVal);
        if (oldVal != null) {
            newVal = oldVal;
        }
        newVal.errors.addAndGet(errors);
        newVal.tries.addAndGet(numTries);
        newVal.topics.putIfAbsent(topic, Boolean.TRUE);
    }

    @Override
    /**
     * @return map of endpointURL -> failure-rate, numTries, List<Topics>
     */
    public Map<String, String> getErrorRateForEndpoints() {
        HashMap<String, String> ret = new HashMap<String, String>();
        for (Map.Entry<String, BadEndpointInfo> entry : badEndpoints.entrySet()) {
            String endpoint = entry.getKey();
            BadEndpointInfo val = entry.getValue();
            StringBuffer sb = new StringBuffer();
            int failureRate = (val.tries.get() > 0) ? (int) (((float) val.errors.get() / (float) val.tries.get()) * 100) : 0;
            sb.append("failureRate=").append(failureRate).append(",numTries=").append(val.tries.get()).append(",topics=");
            Enumeration<String> topics = val.topics.keys();
            while (topics.hasMoreElements()) {
                sb.append(",").append(topics.nextElement());
            }
            ret.put(endpoint, sb.toString());
        }
        return ret;
    }

    @Override
    public int getDeliveryQueueSize() {
        return CNSEndpointPublisherJobConsumer.MonitoringInterface.getDeliveryHandlersQueueSize();
    }

    @Override
    public int getRedeliveryQueueSize() {
        return CNSEndpointPublisherJobConsumer.MonitoringInterface.getReDeliveryHandlersQueueSize();
    }

    @Override
    public boolean isConsumerOverloaded() {
        return CNSEndpointPublisherJobConsumer.isOverloaded();
    }

}
