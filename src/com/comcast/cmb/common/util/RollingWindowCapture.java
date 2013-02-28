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

import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * A utility class used to capture objects in a rolling window fashion
 * Note: This class should not be used if there are a lot of adding of events
 * instead callers should get the latest node and modify that in most cases
 * 
 * @author aseem
 * Class is thread-safe
 */
public final class RollingWindowCapture<T extends RollingWindowCapture.PayLoad> {

	private static Logger logger = Logger.getLogger(RollingWindowCapture.class);
    
    private final long _windowSizeSec;
    private final int _tolerance;
    private final AtomicInteger _toleranceCount;

    private class PayLoadNode {
        public final T _payLoad;
        private final long _captureTime;
        public PayLoadNode(T payload, long captureTime) {
            _payLoad = payload;
            _captureTime = captureTime;
        }
    }

    //internally we used a CopyOnWriteArrayList to implement the rolling window in chronological order. 
    //First node is the oldest node
    //We use CopyOnWriteArrayList since its the only standard data-structure provided that offers concurrent
    //access to its elements and provides the List interface, specifically get(index) method. We need this
    //method to return the tail of the rollingwindow.
    
    private CopyOnWriteArrayList<PayLoadNode> _queue = new CopyOnWriteArrayList<PayLoadNode>();
    
    public static interface Visitor<T> {
        public void processNode(T n);
    }

    /**
     * The payl0ad to capture in window
     *
     */
    public static abstract class PayLoad {}
    
    /**
     * 
     * @param windowSizeSec the size of rolling window in seconds
     * @param tolerance the number of adds you can go over before must reduce the window
     */
    public RollingWindowCapture(int windowSizeSec, int tolerance) {
        _windowSizeSec = windowSizeSec;
        _tolerance = tolerance;
        _toleranceCount = new AtomicInteger();
    }
    
    /**
     * Method would traverse the entire list of elements in the window and call
     * the visitor on each node
     * @param v the Visitor impl
     */
    public void visitAllNodes(Visitor<T> v) {
        cleanupWindow();
        Iterator<PayLoadNode> it = _queue.iterator();
        while (it.hasNext()) {
            PayLoadNode node = it.next();
            v.processNode(node._payLoad);
        }
    }
    
    /**
     * Add current time to the header of list
     * Note: This is an expensive operation O(n) complexity where n is the number of elements in the list
     */
    public void addNow(T payLoad) {
        PayLoadNode node = new PayLoadNode(payLoad, System.currentTimeMillis());        
        _queue.add(node);
        if (_toleranceCount.incrementAndGet() >= _tolerance) {
            cleanupWindow();
        }
    }

    /**
     * @return The last added payload or null if no element
     */
    public T getLatestPayload() {
        boolean success = false;
        while (!success) {
            if (_queue.size() == 0) return null;
            try {
                PayLoadNode head = _queue.get(_queue.size() - 1);
                success = true;
                return head._payLoad;
            } catch (IndexOutOfBoundsException e) {}
        }
        return null;
    }
    
    /**
     * Go through the beginning of the queue and remove all expired nodes
     */
    private void cleanupWindow() {
        long now = System.currentTimeMillis();
        Iterator<PayLoadNode> it = _queue.iterator();
        while (it.hasNext()) {
            PayLoadNode node = it.next();
            if (node._captureTime + (_windowSizeSec*1000) < now) {
                if (!_queue.remove(node)) {
                    logger.debug("event=cleanup_window status=queue_did_not_change info=node_may_have_been_deleted");
                }
            } else {
                //found the first node in the capture window. we are done
                _toleranceCount.set(0);
                return;
            }
        }
        _toleranceCount.set(0);
    }
}
