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
package com.comcast.cns.tools;

import org.apache.log4j.Logger;

/**
 * This class represents Thread that backoff when no message exists in all the partitions
 * @author aseem 
 */
public class CNSPublisherJobThread extends Thread {
    private static Logger logger = Logger.getLogger(CNSPublisherJobThread.class);
    
    private final CNSPublisherPartitionRunnable runnable;
    private final int numPartitions;
    private final long maxDelayMS;
    
    public CNSPublisherJobThread(String threadName, CNSPublisherPartitionRunnable runnable, int numPartitions, long maxDelayMS) {
        super(threadName);
        this.runnable = runnable;             
        this.numPartitions = numPartitions;
        this.maxDelayMS = maxDelayMS;
    }
    
    @Override
    public void run() {
        long sleepAmount = 10;
        while (true) {
            boolean messageFoundInFullPass = false; 
            for (int i = 0; i < numPartitions; i++) {
                if (runnable.run(i)) {
                    messageFoundInFullPass = true;
                }
            }
            
            if (!messageFoundInFullPass) {
                if (sleepAmount * 2 < maxDelayMS) {
                    sleepAmount *= 2;
                } else {
                    sleepAmount = maxDelayMS;
                }
                
                try {
                    logger.debug("event=run messageFoundInFullPass=true sleepAmount=" + sleepAmount);
                    sleep(sleepAmount);
                } catch (InterruptedException e) {
                    logger.error("Could not put thread to sleep", e);
                }
            } else {
                sleepAmount = 10;
            }
        }
    }
}

