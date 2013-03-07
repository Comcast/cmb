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
package com.comcast.cqs.persistence;

import java.util.List;

/**
 * 
 * interface provides functionality to fetch ordered list of messageids. Used by
 * Payload cache to interact with the Mem-Q cache.
 * @author aseem
 */
public interface ICQSMessagePersistenceIdSequence {
    /**
     * @param queueUrl
     * @param num number of message-ids to return
     * @return message ids from the head of the queue. If data not available, empty list is returned
     */
    public List<String> getIdsFromHead(String queueUrl, int num);
    
    /**
     * @param queueUrl
     * @return number of messages in queue
     */
    public long getQueueMessageCount(String queueUrl);
}
