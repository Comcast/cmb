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
package com.comcast.plaxo.cmb.common.persistence;

import com.comcast.plaxo.cns.persistence.CNSAttributesCassandraPersistence;
import com.comcast.plaxo.cns.persistence.ICNSAttributesPersistence;
import com.comcast.plaxo.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.plaxo.cns.persistence.ICNSTopicPersistence;
import com.comcast.plaxo.cns.persistence.CNSSubscriptionCassandraPersistence;
import com.comcast.plaxo.cns.persistence.CNSTopicCassandraPersistence;
import com.comcast.plaxo.cqs.persistence.ICQSMessagePersistence;
import com.comcast.plaxo.cqs.persistence.ICQSQueuePersistence;
import com.comcast.plaxo.cqs.persistence.CQSQueueCassandraPersistence;
import com.comcast.plaxo.cqs.persistence.RedisCachedCassandraPersistence;


/**
 * The factory to create appt implementations of the various persistence interfaces
 * @author bwolf, aseem, jorge, baosen
 */
public class PersistenceFactory {
    /**
     * Note: The attributes are made public and non-final only for unit-test purposes.
     * Non-unit-test code should only call accessors.
     */
	public static ICQSQueuePersistence cqsQueuePersistence = new CQSQueueCassandraPersistence();
	public static ICNSSubscriptionPersistence cnsSubscriptionPersistence = new CNSSubscriptionCassandraPersistence();
	public static ICNSTopicPersistence cnsTopicPersistence = new CNSTopicCassandraPersistence();
	public static IUserPersistence userPersistence = new UserCassandraPersistence();
	public static ICNSAttributesPersistence cnsAttributePersistence = new CNSAttributesCassandraPersistence(); 
	public static ICQSMessagePersistence cqsMessagePersistence = RedisCachedCassandraPersistence.getInstance();
	
	public static IUserPersistence getUserPersistence() {
		return userPersistence;
	}
	
	public static ICNSTopicPersistence getTopicPersistence() {
		return cnsTopicPersistence;
	}
	
	public static ICNSSubscriptionPersistence getSubscriptionPersistence() {
		return cnsSubscriptionPersistence;
	}
	
	public static ICQSQueuePersistence getQueuePersistence() {
		return cqsQueuePersistence;
	}
	
		
	public static ICNSAttributesPersistence getCNSAttributePersistence() {
	    return cnsAttributePersistence;
	}
	
	public static ICQSMessagePersistence getCQSMessagePersistence() {
		return cqsMessagePersistence;
	}
	
	/**
	 * Only called by unit-tests
	 */
	public static synchronized void reset() {
		cqsQueuePersistence = new CQSQueueCassandraPersistence();
		cnsSubscriptionPersistence = new CNSSubscriptionCassandraPersistence();
		cnsTopicPersistence = new CNSTopicCassandraPersistence();
		userPersistence = new UserCassandraPersistence();
		cnsAttributePersistence = new CNSAttributesCassandraPersistence(); 
		cqsMessagePersistence = RedisCachedCassandraPersistence.getInstance();
	}
}
 