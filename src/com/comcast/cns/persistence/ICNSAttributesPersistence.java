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

import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSTopicAttributes;

/**
 * Interface representing persistence functionality for topic attributes
 * @author tina, bwolf
 *
 */
public interface ICNSAttributesPersistence {
	
	/**
	 * setTopicAttributes
	 * @param attributeName
	 * @param attributeValue
	 * @param topicArn
	 * @return 
	 * @throws Exception
	 */
	public void setTopicAttributes(CNSTopicAttributes topicAttributes, String topicArn) throws Exception;
	
	/**
	 * getTopicAttributes
	 * @param topicArn
	 * @return CNSTopicAttributes
	 * @throws Exception
	 */
	public CNSTopicAttributes getTopicAttributes(String topicArn) throws Exception;
	
	
	/**
	 * setSubscriptionAttributes
	 * @param attributeName
	 * @param attributeValue
	 * @param subscriptionArn
	 * @return
	 * @throws Exception
	 */
	public void setSubscriptionAttributes(CNSSubscriptionAttributes subscriptionAtributes, String subscriptionArn) throws Exception;
	
	/**
	 * getSubscriptionAttributes
	 * @param subscriptionArn
	 * @return CNSSubscriptionAttributes
	 * @throws Exception
	 */
	public CNSSubscriptionAttributes getSubscriptionAttributes(String subscriptionArn) throws Exception;
}
