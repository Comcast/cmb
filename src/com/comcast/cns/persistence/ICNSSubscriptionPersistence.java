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

import java.util.List;

import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;

/**
 * Interface representing persistence functionality for subscriptions
 * @author bwolf, tina, jorge
 *
 */
public interface ICNSSubscriptionPersistence {

	/**
	 * Make a subscription request. Initially the subscription will be unconfirmed / inactive. CNS will automatically send a 
	 * confirmation request containing a token to the specified end point using the desired protocol. It is the client's responsibility 
	 * to parse out the token and call confirmSubscritpion. If topic owner and subscription owner are the same no confirmation 
	 * is necessary and no request will be sent.
	 * @param endpoint end point
	 * @param protocol HTTP, HTTPS, EMAIL, EMAIL-JASON, CQS
	 * @param topicArn topic arn
	 * @param userId user id
	 * @return
	 * @throws Exception
	 */
	public CNSSubscription subscribe(String endpoint, CnsSubscriptionProtocol protocol, String topicArn, String userId) throws Exception;
	
	/**
	 * Get a single subscription object by arn. Not part of official AWS API.
	 * @param arn subscription arn
	 * @param userId user id
	 * @return subscription
	 * @throws Exception
	 */
	public CNSSubscription getSubscription(String arn) throws Exception;
	
	/**
	 * List all subscription for a user, unconfirmed subscriptions will not reveal their arns. Pagination for more than 100 subscriptions.
	 * @param nextToken initially null, on subsequent calls arn of last result from prior call
	 * @param protocol optional filter by protocol (this parameter is not part of official AWS API)
	 * @return list of subscriptions
	 * @throws Exception
	 */
	public List<CNSSubscription> listSubscriptions(String nextToken, CnsSubscriptionProtocol protocol, String userId) throws Exception;
	
	/**
	 * List all subscriptions for a user including unconfirmed subscriptions. Pagination for more than 100 subscriptions. Not part of official AWS API.
	 * Not part of official AWS API.
	 * @param nextToken
	 * @param protocol optional filter by protocol (this parameter is not part of official AWS API)
	 * @param userId
	 * @return list of subscriptions
	 * @throws Exception
	 */
	public List<CNSSubscription> listAllSubscriptions(String nextToken, CnsSubscriptionProtocol protocol, String userId) throws Exception;
	
	/**
	 * List all active subscriptions for a topic, unconfirmed subscriptions will not reveal their arns. Pagination for more than 100 subscriptions.
	 * @param nextToken initially null, on subsequent calls arn of last result from prior call
	 * @param topicArn topic arn 
	 * @param protocol optional filter by protocol (this parameter is not part of official AWS API)
	 * @param userId user id
	 * @return list of subscriptions
	 * @throws Exception
	 */
	public List<CNSSubscription> listSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol) throws Exception;
	
	/**
	 * List all active subscriptions for a topic, unconfirmed subscriptions will not reveal their arns. Page size is configurable.
	 * @param nextToken initially null, on subsequent calls arn of last result from prior call
	 * @param topicArn topic arn 
	 * @param protocol optional filter by protocol (this parameter is not part of official AWS API)
	 * @param userId user id
	 * @param pageSize maximum number of subscriptions to return
	 * @return list of subscriptions
	 * @throws Exception
	 */
	public List<CNSSubscription> listSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol, int pageSize) throws Exception;

	/**
	 * List all subscriptions for a topic including unconfirmed subscriptions. Pagination for more than 100 subscriptions. Not part of official AWS API.
	 * @param nextToken initially null, on subsequent calls arn of last result from prior call
	 * @param topicArn topic arn
	 * @param protocol optional filter by protocol (this parameter is not part of official AWS API)
	 * @param userId user id
	 * @return list of subscriptions
	 * @throws Exception
	 */
	public List<CNSSubscription> listAllSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol) throws Exception;
	
	/**
	 * Confirm subscription to make it active. Only confirmed subscriptions will receive messages.
	 * @param authenticateOnUnsubscribe define if unsubscribe will require authentication
	 * @param token token from confirmation request
	 * @param topicArn topic arn
	 * @param userId user id
	 * @return subscription
	 * @throws Exception
	 */
	public CNSSubscription confirmSubscription(boolean authenticateOnUnsubscribe, String token, String topicArn) throws Exception;
	
	/**
	 * Unsubscribe from a topic.
	 * @param arn subscription arn
	 * @throws Exception
	 */
	public void unsubscribe(String arn) throws Exception;
	
	/**
	 * Delete all subscribers for a topic. Called by deleteTopic.
	 * @param topicArn 
	 * @throws Exception 
	 */
	public void unsubscribeAll(String topicArn) throws Exception;
	
	/**
	 * @param topicArn
	 * @param columnName
	 * @return
	 * @throws Exception
	 */
	
	public long getCountSubscription(String topicArn, String columnName) throws Exception;
}
