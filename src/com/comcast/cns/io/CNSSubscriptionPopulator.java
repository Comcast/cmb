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
package com.comcast.cns.io;

import java.util.List;

import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSSubscription;

/**
 * Class to generate API response for subscriptions
 * @author jorge, bwolf
 *
 */
public class CNSSubscriptionPopulator {

	public static String getSubscribeResponse(String subscriptionArn) {
		
		String res = "<SubscribeResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" +
		"<SubscribeResult>" +
		"<SubscriptionArn>" + subscriptionArn + "</SubscriptionArn>" + 
		"</SubscribeResult>" +
		CNSPopulator.getResponseMetadata() +
		"</SubscribeResponse>";
		
		return res;
	}

	private static String printSubscription(CNSSubscription subscription) {
		
		String res = "<member>" +
		"<TopicArn>"+ subscription.getTopicArn() +"</TopicArn>" +
		"<Protocol>"+ subscription.getProtocol() +"</Protocol>" +
		"<SubscriptionArn>"+ subscription.getArn() +"</SubscriptionArn>" +
		"<Owner>"+ subscription.getUserId() +"</Owner>" +
		"<Endpoint>"+ subscription.getEndpoint() +"</Endpoint>" +
		"</member>";
		
		return res;
	}

	public static String getListSubscriptionResponse(List<CNSSubscription> subscriptions, String nextToken) {
		
		String res = "<ListSubscriptionsResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n" +
		"\t<ListSubscriptionsResult>\n" +
		"\t\t<Subscriptions>\n";
		
		for (CNSSubscription sub: subscriptions) {
			res += "\t\t\t" + printSubscription(sub) + "\n";
		}
		
		res += "\t\t</Subscriptions>\n";

		if (nextToken != null) {
			res += "\t\t<NextToken>" + nextToken + "</NextToken>\n";
		}
		
		res += "\t</ListSubscriptionsResult>\n" +
		CNSPopulator.getResponseMetadata() + "\n" +
		"</ListSubscriptionsResponse>\n";

		return res;
	}

	public static String getUnsubscribeResponse() {
		return "<UnsubscribeResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" +
		CNSPopulator.getResponseMetadata() +	
		"</UnsubscribeResponse>";
	}

	public static String getListSubscriptionByTopicResponse(List<CNSSubscription> subscriptions, String nextToken) {
		
		String res = "<ListSubscriptionsByTopicResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n" +
		"\t<ListSubscriptionsByTopicResult>\n" +
		"\t\t<Subscriptions>\n";
		
		for (CNSSubscription sub: subscriptions) {
			res += "\t\t\t" +printSubscription(sub) + "\n"; 
		}
		
		res += "\t\t</Subscriptions>\n";

		if (nextToken != null) {
			res += "\t\t<NextToken>" + nextToken + "</NextToken>\n";
		}
		
		res += "\t</ListSubscriptionsByTopicResult>\n" +
		CNSPopulator.getResponseMetadata() + "\n" +
		"</ListSubscriptionsByTopicResponse>\n";

		return res;
	}

	public static String getConfirmSubscriptionResponse(CNSSubscription subscription) {
		
		return "<ConfirmSubscriptionResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" +
		"<ConfirmSubscriptionResult>" +
		"<SubscriptionArn>" + subscription.getArn() + "</SubscriptionArn>" +
		"</ConfirmSubscriptionResult>" +
		CNSPopulator.getResponseMetadata() +
		"</ConfirmSubscriptionResponse>";
	}

	public static String getPublishResponse(CNSMessage cnsMessage){
		return "<PublishResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" +
		"<PublishResult>" +
		"<MessageId>" + cnsMessage.getMessageId() + "</MessageId>" +
		"</PublishResult>" +
		CNSPopulator.getResponseMetadata() +
		"</PublishResponse>";
	}
}