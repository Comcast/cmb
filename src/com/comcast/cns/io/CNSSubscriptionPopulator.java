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
		
		StringBuffer out = new StringBuffer("<SubscribeResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n");
		
		out.append("\t<SubscribeResult>\n");
		out.append("\t\t<SubscriptionArn>" + subscriptionArn + "</SubscriptionArn>\n");
		out.append("\t</SubscribeResult>\n");
		out.append("\t").append(CNSPopulator.getResponseMetadata()).append("\n");
		out.append("</SubscribeResponse>\n");
		
		return out.toString();
	}

	private static String printSubscription(CNSSubscription subscription) {
		
		StringBuffer out = new StringBuffer("<member>");
		
		out.append("<TopicArn>").append(subscription.getTopicArn()).append("</TopicArn>");
		out.append("<Protocol>").append(subscription.getProtocol()).append("</Protocol>");
		out.append("<SubscriptionArn>").append(subscription.getArn()).append("</SubscriptionArn>");
		out.append("<Owner>").append(subscription.getUserId()).append("</Owner>");
		out.append("<Endpoint>").append(subscription.getEndpoint()).append("</Endpoint>");
		out.append("</member>");
		
		return out.toString();
	}

	public static String getListSubscriptionResponse(List<CNSSubscription> subscriptions, String nextToken) {
		
		StringBuffer out = new StringBuffer("<ListSubscriptionsResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n");
		out.append("\t<ListSubscriptionsResult>\n");
		out.append("\t\t<Subscriptions>\n");
		
		for (CNSSubscription sub: subscriptions) {
			out.append("\t\t\t").append(printSubscription(sub)).append("\n");
		}
		
		out.append("\t\t</Subscriptions>\n");

		if (nextToken != null) {
			out.append("\t\t<NextToken>").append(nextToken).append("</NextToken>\n");
		}
		
		out.append("\t</ListSubscriptionsResult>\n");
		out.append("\t").append(CNSPopulator.getResponseMetadata()).append("\n");
		out.append("</ListSubscriptionsResponse>\n");

		return out.toString();
	}

	public static String getUnsubscribeResponse() {
		return "<UnsubscribeResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n\t" +
		CNSPopulator.getResponseMetadata() +	
		"\n</UnsubscribeResponse>\n";
	}

	public static String getListSubscriptionByTopicResponse(List<CNSSubscription> subscriptions, String nextToken) {
		
		StringBuffer out = new StringBuffer("<ListSubscriptionsByTopicResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n");
		out.append("\t<ListSubscriptionsByTopicResult>\n");
		out.append("\t\t<Subscriptions>\n");
		
		for (CNSSubscription sub: subscriptions) {
			out.append("\t\t\t").append(printSubscription(sub)).append("\n");
		}
		
		out.append("\t\t</Subscriptions>\n");

		if (nextToken != null) {
			out.append("\t\t<NextToken>" + nextToken + "</NextToken>\n");
		}
		
		out.append("\t</ListSubscriptionsByTopicResult>\n");
		out.append("\t").append(CNSPopulator.getResponseMetadata()).append("\n");
		out.append("</ListSubscriptionsByTopicResponse>\n");

		return out.toString();
	}

	public static String getConfirmSubscriptionResponse(CNSSubscription subscription) {
		
		StringBuffer out = new StringBuffer("<ConfirmSubscriptionResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n");
		out.append("\t<ConfirmSubscriptionResult>\n");
		out.append("\t\t<SubscriptionArn>").append(subscription.getArn()).append("</SubscriptionArn>\n");
		out.append("\t</ConfirmSubscriptionResult>\n");
		out.append("\t").append(CNSPopulator.getResponseMetadata()).append("\n");
		out.append("</ConfirmSubscriptionResponse>\n");
		
		return out.toString();
	}

	public static String getPublishResponse(List<String> receiptHandles) {
		
		StringBuffer out = new StringBuffer("<PublishResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n");
		out.append("\t<PublishResult>\n");
		for (String handle : receiptHandles) {
			out.append("\t\t<MessageId>").append(handle).append("</MessageId>\n");
		}
		out.append("\t</PublishResult>\n");
		out.append("\t").append(CNSPopulator.getResponseMetadata()).append("\n");
		out.append("</PublishResponse>\n");
		
		return out.toString();
	}
}