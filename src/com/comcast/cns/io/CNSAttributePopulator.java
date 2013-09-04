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

import org.apache.commons.lang.StringEscapeUtils;

import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSTopicAttributes;

/**
 * Class to generate API response for subscription attributes
 * @author bwolf, jorge
 */
public class CNSAttributePopulator {

	public static String getGetSubscriptionAttributesResponse(CNSSubscription sub, CNSSubscriptionAttributes attr) {

		StringBuffer out = new StringBuffer("<GetSubscriptionAttributesResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n");
		out.append("\t<GetSubscriptionAttributesResult>\n");
		out.append("\t\t<Attributes>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>EffectiveDeliveryPolicy</key>\n");
		out.append("\t\t\t\t<value>" + attr.getEffectiveDeliveryPolicy() + "</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>Owner</key>\n");
		out.append("\t\t\t\t<value>" + attr.getUserId() + "</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>ConfirmationWasAuthenticated</key>\n");
		out.append("\t\t\t\t<value>" + attr.isConfirmationWasAuthenticated() + "</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>DeliveryPolicy</key>\n"); 

		if (attr.getDeliveryPolicy() == null) {
			out.append("\t\t\t\t<value>" + attr.getEffectiveDeliveryPolicy() + "</value>\n");
		} else {
			out.append("\t\t\t\t<value>" + attr.getDeliveryPolicy() + "</value>\n");
		}

		out.append("\t\t\t</entry>\n");
		
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>RawMessageDelivery</key>\n"); 
		out.append("\t\t\t\t<value>" + sub.getRawMessageDelivery() + "</value>\n");
		out.append("\t\t\t</entry>\n");
		
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>TopicArn</key>\n");
		out.append("\t\t\t\t<value>" + attr.getTopicArn() + "</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>SubscriptionArn</key>\n");
		out.append("\t\t\t\t<value>" + attr.getSubscriptionArn() + "</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t</Attributes>\n");
		out.append("\t</GetSubscriptionAttributesResult>\n");
		out.append(CNSPopulator.getResponseMetadata());
		out.append("</GetSubscriptionAttributesResponse>\n");

		return out.toString();
	}

	public static String getGetTopicAttributesResponse(CNSTopicAttributes attr) {

		StringBuffer out = new StringBuffer("<GetTopicAttributesResponse>\n");
		
		out.append("\t<GetTopicAttributesResult>\n");
		out.append("\t\t<Attributes>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>EffectiveDeliveryPolicy</key>\n");
		out.append("\t\t\t\t<value>").append(attr.getEffectiveDeliveryPolicy().toString()).append("</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>Owner</key>\n");
		out.append("\t\t\t\t<value>").append(attr.getUserId()).append("</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>Policy</key>\n");
		out.append("\t\t\t\t<value>").append(attr.getPolicy()).append("</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>SubscriptionsPending</key>\n");
		out.append("\t\t\t\t<value>").append(attr.getSubscriptionsPending()).append("</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>SubscriptionsConfirmed</key>\n");
		out.append("\t\t\t\t<value>").append(attr.getSubscriptionsConfirmed()).append("</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>SubscriptionsDeleted</key>\n");
		out.append("\t\t\t\t<value>").append(attr.getSubscriptionsDeleted()).append("</value>\n");
		out.append("\t\t\t</entry>\n");

		if (attr.getDisplayName() != null && !attr.getDisplayName().isEmpty()) {
			out.append("\t\t\t<entry>\n");
			out.append("\t\t\t\t<key>DisplayName</key>\n");
			out.append("\t\t\t\t<value>").append(StringEscapeUtils.escapeHtml(attr.getDisplayName())).append("</value>\n");
			out.append("\t\t\t</entry>\n");
		}

		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>DeliveryPolicy</key>\n");
		out.append("\t\t\t\t<value>" + attr.getEffectiveDeliveryPolicy().toString() + "</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t\t<entry>\n");
		out.append("\t\t\t\t<key>TopicArn</key>\n");
		out.append("\t\t\t\t<value>").append(attr.getTopicArn()).append("</value>\n");
		out.append("\t\t\t</entry>\n");
		out.append("\t\t</Attributes>\n");
		out.append("\t</GetTopicAttributesResult>\n");
		out.append(CNSPopulator.getResponseMetadata());
		out.append("</GetTopicAttributesResponse>");

		return out.toString();
	}

	public static String getSetSubscriptionAttributesResponse() {

		String resp = "<SetSubscriptionAttributesResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n" + CNSPopulator.getResponseMetadata() + "</SetSubscriptionAttributesResponse>";
		return resp;
	}

	public static String getSetTopicAttributesResponse() {

		String resp = "<SetTopicAttributesResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n" + CNSPopulator.getResponseMetadata() + "</SetTopicAttributesResponse>";
		return resp;
	}

	public static String getAddPermissionResponse() {

		String resp = "<AddPermissionResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n" + CNSPopulator.getResponseMetadata() + "</AddPermissionResponse>";
		return resp;
	}

	public static String getRemovePermissionResponse() {

		String resp = "<RemovePermissionResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" + CNSPopulator.getResponseMetadata() + "</RemovePermissionResponse>";
		return resp;
	}
}
