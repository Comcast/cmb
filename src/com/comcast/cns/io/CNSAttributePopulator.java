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

import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSTopicAttributes;

/**
 * Class to generate API response for subscription attributes
 * @author bwolf, jorge
 */
public class CNSAttributePopulator {

	public static String getGetSubscriptionAttributesResponse(CNSSubscriptionAttributes attr) {

		String resp = "<GetSubscriptionAttributesResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" +
		"<GetSubscriptionAttributesResult>" +
		"<Attributes>" +
		"<entry>" +
		"<key>EffectiveDeliveryPolicy</key>" +
		"<value>" + attr.getEffectiveDeliveryPolicy() + "</value>" +
		"</entry>" +
		"<entry>" +
		"<key>Owner</key>" +
		"<value>" + attr.getUserId() + "</value>" +
		"</entry>" +
		"<entry>" +
		"<key>ConfirmationWasAuthenticated</key>" +
		"<value>" + attr.isConfirmationWasAuthenticated() + "</value>" +
		"</entry>" +
		"<entry>" +
		"<key>DeliveryPolicy</key>";    

		if (attr.getDeliveryPolicy() == null) {
			resp += "<value>" + attr.getEffectiveDeliveryPolicy() + "</value>";
		} else {
			resp += "<value>" + attr.getDeliveryPolicy() + "</value>";
		}

		resp += "</entry>" +
		"<entry>" +
		"<key>TopicArn</key>" +
		"<value>" + attr.getTopicArn() + "</value>" +
		"</entry>" +
		"<entry>" +
		"<key>SubscriptionArn</key>" +
		"<value>" + attr.getSubscriptionArn() + "</value>" +
		"</entry>" +
		"</Attributes>" +
		"</GetSubscriptionAttributesResult>" +
		CNSPopulator.getResponseMetadata() +
		"</GetSubscriptionAttributesResponse>";

		return resp;
	}

	public static String getGetTopicAttributesResponse(CNSTopicAttributes attr) {

		String resp = "<GetTopicAttributesResponse>" +
		"<GetTopicAttributesResult>" +
		"<Attributes>" +
		"<entry>" +
		"<key>EffectiveDeliveryPolicy</key>" +
		"<value>" + attr.getEffectiveDeliveryPolicy().toString() + "</value>" +
		"</entry>" +
		"<entry>" +
		"<key>Owner</key>" +
		"<value>" + attr.getUserId() + "</value>" +
		"</entry>" +
		"<entry>" +
		"<key>SubscriptionsPending</key>" +
		"<value>" + attr.getSubscriptionsPending() + "</value>" +
		"</entry>" +
		"<entry>" +
		"<key>Policy</key>" +
		"<value>" + attr.getPolicy() + "</value>" +
		"</entry>" +
		"<entry>" +  
		"<key>SubscriptionsConfirmed</key>" +
		"<value>" + attr.getSubscriptionsConfirmed() + "</value>" +
		"</entry>" +
		"<entry>" +
		"<key>SubscriptionsDeleted</key>" +
		"<value>" + attr.getSubscriptionsDeleted() + "</value>" +
		"</entry>";

		if (attr.getDisplayName() != null && !attr.getDisplayName().isEmpty()) {
			resp += "<entry>" +
			"<key>DisplayName</key>" +
			"<value>" + attr.getDisplayName() + "</value>" +
			"</entry>";
		}


		resp +=   "<entry>" +
		"<key>DeliveryPolicy</key>" +
		"<value>" + attr.getEffectiveDeliveryPolicy().toString() + "</value>" +
		"</entry>";
		resp +=   "<entry>" +
		"<key>TopicArn</key>" +
		"<value>" + attr.getTopicArn() + "</value>" +
		"</entry>" +
		"</Attributes>" +
		"</GetTopicAttributesResult>" +
		CNSPopulator.getResponseMetadata() +
		"</GetTopicAttributesResponse>";

		return resp;
	}

	public static String getSetSubscriptionAttributesResponse() {

		String resp = "<SetSubscriptionAttributesResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" + CNSPopulator.getResponseMetadata() + "</SetSubscriptionAttributesResponse>";
		return resp;
	}

	public static String getSetTopicAttributesResponse() {

		String resp = "<SetTopicAttributesResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" + CNSPopulator.getResponseMetadata() + "</SetTopicAttributesResponse>";
		return resp;
	}

	public static String getAddPermissionResponse() {

		String resp = "<AddPermissionResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" + CNSPopulator.getResponseMetadata() + "</AddPermissionResponse>";
		return resp;
	}

	public static String getRemovePermissionResponse() {

		String resp = "<RemovePermissionResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" + CNSPopulator.getResponseMetadata() + "</RemovePermissionResponse>";
		return resp;
	}
}
