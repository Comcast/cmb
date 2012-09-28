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
package com.comcast.cns.model;

import java.util.Date;
import java.util.UUID;

import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.Util;

/**
 * Represents a Subscription
 * @author bwolf, jorge, 
 *
 * Class is not thread-safe. Caller must ensure thread safety
 */
public class CNSSubscription {

    public enum CnsSubscriptionProtocol { http, https, email, email_json, sms, cqs;

    /**
     * 
     * @param endpoint
     * @return true if endpoint is correctly formatted given the protocol
     */
    public boolean isValidEnpoint(String endpoint) {

        switch (this) { 

        case https:
            if(!endpoint.substring(0, 8).equals("https://")) {
                return false;
            } 
            break;
        case http: 
            if(!endpoint.substring(0, 7).equals("http://")) {
                return false;
            }
            break;
        case email:
        case email_json:
            if(!endpoint.contains("@")) {
                return false;
            }
            break;
        case sms:
            if(!com.comcast.cns.util.Util.isPhoneNumber(endpoint)) {
                return false;
            }
            break;
        case cqs:
            if(!com.comcast.cqs.util.Util.isValidQueueArn(endpoint) && !com.comcast.cqs.util.Util.isValidQueueUrl(endpoint)) {
                return false;
            }
            break;
        } 
        return true;
    }
    };

	private String arn;
	private String topicArn;
	private String userId;
	private CnsSubscriptionProtocol protocol;
	private String endpoint;
	private Date requestDate;
	private Date confirmDate;
	private boolean confirmed;
	private String token;
	private boolean authenticateOnUnsubscribe;
	private CNSSubscriptionDeliveryPolicy deliveryPolicy;

	public CNSSubscription(String endpoint, CnsSubscriptionProtocol protocol, String topicArn, String userId) {

		this.endpoint = endpoint;
		this.protocol = protocol;
		this.topicArn = topicArn;
		this.userId = userId;

		this.token = UUID.randomUUID().toString();
		this.requestDate = new Date();
		this.confirmed = false;
		this.authenticateOnUnsubscribe = false;

		//if(protocol == CnsSubscriptionProtocol.http) this.deliveryPolicy = new CNSSubscriptionDeliveryPolicy();
		//System.out.println("deliveryPolicy: " + deliveryPolicy.toString());
	}

	public CNSSubscription(String arn) {
		this.arn = arn;
	}

	public String getArn() {
		return arn;
	}

	public void setArn(String arn) {
		this.arn = arn;
	}

	public String getTopicArn() {
		return topicArn;
	}

	public void setTopicArn(String topicArn) {
		this.topicArn = topicArn;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public CnsSubscriptionProtocol getProtocol() {
		return protocol;
	}

	public void setProtocol(CnsSubscriptionProtocol protocol) {
		this.protocol = protocol;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public Date getRequestDate() {
		return requestDate;
	}

	public void setRequestDate(Date requestDate) {
		this.requestDate = requestDate;
	}

	public Date getConfirmDate() {
		return confirmDate;
	}

	public void setConfirmDate(Date confirmDate) {
		this.confirmDate = confirmDate;
	}

	public boolean isConfirmed() {
		return confirmed;
	}

	public void setConfirmed(boolean confirmed) {
		this.confirmed = confirmed;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public boolean isAuthenticateOnUnsubscribe() {
		return authenticateOnUnsubscribe;
	}

	public void setAuthenticateOnUnsubscribe(boolean authenticateOnUnsubscribe) {
		this.authenticateOnUnsubscribe = authenticateOnUnsubscribe;
	}

	public boolean isTokenExpired() {

		if ((new Date()).getTime() - getRequestDate().getTime() > 3 * 24 * 60 * 60 * 1000) {
			return true;
		}

		return false;
	}
	
	/**
	 * Verify this instance of subscription
	 * @throws CMBException if not valid
	 */
	public void checkIsValid() throws CMBException {

		if (arn == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set arn for subscription");
		}
		
		if (!com.comcast.cns.util.Util.isValidSubscriptionArn(arn)) {
			throw new CMBException(CMBErrorCodes.InternalError, "Invalid subscription arn");
		}

		if (topicArn == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set topic arn for subscription");
		}
		
		if (!com.comcast.cns.util.Util.isValidTopicArn(topicArn)) {
			throw new CMBException(CMBErrorCodes.InternalError, "Invalid topic arn");
		}
		
		if (userId == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set user id for subscription");
		}
		
		if (protocol == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set protocol for subscription");
		}
		
		if (endpoint == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set endpoint for subscription");
		}
		
		if (confirmed && confirmDate == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Bad confirmation data");
		}
		
		if (!confirmed && confirmDate != null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Bad confirmation data");
		}
	}

	@Override
	public String toString() {
		return "arn=" + getArn() + " topicArn=" + getTopicArn() + " user_id=" + getUserId() + " protocol=" + getProtocol() + " endpoint=" + getEndpoint() + " request_date=" + getRequestDate() + " confirm_date=" + getConfirmDate() + " confirmed=" + isConfirmed() + " token=" + getToken();
	}

	@Override
	public boolean equals(Object o) {

		if (!(o instanceof CNSSubscription)) {
			return false;
		}

		CNSSubscription s = (CNSSubscription)o;

		if (Util.isEqual(getArn(), s.getArn()) &&
				Util.isEqual(getTopicArn(), s.getTopicArn()) &&
				Util.isEqual(getUserId(), s.getUserId()) &&
				Util.isEqual(getProtocol(), s.getProtocol()) &&
				Util.isEqual(getEndpoint(), s.getEndpoint()) &&
				Util.isEqual(getRequestDate(), s.getRequestDate()) &&
				Util.isEqual(getConfirmDate(), s.getConfirmDate()) &&
				Util.isEqual(isConfirmed(), s.isConfirmed()) &&
				Util.isEqual(getToken(), s.getToken())) {
			return true;
		}

		return false;
	}

	public CNSSubscriptionDeliveryPolicy getDeliveryPolicy() {
		return deliveryPolicy;
	}

	public void setDeliveryPolicy(CNSSubscriptionDeliveryPolicy deliveryPolicy) {
		this.deliveryPolicy = deliveryPolicy;
	}
}
