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

import org.apache.log4j.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSMessage.CNSMessageStructure;
import com.comcast.cns.model.CNSMessage.CNSMessageType;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cqs.api.CQSAPI;
import com.comcast.cqs.util.Util;

/**
 * Endpoint publisher for CQS endpoints
 * @author jorge, bwolf, baosen
 *
 */
public class CQSEndpointPublisher extends AbstractEndpointPublisher {

	private static Logger logger = Logger.getLogger(CQSEndpointPublisher.class);
	
    private BasicAWSCredentials awsCredentials;
    private AmazonSQSClient sqs;
	
	@Override
	public void send() throws Exception {
		
		if ((message == null) || (endpoint == null)) {
			throw new Exception("Message and Endpoint must both be set");
		}
		
		String absoluteQueueUrl;
		
		if (com.comcast.cqs.util.Util.isValidQueueUrl(endpoint)) {
			absoluteQueueUrl = endpoint;
		} else {
			absoluteQueueUrl = com.comcast.cqs.util.Util.getAbsoluteQueueUrlForArn(endpoint);
		}

		try {

			String msg = null;
			
			if (message.getMessageStructure() == CNSMessageStructure.json) {
				msg = message.getProtocolSpecificMessage(CnsSubscriptionProtocol.cqs);
			} else {
				msg = message.getMessage();
			}
			
			if (!rawMessageDelivery && message.getMessageType() == CNSMessageType.Notification) {
				msg = com.comcast.cns.util.Util.generateMessageJson(message, CnsSubscriptionProtocol.cqs);
			}
			
			if (msg == null) {
				logger.warn("event=message_is_null endpoint=" + endpoint);
				return;
			}
			
			if (CMBProperties.getInstance().useInlineApiCalls() && CMBProperties.getInstance().getCQSServiceEnabled()) {
				CQSAPI.sendMessage(user.getUserId(), Util.getRelativeForAbsoluteQueueUrl(absoluteQueueUrl), msg, null, messageAttributes);
			} else {
		        awsCredentials = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
		        sqs = new AmazonSQSClient(awsCredentials);
				sqs.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());
				SendMessageRequest sendMessageRequest = new SendMessageRequest(absoluteQueueUrl, msg);
				if (messageAttributes != null) {
					for (String messageAttributeName : messageAttributes.keySet()) {
		            	MessageAttributeValue value = new MessageAttributeValue();
		            	value.setDataType(messageAttributes.get(messageAttributeName).getDataType());
		            	value.setStringValue(messageAttributes.get(messageAttributeName).getStringValue());
		            	sendMessageRequest.addMessageAttributesEntry(messageAttributeName, value);
					}
				}
				sqs.sendMessage(sendMessageRequest);			
			}
		
			if (CMBProperties.getInstance().getMaxMessagePayloadLogLength() > 0) {
				if (msg.length() > CMBProperties.getInstance()
						.getMaxMessagePayloadLogLength()) {

					logger.debug("event=delivering_cqs_message endpoint="
							+ endpoint
							+ "\" message=\""
							+ msg.substring(0, CMBProperties.getInstance()
									.getMaxMessagePayloadLogLength() - 1));

				} else {
					logger.debug("event=delivering_cqs_message endpoint="
							+ endpoint + "\" message=\"" + msg);
				}
			} else {
				logger.debug("event=delivering_cqs_message endpoint="
						+ endpoint);
			}

		} catch (Exception ex) {
			logger.warn("event=send_cqs_message endpoint=" + endpoint + "\" message=\"" + message, ex);
			throw new CMBException(CNSErrorCodes.InternalError, "internal service error");
		}

		logger.debug("event=send_cqs_message endpoint=" + endpoint + " message=\"" + message + "\"");
	}
}
