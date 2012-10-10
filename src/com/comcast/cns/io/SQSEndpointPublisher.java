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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.util.CNSErrorCodes;

/**
 * Endpoint publisher for CQS endpoints
 * @author bwolf
 *
 */
public class SQSEndpointPublisher implements IEndpointPublisher {

	private static Logger logger = Logger.getLogger(CQSEndpointPublisher.class);

	private String endpoint;
	private String message;
	private User user;
	
    private BasicAWSCredentials awsCredentials;
    private AmazonSQS sqs;
	
	@Override
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public void setMessage(String message) {
		this.message = message;
	}
	
	@Override
	public void send() throws Exception {
		
		if ((message == null) || (endpoint == null)) {
			throw new Exception("Message and Endpoint must both be set");
		}
		
        awsCredentials = new BasicAWSCredentials(CMBProperties.getInstance().getAwsAccessKey(), CMBProperties.getInstance().getAwsAccessSecret());
        sqs = new AmazonSQSClient(awsCredentials);
		
		String url;
		
		if (com.comcast.cqs.util.Util.isValidQueueUrl(endpoint)) {
			url = endpoint;
		} else {
			url = com.comcast.cqs.util.Util.getAbsoluteQueueUrlForArn(endpoint);
		}

		try {
			sqs.sendMessage(new SendMessageRequest(url, message));			
		} catch(Exception ex) {
			logger.warn("event=send_sqs_message status=failure endpoint=" + endpoint + "\" message=\"" + message, ex);
			throw new CMBException(CNSErrorCodes.InternalError, "internal service error");
		}

		logger.debug("event=send_sqs_message endpoint=" + endpoint + " message=\"" + message + "\"");
	}

	@Override
	public String getEndpoint() {
		return endpoint;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public void setUser(User user) {
		this.user = user;
	}

	@Override
	public User getUser() {
		return user;
	}

	@Override
	public void setSubject(String subject) {
	}

	@Override
	public String getSubject() {
		return null;
	}
}
