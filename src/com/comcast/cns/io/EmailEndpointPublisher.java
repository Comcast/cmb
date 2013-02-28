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

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.util.MailWrapper;

/**
 * Endpoint publisher for email endpoints
 * @author bwolf, jorge
 *
 */
public class EmailEndpointPublisher implements IEndpointPublisher{
	protected String message;
	protected String endpoint;
	protected String subject;
	
	protected static Logger logger = Logger.getLogger(EmailEndpointPublisher.class);
	
	@Override
	public void setEndpoint(String endpoint) {
		logger.debug("event=email_set_endpoint endpoint=" + endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public void send() throws Exception {
		
		if (!CMBProperties.getInstance().getSmtpEnabled()) {
			logger.warn("event=send_email error_code=smtp_disabled endpoint=" + endpoint);
			return;
		}
		
		logger.debug("event=send_email endpoint=" + endpoint + " subject=\"" + subject);
		
		MailWrapper mailAgent = new MailWrapper(); 
		mailAgent.postMail(new String [] { endpoint }, subject, message, CMBProperties.getInstance().getSmtpReplyAddress());
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
	}

	@Override
	public User getUser() {
		return null;
	}

	@Override
	public void setSubject(String subject) {
		this.subject = subject;
	}

	@Override
	public String getSubject() {
		return subject;
	}
}
