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

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSMessage.CNSMessageStructure;
import com.comcast.cns.model.CNSMessage.CNSMessageType;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.util.MailWrapper;

/**
 * Endpoint publisher for email endpoints
 * @author bwolf, jorge
 *
 */
public class EmailEndpointPublisher extends AbstractEndpointPublisher {
	protected static Logger logger = Logger.getLogger(EmailEndpointPublisher.class);

	@Override
	public void send() throws Exception {
		
		if (!CMBProperties.getInstance().getSmtpEnabled()) {
			logger.warn("event=send_email error_code=smtp_disabled endpoint=" + endpoint);
			return;
		}
		
		String msg = null;
		
		if (message.getMessageStructure() == CNSMessageStructure.json) {
			msg = message.getProtocolSpecificMessage(CnsSubscriptionProtocol.email);
		} else {
			msg = message.getMessage();
		}
		
		if (!rawMessageDelivery && message.getMessageType() == CNSMessageType.Notification) {
			msg = com.comcast.cns.util.Util.generateMessageJson(message, CnsSubscriptionProtocol.email);
		}

		logger.info("event=send_email endpoint=" + endpoint + " subject=\"" + subject + " message=\"" + msg + "\"");
		
		MailWrapper mailAgent = new MailWrapper(); 
		mailAgent.postMail(new String [] { endpoint }, subject, msg, CMBProperties.getInstance().getSmtpReplyAddress());
	}
}
