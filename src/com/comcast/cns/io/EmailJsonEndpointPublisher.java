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

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.util.MailWrapper;

/**
 * Endpoint publisher for emails in json
 * @author bwolf, jorge
 *
 */
public class EmailJsonEndpointPublisher extends EmailEndpointPublisher {
	
	@Override
	public void send() throws Exception {

		if (!CMBProperties.getInstance().getSmtpEnabled()) {
			logger.warn("event=send_email error_code=smtp_disabled endpoint=" + endpoint);
			return;
		}
		
		logger.debug("event=send_email_json endpoint=" + endpoint + " subject=" + subject);
		
		MailWrapper mailAgent = new MailWrapper(); 
		mailAgent.postMail(new String [] { endpoint }, subject, message, CMBProperties.getInstance().getSmtpReplyAddress());
	}
}
