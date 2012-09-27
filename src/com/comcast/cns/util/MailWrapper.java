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
package com.comcast.plaxo.cns.util;

import javax.mail.*;
import javax.mail.internet.*;

import com.comcast.plaxo.cmb.common.util.CMBProperties;

import java.util.*;

/**
 * Utility class for providing email sending functionality
 * @author bwolf
 *
 */
public class MailWrapper {

	private static final String SMTP_HOST_NAME = CMBProperties.getInstance().getSmtpHostName();
	private static final String SMTP_AUTH_USER = CMBProperties.getInstance().getSmtpUserName();
	private static final String SMTP_AUTH_PWD  = CMBProperties.getInstance().getSmtpPassword();

	public void postMail(String recipients[ ], String subject, String message , String from) throws MessagingException {

		boolean debug = false;

		Properties props = new Properties();
		props.put("mail.smtp.host", SMTP_HOST_NAME);
		props.put("mail.smtp.auth", "true");

		Authenticator auth = new SMTPAuthenticator();
		Session session = Session.getDefaultInstance(props, auth);

		session.setDebug(debug);

		Message msg = new MimeMessage(session);

		InternetAddress addressFrom = new InternetAddress(from);
		msg.setFrom(addressFrom);

		InternetAddress[] addressTo = new InternetAddress[recipients.length];
		
		for (int i = 0; i < recipients.length; i++) {
			addressTo[i] = new InternetAddress(recipients[i]);
		}
		
		msg.setRecipients(Message.RecipientType.TO, addressTo);
		msg.setSubject(subject);
		msg.setContent(message, "text/plain");

		Transport.send(msg);
	}

	private class SMTPAuthenticator extends javax.mail.Authenticator
	{
		public PasswordAuthentication getPasswordAuthentication()
		{
			String username = SMTP_AUTH_USER;
			String password = SMTP_AUTH_PWD;
			return new PasswordAuthentication(username, password);
		}
	}
}
