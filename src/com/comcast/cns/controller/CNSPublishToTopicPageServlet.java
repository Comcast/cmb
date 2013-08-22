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
package com.comcast.cns.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.services.sns.model.PublishRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cns.util.Util;

/**
 * Publish to topic admin page
 * @author tina, bwolf, aseem
 *
 */
public class CNSPublishToTopicPageServlet extends AdminServletBase {

	private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger(CNSPublishToTopicPageServlet.class);
	
    @Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		
		String topicArn = request.getParameter("topicArn");
		String userId = request.getParameter("userId");
		String message = request.getParameter("message");
		String subject = request.getParameter("subject");
		Map<?, ?> parameters = request.getParameterMap();
		
		connect(request);
		
		out.println("<html>");

		header(request, out, "Publish");

		out.println("<script type='text/javascript' language='javascript'>");
		out.println("function changeMsgStructure(type) { ");
		out.println(" if (type == 'same')  ");
		out.println(" document.getElementById('message').value = ''; ");
		out.println(" else ");
		out.print(" document.getElementById('message').value = '{ \\n  default: <enter your message here>,\\n' + ");
		out.print("'  email: <enter your message here>,\\n' + ");
		out.print("'  cqs: <enter your message here>,\\n' +");
		//out.print("'  sms: <enter your message here>,\\n'+");
		out.print("'  http: <enter your message here>,\\n'+");
		out.print("'  https: <enter your message here> \\n}';");
		out.println("}");
		out.println("</script>");
		
		out.println("<body>");
		
		out.println("<h2>Publish to Topic</h2>");
		
		if (parameters.containsKey("Publish")) {
			
			out.println("<table><tr><td><b>");
			
			try {
				
				PublishRequest publishRequest = new PublishRequest(topicArn, message, subject);
				sns.publish(publishRequest);
				logger.debug("event=publish topic_arn=" + topicArn + " user_id= " + userId);

			} catch (Exception ex) {
				logger.error("event=publish topic_arn=" + topicArn + " user_id= " + userId);
				throw new ServletException(ex);
			}
			
			out.println("</b></td></tr>");
			out.println("<tr><td align=right><br/><input type='button' name='Close' value='Close' onclick='window.close()'></td></tr></table>");
		
		} else {
		
			if (topicArn != null) {
			
				out.print("<form action=\"/webui/cnsuser/publish?topicArn=" + topicArn + "&userId=" + userId + "\" " + "method=POST>");
				out.println("<table><tr><td><b>Topic Name:</b></td><td>"+ Util.getNameFromTopicArn(topicArn) + "</td></tr>");
				out.println("<tr><td>&nbsp;</td><td>&nbsp;</td></tr>");
				out.println("<tr><td valign=top><b>Subject:</b></td><td valign=top><input type='text' size='90' name='subject'><br/><I><font color='grey'>Up to 100 printable ASCII characters (optional)</font></I></td></tr>");
				out.println("<tr><td>&nbsp;</td><td>&nbsp;</td></tr>");
				out.println("<tr><td valign=top><b>Message:</b></td><td valign=top><textarea name='message' cols = '70' rows='8' id='message' ></textarea><br/><I><font color='grey'>Up to 256 KB of Unicode text</font></I></td></tr>");
				out.println("<tr><td>&nbsp;</td><td>&nbsp;</td></tr>");
				out.println("<tr><td>&nbsp;</td><td><input type='radio' name='msgType' value='same' checked='checked' onclick='changeMsgStructure(this.value)'/>Use same message body for all protocols</br/>");
				out.println("<input type='radio' name='msgType' value='json' onclick='changeMsgStructure(this.value)'/>Use different message body for different protocols</td></tr>");
				out.println("</table>");
			
				//out.println("<p><i><font color='grey'>For SMS notifications, it is best to leave the Subject field blank and place your text in the Message field to send a maximum of 140 characters. If the Subject field is not blank, the text in the Subject field will be used as content for the SMS messages.</font></i></p>");
				
				out.println("<hr/>");
				out.println("<input type='button' name='Cancel' value='Cancel' style='float:right;' onclick='window.close()'><input type='submit' name='Publish' value='Publish Message' style='float:right;'>");
				out.println("</form>");
			}
		}
		
		out.println("</body></html>");
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
