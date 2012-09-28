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

import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;

/**
 * Admin page for editing topic display name
 * @author tina, bwolf, aseem
 *
 */
public class CNSEditTopicDisplayNamePage extends AdminServletBase {
	
    private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger(CNSEditTopicDisplayNamePage.class);
	
    @Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String topicArn = request.getParameter("topicArn");
		String displayName = request.getParameter("displayName");
		String userId =  request.getParameter("userId");
		Map<?, ?> parameters = request.getParameterMap();
		
		connect(userId);
		
		out.println("<html>");
		out.println("<head><title>Edit Topic Display Name</title></head>");
		
		if (parameters.containsKey("Edit")) {
		
			try {
				SetTopicAttributesRequest setTopicAttributesRequest = new SetTopicAttributesRequest(topicArn, "DisplayName", displayName);
				sns.setTopicAttributes(setTopicAttributesRequest);
				logger.debug("event=update_display_name status=success topic_arn=" + topicArn);
			} catch (Exception ex) {
				logger.error("event=update_display_name status=failed topic_arn= " + topicArn, ex);
				throw new ServletException(ex);
			}
			
			out.println("<body onload='javascript:window.opener.location.reload();window.close();'>");
		
		} else {
		
			Map<String, String> attributes = null;
			
			try {
				GetTopicAttributesRequest getTopicAttributesRequest = new GetTopicAttributesRequest(topicArn);
				GetTopicAttributesResult getTopicAttributesResult = sns.getTopicAttributes(getTopicAttributesRequest);
				attributes = getTopicAttributesResult.getAttributes();
			} catch (Exception ex) {
				logger.error("event=update_display_name status=failed topic_arn= " + topicArn, ex);
				throw new ServletException(ex);
			}

			out.println("<body>");
			out.println("<h1>Edit Topic Display Name</h1>");
			out.print("<form action=\"");
            out.print(response.encodeURL("EditDisplayName") + "?topicArn="+topicArn);
            out.print("\" ");
            out.println("method=POST>");
            out.println("<input type='hidden' name='userId' value='"+ userId +"'>");
			out.println("<p>The Display Name of a topic will be used, if present, in the \"From:\" field of any email notifications from the topic. It is also required and included in every SMS notification sent out.</p>");
			out.println("<p><b>Display Name:</b> <input type='text' name='displayName' size='100' value= '" + (attributes.get("DisplayName") == null ? "" : attributes.get("DisplayName")) + "'></p>");
			out.println("<br/><I><font color='grey'>Up to 100 printable ASCII characters</font></I>");
			out.println("<hr/>");
			out.println("<input type='button' value='Cancel' onclick='window.close();' style='float:right;'><input type='submit' value='Edit' name='Edit' style='float:right;'></form>");
		}
		
		out.println("</body></html>");
		
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
    @Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
