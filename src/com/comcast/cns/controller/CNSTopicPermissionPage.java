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
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.RemovePermissionRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.CMBStatement;
import com.comcast.cns.util.Util;
import com.comcast.cqs.controller.CQSQueuePermissionsPage;

/**
 * Topic permissions admin page
 * @author tina, bwolf
 *
 */
public class CNSTopicPermissionPage extends AdminServletBase {
	
    private static final long serialVersionUID = 1L;
    private Logger logger = Logger.getLogger(CQSQueuePermissionsPage.class);
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String topicArn = request.getParameter("topicArn");
		String userId = request.getParameter("userId");
		String labelSid = request.getParameter("sid");
		Map<?, ?> params = request.getParameterMap();
		
		connect(userId);
		
		if (params.containsKey("Remove")) {
			
			try {
				RemovePermissionRequest removePermissionRequest = new RemovePermissionRequest(topicArn, labelSid);
				sns.removePermission(removePermissionRequest);
				logger.debug("event=remove_permission topic_arn=" + topicArn + " label=" + labelSid + " user_id=" + userId);
			} catch (Exception ex) {
				logger.error("event=remove_permission topic_arn=" + topicArn + " label=" + labelSid + " user_id=" + userId, ex);
				throw new ServletException(ex);
	        } 
		}
		
		Map<String, String> attributes = null;
		
		try {
			GetTopicAttributesRequest getTopicAttributesRequest = new GetTopicAttributesRequest(topicArn);
			GetTopicAttributesResult getTopicAttributesResult = sns.getTopicAttributes(getTopicAttributesRequest);
			attributes = getTopicAttributesResult.getAttributes();
		} catch (Exception ex) {
			logger.error("event=get_topic_attributes topic_arn=" + topicArn + " user_id=" + userId, ex);
			throw new ServletException(ex);
		}
		
		CMBPolicy policy = null;
		
		try {
			if (attributes.get("Policy") != null && !attributes.get("Policy").equals("") && !attributes.get("Policy").equals("null")) {
				policy = new CMBPolicy(attributes.get("Policy"));
			}
		} catch (Exception ex) {
			throw new ServletException(ex);
		}
		
		out.println("<html>");
		
		header(request, out, "Permissions for Topic "+ Util.getNameFromTopicArn(topicArn));
		
		out.println("<body>");

		out.println("<h2>Permissions for Topic "+ Util.getNameFromTopicArn(topicArn) + "</h2>");
		
		if (user != null) {
			
			out.println("<table><tr><td><b>User Name:</b></td><td>"+ user.getUserName()+"</td></tr>");
			out.println("<tr><td><b>User ID:</b></td><td>"+ user.getUserId()+"</td></tr>");
			out.println("<tr><td><b>Access Key:</b></td><td>"+user.getAccessKey()+"</td></tr>");
			out.println("<tr><td><b>Access Secret:</b></td><td>"+user.getAccessSecret()+"</td></tr>");
			out.println("<tr><td><b>Topic Name:</b></td><td>"+Util.getNameFromTopicArn(topicArn)+"</td></tr>");
			out.println("<tr><td><b>Topic Arn:</b></td><td>"+topicArn+"</td></tr></table><br>");
		}
		
		if (policy != null && !policy.getStatements().isEmpty()) {
			
			List<CMBStatement> stmts = policy.getStatements();
			
			if (stmts != null && stmts.size() > 0) {
				out.println("<span class='content'><table border='1' width='70%'><tr><th>Effect</th><th>Users</th><th>Actions</th><th>Label</th><th>&nbsp;</th></tr>");
			}
			
			for (int i = 0; stmts != null && i < stmts.size(); i++) {
				
				out.println("<form action=\"/webui/cnsuser/permission/?userId="+user.getUserId()+"&topicArn="+topicArn+"\" method=POST>");
				
		        CMBStatement stmt = stmts.get(i);
				
		        out.println("<tr><td>" + stmt.getEffect().toString()+ "</td><td>");
				
				if (stmt.getPrincipal() != null && stmt.getPrincipal().size() > 0) {
					
					for (int k = 0; k < stmt.getPrincipal().size(); k++) {
						String user1 = stmt.getPrincipal().get(k);
						out.println(user1+"<br>");
					}
				}
				
				out.println("</td><td>");
				
				if (stmt.getAction() != null && stmt.getAction().size() > 0) {
					
					for (int j = 0; j < stmt.getAction().size(); j++) {
						String action1 = stmt.getAction().get(j);
						out.println(action1+"<br>");
					}
				}
				
				String sid = stmt.getSid();
				out.println("</td>");
				out.println("<td>" + sid + "</td>");
				out.println("<td><input type='submit' value='Remove' name='Remove'><input type='hidden' name='sid' value='"+ sid +"'></td></tr></form>");
			}
			
			out.println("</table></span>");
		}
		
		out.println("<p><a href='' onclick=\"window.open('/webui/cnsuser/addpermission/?topicArn=" + topicArn + "&topicName=" + Util.getNameFromTopicArn(topicArn) + "&userId=" + userId + "', 'AddTopicPermission', 'location=0,menubar=0,scrollbars=0,status=0,titlebar=0,toolbar=0,height=470,width=730')\">Add permission</a></p>");
		out.println("<h5 style='text-align:center;'><a href='/webui'>ADMIN HOME</a>");
        out.println("<a href='/webui/cnsuser?userId="+userId+"&topicArn="+topicArn+"'>BACK TO TOPIC</a></h5>");
		out.println("</body></html>");
		
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
