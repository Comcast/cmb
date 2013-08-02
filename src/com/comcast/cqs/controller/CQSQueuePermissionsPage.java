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
package com.comcast.cqs.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.CMBStatement;
import com.comcast.cqs.util.Util;

/**
 * Admin page for queue permissions
 * @author bwolf, tina, baosen, aseem
 *
 */
public class CQSQueuePermissionsPage extends AdminServletBase {
	
    private static final long serialVersionUID = 1L;
    private Logger logger = Logger.getLogger(CQSQueuePermissionsPage.class);
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String userId = request.getParameter("userId");
		String queueName = request.getParameter("queueName");
		String labelSid = request.getParameter("sid");
		Map<?, ?> params = request.getParameterMap();
		
		String queueUrl = Util.getAbsoluteQueueUrlForName(queueName, userId);
		
		connect(request);
		
		if (params.containsKey("Remove")) {
			
			try {
				RemovePermissionRequest removePermissionRequest = new RemovePermissionRequest(queueUrl, labelSid);
				sqs.removePermission(removePermissionRequest);
				logger.debug("event=remove_permission queue_url=" + queueUrl + " label=" + labelSid + " user_id=" + userId);
			} catch (Exception ex) {
				logger.error("event=remove_permission queue_url=" + queueUrl + " label=" + labelSid + " user_id=" + userId, ex);
				throw new ServletException(ex);
	        } 
		}
		
		out.println("<html>");
		
		header(request, out, "Permissions for Queue "+ Util.getNameForAbsoluteQueueUrl(queueUrl));
		
		out.println("<body>");

		out.println("<h2>Permissions for Queue "+ Util.getNameForAbsoluteQueueUrl(queueUrl) + "</h2>");
		
		if (user != null) {
			
			out.println("<table><tr><td><b>User Name:</b></td><td>"+ user.getUserName()+"</td></tr>");
			out.println("<tr><td><b>User ID:</b></td><td>"+ user.getUserId()+"</td></tr>");
			out.println("<tr><td><b>Access Key:</b></td><td>"+user.getAccessKey()+"</td></tr>");
			out.println("<tr><td><b>Access Secret:</b></td><td>"+user.getAccessSecret()+"</td></tr>");
			out.println("<tr><td><b>Queue Name:</b></td><td>"+Util.getNameForAbsoluteQueueUrl(queueUrl)+"</td></tr>");
			out.println("<tr><td><b>Queue Url:</b></td><td>"+queueUrl+"</td></tr></table><br>");
		}
		
		Map<String, String> attributes = null;
		
		try {
			GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(queueUrl);
			getQueueAttributesRequest.setAttributeNames(Arrays.asList("Policy"));
			GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(getQueueAttributesRequest);
			attributes = getQueueAttributesResult.getAttributes();
		} catch (Exception ex) {
			logger.error("event_failed_to_get_attributes queue_url=" + queueUrl, ex);
			throw new ServletException(ex);
		}
		
		CMBPolicy policy;
		
		try {
			policy = new CMBPolicy(attributes.get("Policy"));
		} catch (Exception ex) {
			throw new ServletException(ex);
		}
		
		if (policy != null && !policy.getStatements().isEmpty()) {
			
			List<CMBStatement> stmts = policy.getStatements();
			
			if (stmts != null && stmts.size() > 0) {
				out.println("<span class='content'><table border='1' width='70%'><tr><th>Effect</th><th>Principals</th><th>Actions</th><th>Label</th><th>&nbsp;</th></tr>");
			}
			
			for (int i = 0; stmts != null && i < stmts.size(); i++) {
				
				out.print("<form action=\"/webui/cqsuser/permissions/?userId="+user.getUserId()+"&queueUrl="+queueUrl+"\" method=POST>");
				
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
				out.println("<td><input type='submit' value='Remove' name='Remove'><input type='hidden' name='sid' value='"+ sid +"'><input type='hidden' name='userId' value='"+ userId +"'><input type='hidden' name='queueName' value='"+ queueName +"'></td></tr></form>");
			}
			
			out.println("</table></span>");
		}
		
		out.println("<p><a href='' onclick=\"window.open('/webui/cqsuser/addpermission/?queueName="+ queueName + "&userId="+userId+"', 'AddQueuePermission', 'location=0,menubar=0,scrollbars=0,status=0,titlebar=0,toolbar=0,height=470,width=730')\">Add permission</a></p>");
		out.println("<h5 style='text-align:center;'><a href='/webui'>ADMIN HOME</a>");
        out.println("<a href='/webui/cqsuser?userId="+userId+"'>BACK TO QUEUE</a></h5>");
		out.println("</body></html>");
		
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
