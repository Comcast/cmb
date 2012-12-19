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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.services.sns.model.AddPermissionRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cqs.controller.CQSAddQueuePermissionPage;

/**
 * Add permission to topic admin page
 * @author tina, bwolf
 *
 */
public class CNSTopicAddPermissionPage extends AdminServletBase {
	
	private static final long serialVersionUID = 1L;
	
	private Logger logger = Logger.getLogger(CNSTopicAddPermissionPage.class);
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		redirectUnauthenticatedUser(request, response);

		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		
		String topicArn = request.getParameter("topicArn");
		String topicName = request.getParameter("topicName");
		String users = request.getParameter("users");
		String everybody = request.getParameter("everybody");
		String[] actions = request.getParameterValues("actions");
		String allActions = request.getParameter("allActions");
		String permAction = request.getParameter("permission");
		String userId = request.getParameter("userId");

		connect(userId);
		
		List<String> usersList = null;
		List<String> actionsList = null;
		
		if (everybody == null) {
			
			if (users != null && !users.trim().equals("")) {
				usersList = Arrays.asList(users.split(","));
			}
			
		} else {		
			usersList = new ArrayList<String>();
			usersList.add("*");
		}
		
		if (allActions == null) {
			
			if (actions != null && actions.length > 0) {
				actionsList = Arrays.asList(actions);
			}
			
		} else {
			actionsList = new ArrayList<String>();
			actionsList.add("*");
		}
		
		String validInput = CQSAddQueuePermissionPage.checkValidInput(usersList, actionsList);
		
		Map<?, ?> params = request.getParameterMap();
		out.println("<html>");
		
		simpleHeader(request, out, "Add Permission to Topic "+ topicName);
		
		if (params.containsKey("Submit") && validInput.equals("")) {

			String sid = Long.toHexString(Double.doubleToLongBits(Math.random()));
			
			if (permAction.equals("add") && usersList != null && usersList.size() > 0 && actionsList != null && actionsList.size() > 0) {
				
				try {
					AddPermissionRequest addPermissionRequest = new AddPermissionRequest(topicArn, sid, usersList, actionsList);
					sns.addPermission(addPermissionRequest);
				} catch (Exception ex) {
					logger.error("event_failed_to_add_permission arn=" + topicArn + " label=" + sid, ex);
					throw new ServletException(ex);
				}
			} 
			
			out.println("<body onload='javascript:window.opener.location.reload();window.close();'>");
			
		} else {
			
			out.println("<body>");
			out.println("<h1>Add Permission to Topic "+ topicName + "</h1>");
			out.println("<form action=\"");
			out.println(response.encodeURL("AddPermission") + "?topicArn="+topicArn);
			out.println("\" ");
			out.println("method=POST>");
			out.println("<p>This view allows direct manipulation of your topic access control policy.");
			out.println("<table><tr><td colspan=2>&nbsp;</td></tr>");
			out.println("<tr><td><b>Effect:</b></td><td><input type='radio' name='permission' value='add' checked>Add</td></tr>");
			
			out.println("<tr><td><b>Users:</b></td><td><input type='text' name='users' size=80 id='users' value='" + (users==null? "" : users) + "'>");
			out.println("<input type='checkbox' name='everybody' " + (everybody != null ? "checked" :"") + " onclick='if(this.checked) {document.getElementById(\"users\").disabled=true;} else {document.getElementById(\"users\").disabled=false;}'>Everybody(*) </td></tr>");
			out.println("<tr><td>&nbsp;</td><td><font color='grey'>Use commas between multiple users.</font></td></tr>");
            out.println("<tr><td colspan=2>&nbsp;</td></tr>");
            out.println("<tr><td valign=top><b>Actions:</b></td><td valign=top><select name='actions' multiple size='8' id='actions'>");
            
            out.println("<option value='Publish'" + (actionsList !=null && actionsList.contains("Publish") ? "selected" : "") + ">CNS:Publish</option>");
            out.println("<option value='RemovePermission'" + (actionsList !=null && actionsList.contains("RemovePermission") ? "selected" : "") + ">CNS:RemovePermission</option>");
            out.println("<option value='SetTopicAttributes'" + (actionsList !=null && actionsList.contains("SetTopicAttributes") ? "selected" : "") + ">CNS:SetTopicAttributes</option>");
            out.println("<option value='DeleteTopic'" + (actionsList !=null && actionsList.contains("DeleteTopic") ? "selected" : "") + ">CNS:DeleteTopic</option>");
            out.println("<option value='ListSubscriptionsByTopic'" + (actionsList !=null && actionsList.contains("ListSubscriptionsByTopic") ? "selected" : "") + ">CNS:ListSubscriptionsByTopic</option>");
            out.println("<option value='GetTopicAttributes'" + (actionsList !=null && actionsList.contains("GetTopicAttributes") ? "selected" : "") + ">CNS:GetTopicAttributes</option>");
            out.println("<option value='AddPermission'" + (actionsList !=null && actionsList.contains("AddPermission") ? "selected" : "") + ">CNS:AddPermission</option>");
            out.println("<option value='Subscribe'" + (actionsList !=null && actionsList.contains("Subscribe") ? "selected" : "") + ">CNS:Subscribe</option></select>");
            out.println("<input type='checkbox' name='allActions' " + (allActions != null ? "checked" : "") +" onclick='if(this.checked) { document.getElementById(\"actions\").disabled=true; } else { document.getElementById(\"actions\").disabled=false; }'>All CNS Actions(CNS:*)</td></tr>");
            out.println("<tr><td colspan=2>&nbsp;</td></tr></table>");
            out.println("<input type='hidden' name='userId' value='"+ userId +"'>");
            out.println("<hr/>");
			out.println("<input type='button' value='Cancel' onclick='window.close();' style='float:right;'><input type='submit' value='Submit' name='Submit' style='float:right;'></form>");
            
			if (params.containsKey("Submit") && !validInput.equals("")) {
            	out.println("<p><font color='red'>" + validInput+ "</font>");
            }
			
			out.println("</body></html>");
		}
	}
	
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
