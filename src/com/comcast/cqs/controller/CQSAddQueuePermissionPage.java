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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.util.Util;

/**
 * Add queue permission
 * @author bwolf, tina, aseem, baosen
 *
 */
public class CQSAddQueuePermissionPage extends AdminServletBase {

	private static final long serialVersionUID = 1L;
	private Logger logger = Logger.getLogger(CQSAddQueuePermissionPage.class);
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String queueName = request.getParameter("queueName");
		String userId = request.getParameter("userId");
		String allow = request.getParameter("allow");
		String users = request.getParameter("users");
		String everybody = request.getParameter("everybody");
		String[] actions = request.getParameterValues("actions");
		String allActions = request.getParameter("allActions");
		
		String queueUrl = Util.getAbsoluteQueueUrlForName(queueName, userId);
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
		
		String validInput = checkValidInput(usersList, actionsList);
		
		Map<?, ?> params = request.getParameterMap();
		
		out.println("<html>");
		
		simpleHeader(request, out, "Add Permission to Queue "+ queueName);
		
		if (params.containsKey("Add") && validInput.equals("")) {
			
			String sid = Long.toHexString(Double.doubleToLongBits(Math.random()));
			
			if (allow.equals("allow")) {
				
				if (usersList != null && usersList.size() > 0 && actionsList != null && actionsList.size() > 0) {
					
					try {
						AddPermissionRequest addPermissionRequest = new AddPermissionRequest(queueUrl, sid, usersList, actionsList);
						sqs.addPermission(addPermissionRequest);
						logger.debug("event=add_permission queue_url=" + queueUrl + " label=" + sid + " user_id=" + userId);
					} catch (Exception ex) {
						logger.error("event=add_permission queue_url=" + queueUrl + " label=" + sid + " user_id=" + userId, ex);
						throw new ServletException(ex);
					}
				} 
			}
			
			out.println("<body onload='javascript:window.opener.location.reload();window.close();'>");
		
		} else {
		
			out.println("<body>");
			out.println("<h1>Add Permission to Queue "+ queueName + "</h1>");
			out.print("<form action=\"");
            out.print(response.encodeURL("AddPermission") + "?queueName="+queueName);
            out.print("\" ");
            out.println("method=POST>");
            out.println("<input type='hidden' name='userId' value='"+ userId +"'>");
            out.println("<p>Permissions enable you to control which operations a user can perform on a queue.</p>");
            out.println("<table><tr><td colspan=2>&nbsp;</td></tr>");
            out.println("<tr><td><b>Effect:</b></td><td><input type='radio' name='allow' value='allow' checked>Allow</td></tr>");
            
            out.println("<tr><td><b>Principal:</b></td><td><input type='text' name='users' size=80 id='users' value='" + (users == null ? "" : users) +"'>");
            out.println("<input type='checkbox' name='everybody' " +  (everybody != null ? "checked" : "") + " onclick='if(this.checked) {document.getElementById(\"users\").disabled=true;} else {document.getElementById(\"users\").disabled=false;}'>Everybody(*) </td></tr>");
            out.println("<tr><td>&nbsp;</td><td><font color='grey'>Use commas between multiple principals.</font></td></tr>");
            out.println("<tr><td colspan=2>&nbsp;</td></tr>");
            out.println("<tr><td valign=top><b>Actions:</b></td><td valign=top><select name='actions' multiple size='6' id='actions'>");
            
            out.println("<option value='SendMessage'" + (actionsList !=null && actionsList.contains("SendMessage") ? "selected" : "") + ">SendMessage</option>");
            out.println("<option value='ReceiveMessage'" + (actionsList !=null && actionsList.contains("ReceiveMessage") ? "selected" : "") + ">ReceiveMessage</option>");
            out.println("<option value='DeleteMessage'" + (actionsList !=null && actionsList.contains("DeleteMessage") ? "selected" : "") + ">DeleteMessage</option>");
            out.println("<option value='ChangeMessageVisibility'" + (actionsList !=null && actionsList.contains("ChangeMessageVisibility") ? "selected" : "") + ">ChangeMessageVisibility</option>");
            out.println("<option value='GetQueueAttributes'" + (actionsList !=null && actionsList.contains("GetQueueAttributes") ? "selected" : "") + ">GetQueueAttributes</option>");
            out.println("<option value='GetQueueUrl'" + (actionsList !=null && actionsList.contains("GetQueueUrl") ? "selected" : "") + ">GetQueueUrl</option></select>");
            out.println("<input type='checkbox' name='allActions' " + (allActions != null ? "checked" : "") +" onclick='if(this.checked) { document.getElementById(\"actions\").disabled=true; } else { document.getElementById(\"actions\").disabled=false; }'>All CQS Actions(CQS:*)</td></tr>");
            out.println("<tr><td colspan=2>&nbsp;</td></tr></table>");
            out.println("<hr/>");
			out.println("<input type='button' value='Cancel' onclick='window.close();' style='float:right;'><input type='submit' value='Add Permission' name='Add' style='float:right;'></form>");
            
			if (params.containsKey("Add") && !validInput.equals("")) {
            	out.println("<p><font color='red'>" + validInput+ "</font></p>");
            }
			
			out.println("</body></html>");
		}
		
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	public static String checkValidInput(List<String> usersList, List<String> actionsList) {
		
		String msg = "";
		
		if (usersList != null && usersList.size() > 0) {
		
			Iterator<String> i = usersList.iterator();
			
			while (i.hasNext()) {
			
				String userId1 = (String)i.next();
				
				if (!userId1.equals("*")) {
				
					IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
					
					try {
						
						if (userHandler.getUserById(userId1) == null) {
							msg = "Invalid User Id!";
						}
						
					} catch (PersistenceException e) {
						msg = e.getMessage();
					}
				}
			}
			
		} else {
			msg = "No User input!";
		}

		if (actionsList == null) {
			msg += " No Action selected!";
		}
		
		return msg;
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
