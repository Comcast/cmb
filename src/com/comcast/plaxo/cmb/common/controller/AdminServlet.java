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

package com.comcast.plaxo.cmb.common.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cmb.common.util.ValueAccumulator.AccumulatorName;


/**
 * Administrator application.
 * @author bwolf, aseem, baosen, tina, michael
 */
public class AdminServlet extends AdminServletBase {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(AdminServlet.class);
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		Map<?, ?> parameters = request.getParameterMap();
		String userName = request.getParameter("user");
		String passwd = request.getParameter("password");
		IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
		
		if (parameters.containsKey("Create")) {
			
			try {
				userHandler.createUser(userName, passwd);
				logger.debug("event=create_user status=success user_name=" + userName);
			} catch (PersistenceException ex) {
				logger.error("event=create_user status=failed user_name=" + userName, ex);
				throw new ServletException(ex);
			}
			
		} else if (parameters.containsKey("Delete")) {
			
			try {
				userHandler.deleteUser(userName);
				logger.debug("event=delete_user status=success user_name=" + userName);
			} catch (PersistenceException ex) {
				logger.error("event=delete_user status=failed user_name=" + userName, ex);
				throw new ServletException(ex);
			}
		}
		
		out.println("<html>");
		out.println("<head><title>All Users</title></head><body>");

		header(out);

		out.println("<h2>All Users</h2>");
        out.print("<table><tr><td>UserName</td><td>Password</td><td></td></tr>");
        out.print("<form action=\"");
        out.print(response.encodeURL("ADMIN"));
        out.print("\" ");
        out.println("method=POST>");
        out.print("<tr><td><input type='text' name='user' /></td><td><input type='password' name='password'></td><td><input type='submit' value='Create' name='Create' /></td></tr></form></table>");
        List<User> users = new ArrayList<User>();
		
        try {
			
        	users = userHandler.getAllUsers();
			Collections.sort(users, new Comparator() { 
	            public int compare(Object o1, Object o2) {
	                User u1 = (User) o1;
	                User u2 = (User) o2;
	               return u1.getUserName().compareToIgnoreCase(u2.getUserName());
	            }
	 
	        });
			
        } catch (PersistenceException ex) {
			logger.error("event=get_all_users status=failed", ex);
			throw new ServletException(ex);
		}

        for (int i = 0; users != null && i < users.size(); i++) {
        
        	if (i == 0) {
        		out.println("<p><hr width='80%' align='left' /><p><table border='1' width='80%'>");
        		out.println("<tr><td><b>User Name</b></td>");
        		out.println("<td><b>User ID</b></td>");
        		out.println("<td><b>Access Key</b></td>");
        		out.println("<td><b>Access Secret</b></td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>");
        	}
        	
        	User user = (User)users.get(i);
        	
        	out.print("<form action=\"");
            out.print(response.encodeURL("ADMIN"));
            out.print("\" ");
            out.println("method=POST>");
        	out.println("<tr><td>"+user.getUserName() +"<input type='hidden' name='user' value="+user.getUserName()+"></td>");
        	out.println("<td>"+user.getUserId()+"</td>");
        	out.println("<td>"+user.getAccessKey()+"</td>");
        	out.println("<td>"+user.getAccessSecret()+"</td>");
        	out.println("<td><a href='"+cnsAdminBaseUrl+response.encodeURL("CNSUser") + "?userId="+user.getUserId()+"'>CNS</a></td>");
        	out.println("<td><a href='"+cqsAdminBaseUrl+response.encodeURL("CQSUser") + "?userId="+user.getUserId()+"'>CQS</a></td>");
		    
        	out.println("<td><input type='submit' value='Delete' name='Delete' /></td></tr></form>");
        }
        
        out.println("</table></body></html>");
        
        logger.info("action=adminServletProcess CassandraTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime));
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		doGet(request, response);
	}
}