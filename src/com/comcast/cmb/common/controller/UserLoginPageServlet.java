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
package com.comcast.cmb.common.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.AuthUtil;
import com.comcast.cmb.common.util.CMBProperties;

/**
 * Admin page for user login
 * @author aseem, bwolf, tina
 *
 */
public class UserLoginPageServlet extends AdminServletBase {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(UserLoginPageServlet.class);
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		Map<?, ?> parameters = request.getParameterMap();
		String userName = request.getParameter("user");
		userName = userName == null ? "" : userName;
		String password = request.getParameter("passwd");
		IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
		User user = null;
		
		if (parameters.containsKey("Login")) {
			
			try {
				
				user = userHandler.getUserByName(userName);
				HttpSession session = request.getSession(true);

				if (user!=null && AuthUtil.verifyPassword(password, user.getHashPassword())) {
					logger.info("event=login_admin_ui user_name=" + userName + " user_id=" + user.getUserId());
					session.setAttribute("USER", user);
				} else if (user==null && CMBProperties.getInstance().getCNSUserName().equals(userName) && CMBProperties.getInstance().getCNSUserPassword().equals(password)) {
					logger.warn("event=login_admin_ui action=created_missing_admin_user user_name=" + userName);
					userHandler.createUser(userName, password);
					user = userHandler.getUserByName(userName);
					session.setAttribute("USER", user);
				} else {
					logger.warn("event=login_admin_ui user_name=" + userName);
					user = null;
					session.removeAttribute("USER");
				}
				
			} catch (Exception ex) {
				logger.error("event=login_admin_ui user_name=" + userName, ex);
				throw new ServletException(ex);
			}
			
		} else if (parameters.containsKey("Logout")) {
			logout(request, response);
		}
		
		if (user != null) {
			
			if (isAdmin(request)) {
				response.sendRedirect(response.encodeURL("/ADMIN?userId="+ user.getUserId()));
			} else {
				response.sendRedirect(response.encodeURL("/User?userId="+ user.getUserId()));
			}
			
		} else {
			
			out.println("<html>");
			
			header(request, out, "User Login");
			
			out.println("<body>");

			out.println("<h2>User Login</h2>");
			
			if (parameters.containsKey("Login")) {
				out.println("<p><font color='red'>User doesn't exist or password does not match!</font>");
			}
			
			out.println("<form action=\"/UserLogin\" method=POST>");
	        out.println("<table><tr><td>Username:</td><td><input type='text' name='user' value='"+ userName + "'></td></tr>");
	        out.println("<tr><td>Password:</td><td><input type='password' name='passwd'></td></tr>");
	        out.println("<tr><td>&nbsp;</td></tr>");
	        out.println("<tr><td><input type='submit' value='Login' name='Login' /></td></tr></table></form></body></html>");
	        out.println("</body></html>");
		}
		
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		doGet(request, response);
	}
}
