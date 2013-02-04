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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;

/**
 * Class used to display user information
 * @author bwolf, aseem, tina
 */
public class UserPageServlet extends AdminServletBase {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(UserPageServlet.class);
	
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		
		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}
		
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String userId = request.getParameter("userId");
		IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
		User user = null;
		
		try {
			user = userHandler.getUserById(userId);
		} catch (PersistenceException ex) {
			logger.error("event=get_user status=failed user_id=" + userId, ex);
			throw new ServletException(ex);
		}
		
		out.println("<html>");
		
		header(request, out, "User "+user.getUserName());
		
		out.println("<body>");

		out.println("<h2>User "+user.getUserName()+"</h2>");
		
		if (user != null) {
			out.println("<table><tr><td><b>User Name:</b></td><td>"+ user.getUserName()+"</td></tr>");
			out.println("<tr><td><b>User ID:</b></td><td>"+ user.getUserId()+"</td></tr>");
			out.println("<tr><td><b>Access Key:</b></td><td>"+user.getAccessKey()+"</td></tr>");
			out.println("<tr><td><b>Access Secret:</b></td><td>"+user.getAccessSecret()+"</td></tr>");
			out.println("<tr><td><b>Links:</b></td><td><a href='"+response.encodeURL("/CNSUser")+"?userId="+userId+"'>CNS</a>&nbsp;&nbsp;&nbsp;<a href='"+response.encodeURL("/CQSUser")+"?userId="+userId+"'>CQS</a></td></tr>");
			out.println("</table></html>");
		}
		
        out.println("</body></html>");
		
        logger.info("action=userPageServletProcess CassandraTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime));
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
}
