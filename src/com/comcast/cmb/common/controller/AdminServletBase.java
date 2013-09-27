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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;
import org.jfree.util.Log;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;

/**
 * Admin servlet base
 * @author bwolf
 *
 */
public abstract class AdminServletBase extends HttpServlet {

    private static final long serialVersionUID = 1L;
    
	public static final String cnsServiceBaseUrl = CMBProperties.getInstance().getCNSServiceUrl();
	public static final String cqsServiceBaseUrl = CMBProperties.getInstance().getCQSServiceUrl();
	
    protected volatile User user = null;
    
    protected volatile AmazonSQSClient sqs = null;
    protected volatile AmazonSNSClient sns = null;

    private volatile BasicAWSCredentials awsCredentials = null;
    
	private static Logger logger = Logger.getLogger(AdminServletBase.class);
	
	protected boolean isAuthenticated(HttpServletRequest request) {
		
		HttpSession session = request.getSession(true);
		
		if (session.getAttribute("USER") != null) {
			return true;
		} else {
			return false;
		}
	}
	
	protected User getAuthenticatedUser(HttpServletRequest request) {
		
		if (isAuthenticated(request)) {
			return (User)request.getSession(true).getAttribute("USER");
		} else {
			return null;
		}
	}

	protected boolean isAdmin(HttpServletRequest request) {
		
		if (!isAuthenticated(request)) {
			return false;
		}
		
		User user = (User)request.getSession(true).getAttribute("USER");
		return user.getIsAdmin();
	}
	
	protected boolean redirectUnauthenticatedUser(HttpServletRequest request, HttpServletResponse response) throws IOException {

		if (!isAuthenticated(request)) {
			response.sendRedirect(response.encodeURL("/webui/userlogin"));
			return true;
		}
		
		return false;
	}
	
	protected boolean redirectNonAdminUser(HttpServletRequest request, HttpServletResponse response) throws IOException {

		if (!isAuthenticated(request)) {
			response.sendRedirect(response.encodeURL("/webui/userlogin"));
			return true;
		}
		
		if (!isAdmin(request)) {
			response.sendRedirect(response.encodeURL("/webui/user?userId=" + getAuthenticatedUser(request).getUserId()));
			return true;
		}
	
		return false;
	}

	protected void logout(HttpServletRequest request, HttpServletResponse response) throws IOException {

		HttpSession session = request.getSession(true);
		
		if (session.getAttribute("USER") != null) {
			logger.info("event=logout user_name=" + ((User)session.getAttribute("USER")).getUserName() + " user_id=" + ((User)session.getAttribute("USER")).getUserId());
			session.removeAttribute("USER");
		}

		response.sendRedirect(response.encodeURL("/webui/userlogin"));
	}

	/**
     * Method to set the aws credentials for sqs and sns handlers
     * @param userId
     * @throws ServletException
     */
    protected void connect(HttpServletRequest request) throws ServletException {
    	
        String userId = request.getParameter("userId");
        
        IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
		
		try {
			user = userHandler.getUserById(userId);
		} catch (PersistenceException ex) {
			throw new ServletException(ex);
		}

        if (user == null) {	          
        	throw new ServletException("User " + userId + " does not exist");
        }
        
        if (!user.getUserName().equals(getAuthenticatedUser(request).getUserName()) && !getAuthenticatedUser(request).getIsAdmin()) {
        	throw new ServletException("Only admin may impersonate other users");
        }
		
        awsCredentials = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
        
        sqs = new AmazonSQSClient(awsCredentials);
        sqs.setEndpoint(CMBProperties.getInstance().getCQSServiceUrl());
        
        sns = new AmazonSNSClient(awsCredentials);
        sns.setEndpoint(CMBProperties.getInstance().getCNSServiceUrl());
    }
    
    protected void simpleHeader(HttpServletRequest request, PrintWriter out, String title) throws ServletException {

    	out.println("<head>");
    	out.println("<meta content='text/html; charset=UTF-8' http-equiv='Content-Type'/>");
    	out.println("<title>CMB Admin - " + title + "</title>");
    	
    	//out.println("<link rel='stylesheet' type='text/css' href='/global.css'/>");
    	
    	out.println("<style media='screen' type='text/css'>");
    	
    	try {
    	
	    	BufferedReader br = new BufferedReader(new FileReader(getServletContext().getRealPath("WEB-INF/global.css")));
	    	String line;
	    	StringBuffer sb = new StringBuffer("");
	    	
	    	while ((line = br.readLine()) != null) {
	    	   sb.append(line).append("\n");
	    	}

	    	br.close();
	    	
	    	out.println(sb.toString());
    	
    	} catch (Exception ex) {
    		Log.error("event=failed_to_read_css", ex);
    	}

    	out.println("</style>");
    	
    	out.println("</head>");
    }

    /**
     * Generate standard heading in the response. 
     * @param out
     * @throws ServletException
     * @throws IOException 
     */
    protected void header(HttpServletRequest request, PrintWriter out, String title) throws ServletException {
    	
    	simpleHeader(request, out, title);
    	
    	if (isAuthenticated(request)) {
    		
    		out.println("<span class='header'>");
    		out.println("<table width='100%' border='0'><tr><td width='100%' align='left'>Welcome " + getAuthenticatedUser(request).getUserName() + " | ");
    		
    		if (isAdmin(request)) {
    			out.println("<a href='/webui'>All Users</a>" + " | ");
    			out.println("<a href='/webui/cnsworkerstate'>CNS Dashboard</a>" + " | ");
    			out.println("<a href='/webui/cqsapistate'>CQS Dashboard</a>" + " | ");
    		}
    		
    		out.println("<a href='/webui/userlogin?Logout=Logout'>logout</a></td></tr></table>");
    		out.println("</span>");
    	}
    	
    	out.println("<h1>CMB - Comcast Message Bus - V " + CMBControllerServlet.VERSION +"</h1>");
    	
    	if (!CMBProperties.getInstance().getCMBDeploymentName().equals("")) {
    		out.println("<h3>" + CMBProperties.getInstance().getCMBDeploymentName() + "</h3>");
    	}
    }
    
    protected String httpGet(String urlString) {
        
    	URL url;
    	HttpURLConnection conn;
    	BufferedReader br;
    	String line;
    	String doc = "";

    	try {

    		url = new URL(urlString);
    		conn = (HttpURLConnection)url.openConnection();
    		conn.setRequestMethod("GET");
    		br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

    		while ((line = br.readLine()) != null) {
    			doc += line;
    		}

    		br.close();

    		logger.info("event=http_get url=" + urlString);

    	} catch (Exception ex) {
    		logger.error("event=http_get url=" + urlString, ex);
    	}

    	return doc;
    }
}
