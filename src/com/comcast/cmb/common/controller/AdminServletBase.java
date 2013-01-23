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
    
    public static final String cnsAdminBaseUrl = CMBProperties.getInstance().getCNSAdminUrl().endsWith("/") ? CMBProperties.getInstance().getCNSAdminUrl() : CMBProperties.getInstance().getCNSAdminUrl() + "/";
	public static final String cqsAdminBaseUrl = CMBProperties.getInstance().getCQSAdminUrl().endsWith("/") ? CMBProperties.getInstance().getCQSAdminUrl() : CMBProperties.getInstance().getCQSAdminUrl() + "/";
	
	public static final String cnsAdminUrl = cnsAdminBaseUrl + "ADMIN";
	public static final String cqsAdminUrl = cqsAdminBaseUrl + "ADMIN";
	
	public static final String cnsServiceBaseUrl = CMBProperties.getInstance().getCNSServerUrl();
	public static final String cqsServiceBaseUrl = CMBProperties.getInstance().getCQSServerUrl();
	
    protected volatile User user = null;
    
    protected volatile AmazonSQS sqs = null;
    protected volatile AmazonSNS sns = null;

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
		
		if (!CMBProperties.getInstance().getCnsUserName().equals(user.getUserName())) {
			return false;
		}
		
		return true;
	}
	
	protected boolean redirectUnauthenticatedUser(HttpServletRequest request, HttpServletResponse response) throws IOException {

		if (!isAuthenticated(request)) {
			response.sendRedirect(response.encodeURL("/UserLogin"));
			return true;
		}
		
		return false;
	}
	
	protected boolean redirectNonAdminUser(HttpServletRequest request, HttpServletResponse response) throws IOException {

		if (!isAuthenticated(request)) {
			response.sendRedirect(response.encodeURL("/UserLogin"));
			return true;
		}
		
		if (!isAdmin(request)) {
			response.sendRedirect(response.encodeURL("/User?userId=" + getAuthenticatedUser(request).getUserId()));
			return true;
		}
	
		return false;
	}

	protected void logout(HttpServletRequest request, HttpServletResponse response) throws IOException {

		HttpSession session = request.getSession(true);
		
		if (session.getAttribute("USER") != null) {
			session.removeAttribute("USER");
		}

		response.sendRedirect(response.encodeURL("/UserLogin"));
	}

	/**
     * Method to set the awsCredentials and the appt instances of sqs and sns interfaces
     * @param userId
     * @throws ServletException
     */
    protected void connect(String userId) throws ServletException {
    	
		IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
		try {
			user = userHandler.getUserById(userId);
		} catch (PersistenceException ex) {
			throw new ServletException(ex);
		}

        if (user == null) {	          
        	throw new ServletException("User " + userId + " does not exist");
        }
		
        awsCredentials = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
        
        sqs = new AmazonSQSClient(awsCredentials);
        sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());
        
        sns = new AmazonSNSClient(awsCredentials);
        sns.setEndpoint(CMBProperties.getInstance().getCNSServerUrl());
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
    		out.println("<table width='100%' border='0'><tr><td width='100%' align='left'>Welcome "+getAuthenticatedUser(request).getUserName()+" | <a href='/UserLogin?Logout=Logout'>logout</a></td></tr></table>");
    		out.println("</span>");
    	}
    	
    	out.println("<h1>CMB - Comcast Message Bus - V " + CMBControllerServlet.VERSION +"</h1>");
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
        
        } catch (Exception ex) {
           logger.error("event=failed_http_get", ex);
        }
        
        return doc;
     }
}
