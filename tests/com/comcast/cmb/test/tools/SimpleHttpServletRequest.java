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

package com.comcast.cmb.test.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletInputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

public class SimpleHttpServletRequest implements HttpServletRequest {
    
	private String _pathInfo = null;
    private StringBuffer requestUrl = null;
    private Map<String, String[]> _parameters = null;
    private String method;
    private Map<String, String> headers = null;
    
    public SimpleHttpServletRequest() {
    	headers = new HashMap<String, String>();
    }

    @Override
    public Object getAttribute(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCharacterEncoding() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getContentLength() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getContentType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        return null;
    }

    @Override
    public String getLocalAddr() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getLocalName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getLocalPort() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Locale getLocale() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Enumeration<Locale> getLocales() {
        // TODO Auto-generated method stub
        return null;
    }

    public void setParameterMap(Map<String, String[]> params) {
        _parameters = params;
    }
    @Override
    public String getParameter(String arg0) {
        if (_parameters.get(arg0) == null) {
            return null;
        }
        return _parameters.get(arg0)[0];
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        return _parameters;
    }

    @Override
    public Enumeration<String> getParameterNames() {
        Set<String> keys = _parameters.keySet();
        Vector<String> keyV = new Vector<String>();
        for (String key : keys) {
            keyV.add(key);
        }
        return keyV.elements();
    }

    @Override
    public String[] getParameterValues(String arg0) {
        return _parameters.get(arg0);
    }

    @Override
    public String getProtocol() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BufferedReader getReader() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRealPath(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRemoteAddr() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRemoteHost() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getRemotePort() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getScheme() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getServerName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getServerPort() {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public boolean isSecure() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void removeAttribute(String arg0) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setAttribute(String arg0, Object arg1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setCharacterEncoding(String arg0)
            throws UnsupportedEncodingException {
        // TODO Auto-generated method stub
        
    }


    @Override
    public String getAuthType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getContextPath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Cookie[] getCookies() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getDateHeader(String arg0) {
        // TODO Auto-generated method stub
        return 0;
    }
    
    public void addHeader(String header, String value) {
    	headers.put(header, value);
    }

    @Override
    public String getHeader(String key) {
        return headers.get(key);
    }

    @Override
    public Enumeration<String> getHeaderNames() {
    	return Collections.enumeration(headers.keySet());
    }

    @Override
    public Enumeration<String> getHeaders(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getIntHeader(String arg0) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getMethod() {
        // TODO Auto-generated method stub
        return method;
    }
    
    public void setMethod(String method) {
    	this.method = method;
    }


    public void setPathInfo(String pathInfo) {
        _pathInfo = pathInfo;
    }
    
    @Override
    public String getPathInfo() {
        return _pathInfo;
    }

    @Override
    public String getPathTranslated() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getQueryString() {
    	
    	String queryString = "";
    	
    	for (String key : _parameters.keySet()) {
    		
    		if (queryString == null || queryString.equals("")) {
    			queryString = "?";
    		} else {
    			queryString += "&";
    		}
    		
    		queryString += key + "=" + ((String[])_parameters.get(key))[0];
    	}
        
        return queryString;
    }

    @Override
    public String getRemoteUser() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRequestURI() {
        // TODO Auto-generated method stub
        return null;
    }
    
    public void setRequestUrl(String url) {
        requestUrl = new StringBuffer(url);
    }

    @Override
    public StringBuffer getRequestURL() {
        // TODO Auto-generated method stub
        return requestUrl;
    }

    @Override
    public String getRequestedSessionId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getServletPath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HttpSession getSession() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HttpSession getSession(boolean arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Principal getUserPrincipal() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromUrl() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isRequestedSessionIdValid() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isUserInRole(String arg0) {
        // TODO Auto-generated method stub
        return false;
    }
}
