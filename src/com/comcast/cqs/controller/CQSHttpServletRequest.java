package com.comcast.cqs.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.Part;

import com.comcast.cqs.model.CQSQueue;

public class CQSHttpServletRequest implements HttpServletRequest {
	
	private volatile HttpServletRequest request;
	
	private volatile long requestReceivedTimestamp;
	private volatile long waitTime;
	private volatile boolean isActive;
	private volatile boolean isQueuedForProcessing;

	private volatile CQSQueue queue;
	private volatile Map<String, String> receiveAttributes;
	private volatile List<String> filterAttributes;
	
	public CQSHttpServletRequest(HttpServletRequest httpServletRequest) {
		request = httpServletRequest;
		isActive = true;
		isQueuedForProcessing = false;
		requestReceivedTimestamp = System.currentTimeMillis();
	}

	public long getWaitTime() {
		return waitTime;
	}

	public void setWaitTime(long waitTime) {
		this.waitTime = waitTime;
	}

	public List<String> getFilterAttributes() {
		return filterAttributes;
	}

	public void setFilterAttributes(List<String> filterAttributes) {
		this.filterAttributes = filterAttributes;
	}

	public CQSQueue getQueue() {
		return queue;
	}

	public void setQueue(CQSQueue queue) {
		this.queue = queue;
	}

	public Map<String, String> getReceiveAttributes() {
		return receiveAttributes;
	}

	public void setReceiveAttributes(Map<String, String> receiveAttributes) {
		this.receiveAttributes = receiveAttributes;
	}

	public long getRequestReceivedTimestamp() {
		return requestReceivedTimestamp;
	}

	public void setRequestReceivedTimestamp(long requestReceivedTimestamp) {
		this.requestReceivedTimestamp = requestReceivedTimestamp;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	public boolean isQueuedForProcessing() {
		return isQueuedForProcessing;
	}

	public void setIsQueuedForProcessing(boolean isQueuedForProcessing) {
		this.isQueuedForProcessing = isQueuedForProcessing;
	}

	@Override
	public AsyncContext getAsyncContext() {
		return request.getAsyncContext();
	}

	@Override
	public Object getAttribute(String arg0) {
		return request.getAttribute(arg0);
	}

	@Override
	public Enumeration<String> getAttributeNames() {
		return request.getAttributeNames();
	}

	@Override
	public String getCharacterEncoding() {
		return request.getCharacterEncoding();
	}

	@Override
	public int getContentLength() {
		return request.getContentLength();
	}

	@Override
	public String getContentType() {
		return request.getContentType();
	}

	@Override
	public DispatcherType getDispatcherType() {
		return request.getDispatcherType();
	}

	@Override
	public ServletInputStream getInputStream() throws IOException {
		return request.getInputStream();
	}

	@Override
	public String getLocalAddr() {
		return request.getLocalAddr();
	}

	@Override
	public String getLocalName() {
		return request.getLocalName();
	}

	@Override
	public int getLocalPort() {
		return request.getLocalPort();
	}

	@Override
	public Locale getLocale() {
		return request.getLocale();
	}

	@Override
	public Enumeration<Locale> getLocales() {
		return request.getLocales();
	}

	@Override
	public String getParameter(String arg0) {
		return request.getParameter(arg0);
	}

	@Override
	public Map<String, String[]> getParameterMap() {
		return request.getParameterMap();
	}

	@Override
	public Enumeration<String> getParameterNames() {
		return request.getParameterNames();
	}

	@Override
	public String[] getParameterValues(String arg0) {
		return request.getParameterValues(arg0);
	}

	@Override
	public String getProtocol() {
		return request.getProtocol();
	}

	@Override
	public BufferedReader getReader() throws IOException {
		return request.getReader();
	}

	@Override
	public String getRealPath(String arg0) {
		return request.getRealPath(arg0);
	}

	@Override
	public String getRemoteAddr() {
		return request.getRemoteAddr();
	}

	@Override
	public String getRemoteHost() {
		return request.getRemoteHost();
	}

	@Override
	public int getRemotePort() {
		return request.getRemotePort();
	}

	@Override
	public RequestDispatcher getRequestDispatcher(String arg0) {
		return request.getRequestDispatcher(arg0);
	}

	@Override
	public String getScheme() {
		return request.getScheme();
	}

	@Override
	public String getServerName() {
		return request.getServerName();
	}

	@Override
	public int getServerPort() {
		return request.getServerPort();
	}

	@Override
	public ServletContext getServletContext() {
		return request.getServletContext();
	}

	@Override
	public boolean isAsyncStarted() {
		return request.isAsyncStarted();
	}

	@Override
	public boolean isAsyncSupported() {
		return request.isAsyncSupported();
	}

	@Override
	public boolean isSecure() {
		return request.isSecure();
	}

	@Override
	public void removeAttribute(String arg0) {
		request.removeAttribute(arg0);
	}

	@Override
	public void setAttribute(String arg0, Object arg1) {
		request.setAttribute(arg0, arg1);
	}

	@Override
	public void setCharacterEncoding(String arg0)
			throws UnsupportedEncodingException {
		request.setCharacterEncoding(arg0);
	}

	@Override
	public AsyncContext startAsync() {
		return request.startAsync();
	}

	@Override
	public AsyncContext startAsync(ServletRequest arg0, ServletResponse arg1) {
		return request.startAsync(arg0, arg1);
	}

	@Override
	public boolean authenticate(HttpServletResponse arg0) throws IOException,
			ServletException {
		return request.authenticate(arg0);
	}

	@Override
	public String getAuthType() {
		return request.getAuthType();
	}

	@Override
	public String getContextPath() {
		return request.getContextPath();
	}

	@Override
	public Cookie[] getCookies() {
		return request.getCookies();
	}

	@Override
	public long getDateHeader(String arg0) {
		return request.getDateHeader(arg0);
	}

	@Override
	public String getHeader(String arg0) {
		return request.getHeader(arg0);
	}

	@Override
	public Enumeration<String> getHeaderNames() {
		return request.getHeaderNames();
	}

	@Override
	public Enumeration<String> getHeaders(String arg0) {
		return request.getHeaders(arg0);
	}

	@Override
	public int getIntHeader(String arg0) {
		return request.getIntHeader(arg0);
	}

	@Override
	public String getMethod() {
		return request.getMethod();
	}

	@Override
	public Part getPart(String arg0) throws IOException, IllegalStateException,
			ServletException {
		return request.getPart(arg0);
	}

	@Override
	public Collection<Part> getParts() throws IOException,
			IllegalStateException, ServletException {
		return request.getParts();
	}

	@Override
	public String getPathInfo() {
		return request.getPathInfo();
	}

	@Override
	public String getPathTranslated() {
		return request.getPathTranslated();
	}

	@Override
	public String getQueryString() {
		return request.getQueryString();
	}

	@Override
	public String getRemoteUser() {
		return request.getRemoteUser();
	}

	@Override
	public String getRequestURI() {
		return request.getRequestURI();
	}

	@Override
	public StringBuffer getRequestURL() {
		return request.getRequestURL();
	}

	@Override
	public String getRequestedSessionId() {
		return request.getRequestedSessionId();
	}

	@Override
	public String getServletPath() {
		return request.getServletPath();
	}

	@Override
	public HttpSession getSession() {
		return request.getSession();
	}

	@Override
	public HttpSession getSession(boolean arg0) {
		return request.getSession(arg0);
	}

	@Override
	public Principal getUserPrincipal() {
		return request.getUserPrincipal();
	}

	@Override
	public boolean isRequestedSessionIdFromCookie() {
		return request.isRequestedSessionIdFromCookie();
	}

	@Override
	public boolean isRequestedSessionIdFromURL() {
		return request.isRequestedSessionIdFromURL();
	}

	@Override
	public boolean isRequestedSessionIdFromUrl() {
		return request.isRequestedSessionIdFromUrl();
	}

	@Override
	public boolean isRequestedSessionIdValid() {
		return request.isRequestedSessionIdValid();
	}

	@Override
	public boolean isUserInRole(String arg0) {
		return request.isUserInRole(arg0);
	}

	@Override
	public void login(String arg0, String arg1) throws ServletException {
		request.login(arg0, arg1);
	}

	@Override
	public void logout() throws ServletException {
		request.logout();
	}
}
