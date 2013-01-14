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

public class CMBHttpServletRequest implements HttpServletRequest {
	
	private HttpServletRequest r;
	
	private long requestReceivedTimestamp;
	private long waitTime;
	private boolean isActive;
	private boolean isQueuedForProcessing;

	private CQSQueue queue;
	private Map<String, String> receiveAttributes;
	private List<String> filterAttributes;
	
	public CMBHttpServletRequest(HttpServletRequest httpServletRequest) {
		r = httpServletRequest;
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
		return r.getAsyncContext();
	}

	@Override
	public Object getAttribute(String arg0) {
		return r.getAttribute(arg0);
	}

	@Override
	public Enumeration<String> getAttributeNames() {
		return r.getAttributeNames();
	}

	@Override
	public String getCharacterEncoding() {
		return r.getCharacterEncoding();
	}

	@Override
	public int getContentLength() {
		return r.getContentLength();
	}

	@Override
	public String getContentType() {
		return r.getContentType();
	}

	@Override
	public DispatcherType getDispatcherType() {
		return r.getDispatcherType();
	}

	@Override
	public ServletInputStream getInputStream() throws IOException {
		return r.getInputStream();
	}

	@Override
	public String getLocalAddr() {
		return r.getLocalAddr();
	}

	@Override
	public String getLocalName() {
		return r.getLocalName();
	}

	@Override
	public int getLocalPort() {
		return r.getLocalPort();
	}

	@Override
	public Locale getLocale() {
		return r.getLocale();
	}

	@Override
	public Enumeration<Locale> getLocales() {
		return r.getLocales();
	}

	@Override
	public String getParameter(String arg0) {
		return r.getParameter(arg0);
	}

	@Override
	public Map<String, String[]> getParameterMap() {
		return r.getParameterMap();
	}

	@Override
	public Enumeration<String> getParameterNames() {
		return r.getParameterNames();
	}

	@Override
	public String[] getParameterValues(String arg0) {
		return r.getParameterValues(arg0);
	}

	@Override
	public String getProtocol() {
		return r.getProtocol();
	}

	@Override
	public BufferedReader getReader() throws IOException {
		return r.getReader();
	}

	@Override
	public String getRealPath(String arg0) {
		return r.getRealPath(arg0);
	}

	@Override
	public String getRemoteAddr() {
		return r.getRemoteAddr();
	}

	@Override
	public String getRemoteHost() {
		return r.getRemoteHost();
	}

	@Override
	public int getRemotePort() {
		return r.getRemotePort();
	}

	@Override
	public RequestDispatcher getRequestDispatcher(String arg0) {
		return r.getRequestDispatcher(arg0);
	}

	@Override
	public String getScheme() {
		return r.getScheme();
	}

	@Override
	public String getServerName() {
		return r.getServerName();
	}

	@Override
	public int getServerPort() {
		return r.getServerPort();
	}

	@Override
	public ServletContext getServletContext() {
		return r.getServletContext();
	}

	@Override
	public boolean isAsyncStarted() {
		return r.isAsyncStarted();
	}

	@Override
	public boolean isAsyncSupported() {
		return r.isAsyncSupported();
	}

	@Override
	public boolean isSecure() {
		return r.isSecure();
	}

	@Override
	public void removeAttribute(String arg0) {
		r.removeAttribute(arg0);
	}

	@Override
	public void setAttribute(String arg0, Object arg1) {
		r.setAttribute(arg0, arg1);
	}

	@Override
	public void setCharacterEncoding(String arg0)
			throws UnsupportedEncodingException {
		r.setCharacterEncoding(arg0);
	}

	@Override
	public AsyncContext startAsync() {
		return r.startAsync();
	}

	@Override
	public AsyncContext startAsync(ServletRequest arg0, ServletResponse arg1) {
		return r.startAsync(arg0, arg1);
	}

	@Override
	public boolean authenticate(HttpServletResponse arg0) throws IOException,
			ServletException {
		return r.authenticate(arg0);
	}

	@Override
	public String getAuthType() {
		return r.getAuthType();
	}

	@Override
	public String getContextPath() {
		return r.getContextPath();
	}

	@Override
	public Cookie[] getCookies() {
		return r.getCookies();
	}

	@Override
	public long getDateHeader(String arg0) {
		return r.getDateHeader(arg0);
	}

	@Override
	public String getHeader(String arg0) {
		return r.getHeader(arg0);
	}

	@Override
	public Enumeration<String> getHeaderNames() {
		return r.getHeaderNames();
	}

	@Override
	public Enumeration<String> getHeaders(String arg0) {
		return r.getHeaders(arg0);
	}

	@Override
	public int getIntHeader(String arg0) {
		return r.getIntHeader(arg0);
	}

	@Override
	public String getMethod() {
		return r.getMethod();
	}

	@Override
	public Part getPart(String arg0) throws IOException, IllegalStateException,
			ServletException {
		return r.getPart(arg0);
	}

	@Override
	public Collection<Part> getParts() throws IOException,
			IllegalStateException, ServletException {
		return r.getParts();
	}

	@Override
	public String getPathInfo() {
		return r.getPathInfo();
	}

	@Override
	public String getPathTranslated() {
		return r.getPathTranslated();
	}

	@Override
	public String getQueryString() {
		return r.getQueryString();
	}

	@Override
	public String getRemoteUser() {
		return r.getRemoteUser();
	}

	@Override
	public String getRequestURI() {
		return r.getRequestURI();
	}

	@Override
	public StringBuffer getRequestURL() {
		return r.getRequestURL();
	}

	@Override
	public String getRequestedSessionId() {
		return r.getRequestedSessionId();
	}

	@Override
	public String getServletPath() {
		return r.getServletPath();
	}

	@Override
	public HttpSession getSession() {
		return r.getSession();
	}

	@Override
	public HttpSession getSession(boolean arg0) {
		return r.getSession(arg0);
	}

	@Override
	public Principal getUserPrincipal() {
		return r.getUserPrincipal();
	}

	@Override
	public boolean isRequestedSessionIdFromCookie() {
		return r.isRequestedSessionIdFromCookie();
	}

	@Override
	public boolean isRequestedSessionIdFromURL() {
		return r.isRequestedSessionIdFromURL();
	}

	@Override
	public boolean isRequestedSessionIdFromUrl() {
		return r.isRequestedSessionIdFromUrl();
	}

	@Override
	public boolean isRequestedSessionIdValid() {
		return r.isRequestedSessionIdValid();
	}

	@Override
	public boolean isUserInRole(String arg0) {
		return r.isUserInRole(arg0);
	}

	@Override
	public void login(String arg0, String arg1) throws ServletException {
		r.login(arg0, arg1);
	}

	@Override
	public void logout() throws ServletException {
		r.logout();
	}
}
