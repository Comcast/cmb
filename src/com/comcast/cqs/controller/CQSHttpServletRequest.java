package com.comcast.cqs.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import com.comcast.cqs.model.CQSQueue;

public class CQSHttpServletRequest extends HttpServletRequestWrapper {
	
	private volatile long requestReceivedTimestamp;
	private volatile long waitTime;
	private volatile boolean isActive;
	private volatile boolean isQueuedForProcessing;

	private volatile CQSQueue queue;
	private volatile Map<String, String> receiveAttributes;
	private volatile List<String> filterAttributes;
	
	public CQSHttpServletRequest(HttpServletRequest httpServletRequest) {
		super(httpServletRequest);
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
}
