package com.comcast.cqs.controller;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.model.CQSQueue;

public class CQSHttpServletRequest extends HttpServletRequestWrapper {
	
	private static Logger logger = Logger.getLogger(CQSHttpServletRequest.class);
	private volatile long requestReceivedTimestamp;
	private volatile long waitTime;
	private volatile boolean isActive;
	private volatile boolean isQueuedForProcessing;

	private volatile CQSQueue queue;
	private volatile Map<String, String> receiveAttributes;
	private volatile List<String> filterAttributes;
	private volatile List<String> filterMessageAttributes;
	private volatile List<String> receiptHandles;
	
	private volatile Enumeration<String> postParameterNames;
	private volatile Enumeration<String> parameterNames;
	private volatile Map<String, String> postParameters=new HashMap <String, String>();
	private volatile Map<String, String> parameters=new HashMap <String, String>();
	private volatile Map<String, String[]> parameterMap= new HashMap<String, String[]>();
	private volatile String method ;
	private volatile boolean useStreamFlag=false;
	
	
	public CQSHttpServletRequest(HttpServletRequest httpServletRequest) {
		super(httpServletRequest);
		isActive = true;
		isQueuedForProcessing = false;
		requestReceivedTimestamp = System.currentTimeMillis();
		method = httpServletRequest.getMethod();
		useStreamFlag=(method!=null)&&method.equals("POST")&&CMBProperties.getInstance().getEnableSignatureAuth();

		if(useStreamFlag){
			setParameters(httpServletRequest);
		}
	}

	public List<String> getReceiptHandles() {
		return receiptHandles;
	}

	public void setReceiptHandles(List<String> receiptHandles) {
		this.receiptHandles = receiptHandles;
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

	public List<String> getFilterMessageAttributes() {
		return filterMessageAttributes;
	}

	public void setFilterMessageAttributes(List<String> filterMessageAttributes) {
		this.filterMessageAttributes = filterMessageAttributes;
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
	
	public Enumeration<java.lang.String> getParameterNames(){
		if(useStreamFlag){
			return parameterNames;
		}else {
			return super.getParameterNames();
		}
		
	}
	
	public Enumeration<java.lang.String> getPostParameterNames(){
		if(useStreamFlag){
			return postParameterNames;
		}else {
			return null;
		}
	}
	public String getParameter(String name){
		if(useStreamFlag){
			return parameters.get(name);
		}else {
			return super.getParameter(name);
		}
	}
	
	public String getPostParameter(String name){
		if(useStreamFlag){
			return postParameters.get(name);
		}else {
			return null;
		}		
	}
	
	private void setParameters(HttpServletRequest reqeust) {
		// read from stream, set parameters in queryurl and also in post paramter
		StringBuilder stringBuilder = new StringBuilder(1000);
		Scanner scanner = null;
		try {
			scanner = new Scanner(reqeust.getInputStream());
			while (scanner.hasNextLine()) {
				stringBuilder.append(scanner.nextLine());
			}
		} catch (IllegalStateException e) {
			logger.error("event=failed_to_read_post_body ", e);
		} catch (IOException e) {
			logger.error("event=failed_to_read_post_body ", e);
		} finally {
			if (scanner != null)
				scanner.close();
		}
		String body = stringBuilder.toString();
		if (body == null || body.length() == 0) {
			return;
		}

		// set parameterNames and parameters
		// set parameterMap in setParameterByString
		Vector<String> nameVector = new Vector<String>();
		parameterNames = setParameterByString(reqeust.getQueryString(),
				parameters, nameVector);

		// set postParameterNames and postParameters
		Vector<String> postNameVector = new Vector<String>();
		postParameterNames = setParameterByString(body, postParameters,
				postNameVector);

		// add post parameters to parameters
		nameVector.addAll(postNameVector);
		parameterNames = nameVector.elements();
		parameters.putAll(postParameters);
	}

	private Enumeration<String> setParameterByString(String input,
			Map<String, String> map, Vector<String> vector) {
		if (input == null || input.length() == 0) {
			return null;
		}

		String[] pairs = input.split("&");
		// set postParameterNames and postParameters

		String currentName;
		String currentValue;

		for (int i = 0; i < pairs.length; i++) {
			currentName = pairs[i].substring(0, pairs[i].indexOf("="));
			try {
				currentName = URLDecoder.decode(currentName, "UTF-8");
			} catch (Exception e) {
				logger.error("event=failed_to_decode_parameter_name ", e);
			}
			currentValue = pairs[i].substring(pairs[i].indexOf("=") + 1);
			try {
				currentValue = URLDecoder.decode(currentValue, "UTF-8");
			} catch (Exception e) {
				logger.error("event=failed_to_decode_parameter_value ", e);
			}
			vector.add(currentName);
			map.put(currentName, currentValue);
			// populate parameterMap
			if (parameterMap.containsKey(currentName)) {
				String[] valueArray = parameterMap.get(currentName);
				String[] newValueArray = new String[valueArray.length + 1];
				for (int j = 0; j < valueArray.length; j++) {
					newValueArray[j] = valueArray[j];
				}
				newValueArray[newValueArray.length - 1] = currentValue;
				parameterMap.put(currentName, newValueArray);

			} else {
				String[] valueArray = new String[1];
				valueArray[0] = currentValue;
				parameterMap.put(currentName, valueArray);
			}
		}
		return vector.elements();
	}

	public Map<String, String[]> getParameterMap() {
		if (!useStreamFlag) {
			return super.getParameterMap();
		}
		if (parameterMap.size() > 0) {
			return parameterMap;
		} else {
			for (Map.Entry<String, String> currentEntry : parameters.entrySet()) {
				String[] valueArray = new String[1];
				valueArray[0] = currentEntry.getValue();
				parameterMap.put(currentEntry.getKey(), valueArray);
			}
			return parameterMap;
		}
	}
}
