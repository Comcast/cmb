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
package com.comcast.cqs.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.SuperCfResult;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import javax.servlet.http.HttpServletRequest;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.model.CQSMessage;
/**
 * Utility functions for cqs
 * @author bwolf, vvenkatraman, baosen
 *
 */
public class Util {
	
	public static boolean isValidQueueUrl(String arn) {

		Pattern pattern = Pattern.compile("http://([A-Za-z0-9-_]+.)+[A-Za-z0-9-_]+/[A-Za-z0-9-_]+");
		Matcher matcher = pattern.matcher(arn);
		
		return matcher.matches();
	}
	
	public static boolean isValidQueueArn(String arn) {
		
		Pattern pattern = Pattern.compile("arn:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+");
		Matcher matcher = pattern.matcher(arn);

		return matcher.matches();
	}
	
	public static String getQueueOwnerFromArn(String arn) {
		
		if (!isValidQueueArn(arn)) {
			return null;
		}
		
		String elements[] = arn.split(":");
		
		return elements[4];
	}
	
	public static String getAbsoluteQueueUrlForArn(String arn) {
		
		if (arn == null) {
			return null;
		}
		
		// do not check prefix
				
		String elements[] = arn.split(":");
		
		if (elements.length != 6) {
			return null;
		}
		
		String url = CMBProperties.getInstance().getCQSServiceUrl();
		
		if (!url.endsWith("/")) {
			url += "/";
		}
		
		url += elements[4] + "/" + elements[5];
		
		return url;
	}

	public static String getRelativeQueueUrlForArn(String arn) {
		
		if (arn == null) {
			return null;
		}
		
		// do not check prefix
				
		String elements[] = arn.split(":");
		
		if (elements.length != 6) {
			return null;
		}
		
		String url = elements[4] + "/" + elements[5];
		
		return url;
	}

	public static String getNameForArn(String arn) {
		
		if (arn == null) {
			return null;
		}
		
		// do not check prefix
				
		String elements[] = arn.split(":");
		
		if (elements.length != 6) {
			return null;
		}
		
		String name = elements[5];
		
		return name;
	}

	public static String getAbsoluteQueueUrlForName(String queueName, String userId) {
		
		
		String url = CMBProperties.getInstance().getCQSServiceUrl();
		
		if (!url.endsWith("/")) {
			url += "/";
		}
		
		url += userId + "/" + queueName;
		
		return url;
	}

	public static String getRelativeQueueUrlForName(String queueName, String userId) {
		
		String url = userId + "/" + queueName;
		
		return url;
	}

	public static String getRelativeForAbsoluteQueueUrl(String url) {
		
		if (url == null) {
			return null;
		}
		
		String elements[] = url.split("/");
		
		if (elements.length != 5) {
			return null;
		}
		
		return elements[3] + "/" + elements[4];
	}

	
	public static String getNameForAbsoluteQueueUrl(String url) {
		
		if (url == null) {
			return null;
		}
		
		String elements[] = url.split("/");
		
		if (elements.length != 5) {
			return null;
		}
		
		return elements[4];
	}

	public static String getUserIdForAbsoluteQueueUrl(String url) {
		
		if (url == null) {
			return null;
		}
		
		String elements[] = url.split("/");
		
		if (elements.length != 5) {
			return null;
		}
		
		return elements[3];
	}

	public static String getUserIdForRelativeQueueUrl(String url) {
		
		if (url == null) {
			return null;
		}
		
		String elements[] = url.split("/");
		
		if (elements.length != 2) {
			return null;
		}
		
		return elements[0];
	}

	public static String getArnForAbsoluteQueueUrl(String url) {
		
		if (url == null) {
			return null;
		}
		
		String elements[] = url.split("/");
		
		if (elements.length != 5) {
			return null;
		}
		
		String arn = "arn:cmb:cqs:" + CMBProperties.getInstance().getRegion() + ":" + elements[3] + ":" + elements[4];

		return arn;
	}
	
	public static String getArnForRelativeQueueUrl(String url) {
		
		if (url == null) {
			return null;
		}
		
		String elements[] = url.split("/");
		
		if (elements.length != 2) {
			return null;
		}
		
		String arn = "arn:cmb:cqs:" + CMBProperties.getInstance().getRegion() + ":" + elements[0] + ":" + elements[1];

		return arn;
	}

	/*
	 * user supplied message id in CQS batch actions can only contain alphanumeric, hyphen, and underscore
	 */
    public static boolean isValidId(String str) {
        
    	if (str.length() > CMBProperties.getInstance().getCQSMaxMessageSuppliedIdLength()) {
            return false;
        }
        
    	Pattern pattern = Pattern.compile("^[a-zA-Z0-9_-]+$");
        Matcher matcher = pattern.matcher(str);
        
        return matcher.find();
    }

    public static boolean isParsableToInt(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
	public static Map<String, String> buildMessageMap(CQSMessage message) {
		
		Map<String, String> messageMap = new HashMap<String, String>();
		
		if (message == null) {
			return messageMap;
		}
		
		messageMap.put("MessageId", message.getMessageId());
		messageMap.put("MD5OfBody", message.getMD5OfBody());
		messageMap.put("Body", message.getBody());
		
		if (message.getAttributes() == null) {
			message.setAttributes(new HashMap<String, String>());
		}
		
		if (message.getAttributes() == null	|| !message.getAttributes().containsKey(CQSConstants.SENT_TIMESTAMP)) {
			message.getAttributes().put(CQSConstants.SENT_TIMESTAMP, "" + Calendar.getInstance().getTimeInMillis());
		}
		
		if (message.getAttributes() == null	|| !message.getAttributes().containsKey(CQSConstants.APPROXIMATE_RECEIVE_COUNT)) {
			message.getAttributes().put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, "0");
		}
		
		if (message.getAttributes() != null) {
			
			for (String key : message.getAttributes().keySet()) {
				
				String value = message.getAttributes().get(key);
				
				if (value == null || value.isEmpty()) {
					value = "";
				}
				
				messageMap.put(key, value);
			}
		}
		return messageMap;
	}
	
    /*
     * process get requests Attribute.n regardless of ordinal
     */
    public static List<String> fillAllGetAttributesRequests(HttpServletRequest request) {
        
    	List<String> filterRequests = new ArrayList<String>();
        Map<String, String[]> requestParams = request.getParameterMap();
        
        for (String k: requestParams.keySet()) {
            
        	if (k.contains(CQSConstants.ATTRIBUTE_NAME)) {
                filterRequests.add(requestParams.get(k)[0]);
            }
        }
        
        return filterRequests;
    }

    /*
     * process set requests Attribute.n.Name/Attribute.n.Value regardless of ordinal
     */
    public static HashMap<String, String> fillAllSetAttributesRequests(HttpServletRequest request) throws CMBException {
    	
        HashMap<String, String> filterRequests = new HashMap<String, String>();
        Map<String, String[]> requestParams = request.getParameterMap();
        Pattern p = Pattern.compile("(Attribute\\.(\\d*\\.)?)Name");
        
        boolean found = false;
        
        for (String k: requestParams.keySet()) {
        	
            Matcher m = p.matcher(k);
            
            if (m.find()) {
            	
                found = true;
            	String v = m.group(1) + "Value";
                
                if (requestParams.get(v) == null) {
                    throw new CMBException(CMBErrorCodes.MissingParameter, "The request must contain the parameter Attribute." + m.group(1) + "Value");
                }
                
                filterRequests.put(requestParams.get(k)[0], requestParams.get(v)[0]);
            }
        }
        
        if (!found) {
            throw new CMBException(CMBErrorCodes.MissingParameter, "The request must contain the parameter Attribute.Name");
        }
        
        return filterRequests;
    }

	public static String hashQueueUrl(String queueUrl) throws NoSuchAlgorithmException, UnsupportedEncodingException {
	    
		MessageDigest digest = MessageDigest.getInstance("MD5");
	    String toBeHashed = queueUrl;
	    byte [] hashed = digest.digest(toBeHashed.getBytes("UTF-8"));
	    StringBuilder sb = new StringBuilder(hashed.length * 2 + 8);
	
	    for (int i = 0; i < hashed.length; i++) {
	        
	    	String hex = Integer.toHexString(0xFF & hashed[i]);
	        
	    	if (hex.length() == 1) {
	            // could use a for loop, but we're only dealing with a single byte
	            sb.append('0');
	        }
	        
	    	sb.append(hex);
	    }
	
	    return sb.toString();
	}
	
	public static List<CQSMessage> readMessageFromSuperCfResult(SuperCfResult<String, Composite, String> result) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		
		if (result == null) {
			return messageList;
		}
		
		SuperCfResult<String, Composite, String> curResult = result;
		
		while (curResult != null) {
			
			Composite superColumnName = curResult.getActiveSuperColumn();
			Map<String, String> messageMap = new HashMap<String, String>();
			
			for (String columnName : curResult.getColumnNames()) {
				messageMap.put(columnName, curResult.getString(superColumnName, columnName));
			}
			
			CQSMessage message = buildMessageFromMap(messageMap);
			message.setTimebasedId(superColumnName);
			messageList.add(message);
			
			if (curResult.hasNext()) {
				curResult = curResult.next();
			} else {
				break;
			}
		}
		
		return messageList;
	}
	
	public static CQSMessage buildMessageFromMap(Map<String, String> messageMap) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		if (messageMap == null || messageMap.size() == 0) {
			return null;
		}
		
		String body = "";
		Map<String, String> attributes = new HashMap<String, String>();
		CQSMessage message = new CQSMessage(body, attributes);
		
		for (String key : messageMap.keySet()) {
			
			if (key.equals("MessageId")) {
				message.setMessageId(messageMap.get(key));
				message.setReceiptHandle(messageMap.get(key));
			} else if (key.equals("MD5OfBody")) {
				message.setMD5OfBody(messageMap.get(key));
			} else if (key.equals("Body")) {
				message.setBody(messageMap.get(key));
			} else {
				message.getAttributes().put(key, messageMap.get(key));
			}
		}
		
		return message;
	}
	
	public static List<CQSMessage> readMessagesFromSuperColumns(int length,	Composite previousHandle, Composite nextHandle,	SuperSlice<Composite, String, String> superSlice, boolean ignoreFirstLastColumn) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		
		if (superSlice != null && superSlice.getSuperColumns() != null) {
			
			boolean noMatch = true;
			
			for (HSuperColumn<Composite, String, String> superColumn : superSlice.getSuperColumns()) {
				
				Composite columnName = superColumn.getName();
				
				if (ignoreFirstLastColumn && (previousHandle != null && columnName.compareTo(previousHandle) == 0) || (nextHandle != null && columnName.compareTo(nextHandle) == 0)) {
					noMatch = false;
					continue;
				} else if (superColumn.getColumns() == null	|| superColumn.getColumns().size() == 0) {
					continue;
				}
				
				CQSMessage message = extractMessageFromSuperColumn(superColumn);
				messageList.add(message);
			}
			
			if (noMatch && messageList.size() > length) {
				messageList.remove(messageList.size() - 1);
			}
		}
		
		return messageList;
	}
	
    /*
     * process Attribute.n requests start from 1 ordinal, ignore rest if there is a break
     */
    public static List<String> fillGetAttributesRequests(HttpServletRequest request) {
        
    	List<String> filterRequests = new ArrayList<String>();
        int index = 1;
        String attr = request.getParameter(CQSConstants.ATTRIBUTE_NAME + "." + index);
        
        while (attr != null) {
            filterRequests.add(attr);
            index++;
            attr = request.getParameter(CQSConstants.ATTRIBUTE_NAME + "." + index);
        }
        
        return filterRequests;
    }
    
	public static CQSMessage extractMessageFromSuperColumn(HSuperColumn<Composite, String, String> superColumn)	throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		Map<String, String> messageMap = new HashMap<String, String>();
		
		for (HColumn<String, String> column : superColumn.getColumns()) {
			messageMap.put(column.getName(), column.getValue());
		}
		
		Composite columnName = superColumn.getName();
		CQSMessage message = buildMessageFromMap(messageMap);
		message.setTimebasedId(columnName);
		return message;
	}
	
	public static long getQueueCount(String queueUrl) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		int numberOfPartitions = CMBProperties.getInstance().getCQSNumberOfQueuePartitions();
		CassandraPersistence persistence = new CassandraPersistence(CMBProperties.getInstance().getCQSKeyspace());
		String queueHash = Util.hashQueueUrl(queueUrl);
		long messageCount = 0;
		
		for (int i=0; i<numberOfPartitions; i++) {
			String queueKey = queueHash + "_" + i;
			long partitionCount = persistence.getCount("CQSPartitionedQueueMessages", queueKey, StringSerializer.get(), new CompositeSerializer(), HConsistencyLevel.QUORUM);
			messageCount += partitionCount;
		}
		
		return messageCount;
	}
}
