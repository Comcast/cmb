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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.comcast.cmb.common.persistence.AbstractCassandraPersistence;
import com.comcast.cmb.common.persistence.CassandraPersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.controller.CQSCache;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
/**
 * Utility functions for cqs
 * @author bwolf, vvenkatraman, baosen
 *
 */
public class Util {
	
	private static Logger logger = Logger.getLogger(Util.class);
	
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
	
	public static String getAbsoluteQueueUrlForRelativeUrl(String relativeUrl) {
		
		if (relativeUrl == null) {
			return null;
		}

		String url = CMBProperties.getInstance().getCQSServiceUrl();
		
		if (!url.endsWith("/")) {
			url += "/";
		}

		url += relativeUrl;
		
		return url;
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

	public static String getAbsoluteAWSQueueUrlForArn(String arn) {
		
		if (arn == null) {
			return null;
		}
		
		String elements[] = arn.split(":");
		
		if (elements.length != 6) {
			return null;
		}
		
		String url = "http://" + elements[2] + "." + elements[3] + ".amazonaws.com";
		
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
	
	public static String getLocalAbsoluteQueueUrlForRelativeUrl(String relativeUrl) {
		
		
		String url = CMBProperties.getInstance().getCQSServiceUrl();
		
		if (!url.endsWith("/")) {
			url += "/";
		}
		
		url += relativeUrl;
		
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
		
		String relativeUrl = elements[3] + "/" + elements[4];
		
		if (relativeUrl.contains("?")) {
			relativeUrl = relativeUrl.substring(0, relativeUrl.indexOf("?")-1);
		}
		
		return relativeUrl;
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

	public static String getUserIdForQueueArn(String queueArn) {
		
		if (queueArn == null) {
			return null;
		}
		
		String elements[] = queueArn.split(":");
		
		if (elements.length != 6) {
			return null;
		}
		
		return elements[4];
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
	
	public static String getQueueUrlHashFromCache(String queueUrl){
		String queueUrlHash = null;
		try {
			CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
			if(queue != null){
				queueUrlHash = CQSCache.getCachedQueue(queueUrl)
					.getRelativeUrlHash();
			}
			if (queueUrlHash == null) {
				queueUrlHash = hashQueueUrl(queueUrl);
			}
		} catch (Exception ex) {
			logger.error("event=failed_to_queueRelativeUrlHash", ex);
		}
		return queueUrlHash;
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
	
	public static List<CQSMessage> readMessagesFromSuperColumns(String queueUrl, int length, Composite previousHandle, Composite nextHandle, SuperSlice<Composite, String, String> superSlice, boolean ignoreFirstLastColumn) throws PersistenceException, NoSuchAlgorithmException, IOException  {
		
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
				
				CQSMessage message = extractMessageFromSuperColumn(queueUrl, superColumn);
				
				messageList.add(message);
			}
			
			if (noMatch && messageList.size() > length) {
				messageList.remove(messageList.size() - 1);
			}
		}
		
		return messageList;
	}
	
    public static List<String> fillGetAttributesRequests(HttpServletRequest request) {
        
        // process Attribute.n requests start from 1 ordinal, ignore rest if there is a break

        List<String> attributeNames = new ArrayList<String>();
        String attr = null;
        
        if (request.getParameter(CQSConstants.ATTRIBUTE_NAME) != null) {
        	attr = request.getParameter(CQSConstants.ATTRIBUTE_NAME);
        	attributeNames.add(attr);
        }
        
        int index = 1;
        attr = request.getParameter(CQSConstants.ATTRIBUTE_NAME + "." + index);
        
        while (attr != null) {
        	attributeNames.add(attr);
            index++;
            attr = request.getParameter(CQSConstants.ATTRIBUTE_NAME + "." + index);
        }
        
        return attributeNames;
    }
    
	public static CQSMessage extractMessageFromSuperColumn(String queueUrl, HSuperColumn<Composite, String, String> superColumn) throws NoSuchAlgorithmException, PersistenceException, IOException {
		
		Map<String, String> messageMap = new HashMap<String, String>();
		
		CQSQueue queue = null;
		
		try {
			queue = CQSCache.getCachedQueue(queueUrl);
		} catch (Exception ex) {
			throw new PersistenceException(ex);
		}
		
		if (queue == null) {
			throw new PersistenceException(CMBErrorCodes.InternalError, "Unknown queue " + queueUrl);
		}
		
		for (HColumn<String, String> column : superColumn.getColumns()) {
			messageMap.put(column.getName(), column.getValue());
		}
		
		Composite columnName = superColumn.getName();
		CQSMessage message = buildMessageFromMap(messageMap);
		
		if (queue.isCompressed()) {
			message.setBody(Util.decompress(message.getBody()));
		}
		
		message.setTimebasedId(columnName);
		return message;
	}
	
	public static long getQueueMessageCount(CQSQueue queue) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		int numberOfPartitions = queue.getNumberOfPartitions();
		AbstractCassandraPersistence cassandraHandler = CassandraPersistenceFactory.getInstance(CMBProperties.getInstance().getCQSKeyspace());
		String queueHash = Util.hashQueueUrl(queue.getRelativeUrl());
		long messageCount = 0;
		
		for (int i=0; i<numberOfPartitions; i++) {
			String queueKey = queueHash + "_" + i;
			long partitionCount = cassandraHandler.getCount("CQSPartitionedQueueMessages", queueKey, StringSerializer.get(), new CompositeSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
			messageCount += partitionCount;
		}
		
		return messageCount;
	}

	public static List<Long> getPartitionMessageCounts(CQSQueue queue) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		int numberOfPartitions = queue.getNumberOfPartitions();
		AbstractCassandraPersistence cassandraHandler = CassandraPersistenceFactory.getInstance(CMBProperties.getInstance().getCQSKeyspace());
		String queueHash = Util.hashQueueUrl(queue.getRelativeUrl());
		List<Long> messageCounts = new ArrayList<Long>();
		
		for (int i=0; i<numberOfPartitions; i++) {
			String queueKey = queueHash + "_" + i;
			long partitionCount = cassandraHandler.getCount("CQSPartitionedQueueMessages", queueKey, StringSerializer.get(), new CompositeSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
			messageCounts.add(partitionCount);
		}
		
		return messageCounts;
	}
	
    public static int getShardFromReceiptHandle(String receiptHandle) throws PersistenceException {

    	String handleParts[] = receiptHandle.split(":");
    	
    	if (handleParts.length < 3) {
    		throw new PersistenceException(CMBErrorCodes.InternalError, "Invalid receipt handle " + receiptHandle);
    	}
    	
    	String keyParts[] = handleParts[2].split("_");

    	if (keyParts.length < 3) {
    		//throw new PersistenceException(CMBErrorCodes.InternalError, "Invalid receipt handle " + receiptHandle);
    		logger.warn("event=missing_shard_info receipt_handle=" + receiptHandle + " action=default_to_zero");
    		return 0;
    	}
    	
    	return Integer.parseInt(keyParts[1]);
    }
    
    public static String compress(String decompressed) throws IOException{
        if (decompressed == null || decompressed.equals("")) {
            return decompressed;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(decompressed.length());
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(decompressed.getBytes());
        gzip.close();
        out.close();
        String compressed = Base64.encodeBase64String(out.toByteArray());
        logger.debug("event=compressed from=" + decompressed.length() + " to=" + compressed.length());
		return compressed;
     }
    
    public static String decompress(String compressed) throws IOException {
        if (compressed == null || compressed.equals("")) {
            return compressed;
        }
        Reader reader = null;
        StringWriter writer = null;
        try {
        	if (!compressed.startsWith("H4sIA")) {
        		String prefix = compressed;
        		if (compressed.length() > 100) {
        			prefix = prefix.substring(0, 99);
        		}
        		logger.warn("event=content_does_not_appear_to_be_zipped message=" + prefix);
        		return compressed;
        	}
	        byte[] unencodedEncrypted = Base64.decodeBase64(compressed);
	        ByteArrayInputStream in = new ByteArrayInputStream(unencodedEncrypted);
	        GZIPInputStream gzip = new GZIPInputStream(in);
	        reader = new InputStreamReader(gzip, "UTF-8");
	        writer = new StringWriter();
	        char[] buffer = new char[10240];
	        for (int length = 0; (length = reader.read(buffer)) > 0;) {
	            writer.write(buffer, 0, length);
	        }
        } finally {
        	if (writer != null) {
        		writer.close();
        	}
        	if (reader != null) {
        		reader.close();
        	}
        }
        String decompressed = writer.toString();
        logger.info("event=decompressed from=" + compressed.length() + " to=" + decompressed.length());
        return decompressed;
    }
}
