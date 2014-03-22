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
package com.comcast.cns.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.json.JSONWriter;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSMessage.CNSMessageType;
import com.comcast.cns.model.CNSRetryPolicy.CnsBackoffFunction;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;

/**
 * Utility functions for cns
 * @author aseem, bwolf, jorge
 *
 */
public class Util {
	
	public static final int CNS_USER_TOPIC_LIMIT = 100;
	
	// redisPubSubRegex matches redis://[password@]hostname:port/channelname
	public static final String redisPubSubRegex = "redis://(([^@]+)@)?([a-z0-9.]+):([0-9]+)/(\\S+)";
	public static final Pattern redisPubSubPattern = Pattern.compile(redisPubSubRegex);
	
	public static String generateCnsTopicArn(String topicName, String region, String userId) {
		return "arn:cmb:cns:" + region + ":" + userId + ":" + topicName;
	}
	
	public static String generateCnsTopicSubscriptionArn(String topicArn, CnsSubscriptionProtocol protocol, String endpoint) throws NoSuchAlgorithmException {
		MessageDigest m = MessageDigest.getInstance("MD5");
		String name = protocol + ":" + endpoint;
		byte bytes[] = m.digest(name.getBytes());
		return topicArn + ":" + UUID.nameUUIDFromBytes(bytes).toString();
	}
	
	public static String getNameFromTopicArn(String topicArn) {
		
	    if (topicArn == null) {
	    	return null;
	    }
		
		String []elem = topicArn.split(":");

		if (elem.length == 6) {
			return elem[5];
		} else {
			return null;
		}
	}
	
	/**
	 * 
	 * @param subArn Of the form <topic-arn>:UUID
	 * @return <topic-arn>
	 */
	public static String getCnsTopicArn(String subArn) {
		
	    String []arr = subArn.split(":");
	    
	    if (arr.length < 2) {
	        throw new IllegalArgumentException("Bad format for subscription. Expected <topic-arn>:UUID Got:" + subArn);
	    }
	    
        StringBuffer sb = new StringBuffer(arr[0]);
        
        for (int i = 1; i < arr.length - 1; i++) {
            sb.append(":").append(arr[i]);
        }
        
        return sb.toString();
	}
	
	private static final Pattern topicPattern = Pattern.compile("arn:cmb:cns:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+"); 
	
	public static boolean isValidTopicArn(String arn) {

		if (arn == null) {
			return false;
		}
		
		Matcher matcher = topicPattern.matcher(arn);
		
		return matcher.matches();
	}

	public static String getUserIdFromTopicArn(String arn) {
		
		if (!isValidTopicArn(arn)) {
			return null;
		}
		
		return arn.split(":")[4];
	}
	
	private static final Pattern subPattern = Pattern.compile("arn:cmb:cns:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+:[A-Za-z0-9-_]+"); 
	
	public static boolean isValidSubscriptionArn(String arn) {
		Matcher matcher = subPattern.matcher(arn);		
		return matcher.matches();
	}
	
	private static final Pattern topicNamePattern = Pattern.compile("[A-Za-z0-9-_]+"); 
	
	public static boolean isValidTopicName(String name) {
		
		if (name == null || name.equals("") || name.contains(" ") || name.length() > 256) {
			return false;
		}
		
		Matcher matcher = topicNamePattern.matcher(name);		
		return matcher.matches();
	}
	
	/**
     * Generate the confirmation Json string
     * @param arn The top arn for the topic the user is subscribing to.
     * @param token the token for confirming the subscription
     * @return the Json String 
     */
    public static String generateConfirmationJson(String topicArn, String token, String messageId) {

    	ByteArrayOutputStream out = new ByteArrayOutputStream();
		Writer writer = new PrintWriter(out); 
    	JSONWriter jw = new JSONWriter(writer);
    	String cnsServiceLocation = CMBProperties.getInstance().getCNSServiceUrl();
    	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); // UTC, no offset
        Date now = new Date();

    	try {
	    	jw = jw.object();
	    	jw.key("Type").value(CNSMessageType.SubscriptionConfirmation);
	    	jw.key("MessageId").value(messageId);
	    	jw.key("Token").value(token);
	    	jw.key("TopicArn").value(topicArn);
	    	jw.key("Message").value("You have chosen to subscribe to the topic "+topicArn+"\\nTo confirm the subscription, visit the SubscribeURL included in this message.");
	    	jw.key("SubscribeURL").value(cnsServiceLocation+"?Action=ConfirmSubscription&TopicArn="+topicArn+"&Token="+token);
	    	jw.key("Timestamp").value(df.format(now));
	    	jw.key("SignatureVersion").value("1");
	    	jw.key("Signature").value("");
	    	jw.key("SigningCertURL").value("");
	    	jw.endObject();	    	    	
	    	writer.flush();
	    		    	
    	} catch (Exception e) {
    		return "";
    	}   	
    	
    	return out.toString();
    }	
    
    
    /**
     * Generate the Json message to send to all the endpoints except email
     * @param arn The topic arn for the topic the user is subscribing to.
     * @param message the message to send
     * @param subject the subject to send, also included as the subject in email-json
     * @return the Json String 
     */
    public static String generateMessageJson(CNSMessage cnsMessage, CnsSubscriptionProtocol prot) {

    	ByteArrayOutputStream out = new ByteArrayOutputStream();
		Writer writer = new PrintWriter(out); 
    	JSONWriter jw = new JSONWriter(writer);
    	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); //Time is in UTC zone. i,e no offset
        String timestamp = df.format(cnsMessage.getTimestamp());
        
    	try {
    		String message = cnsMessage.getProtocolSpecificMessage(prot);
	    	jw = jw.object();
	    	jw.key("Message").value(message);
	    	jw.key("MessageId").value(cnsMessage.getMessageId());
	    	jw.key("Signature").value("");
	    	jw.key("SignatureVersion").value("1");
	    	jw.key("SigningCertURL").value("");
	    	jw.key("Subject").value(cnsMessage.getSubject());
	    	jw.key("Timestamp").value(timestamp);
	    	jw.key("TopicArn").value(cnsMessage.getTopicArn());
	    	jw.key("Type").value(cnsMessage.getMessageType().toString());
	    	String unsubscribeUrl = CMBProperties.getInstance().getCmbUnsubscribeUrl();
	    	if (unsubscribeUrl.contains("%a")) {
	    		unsubscribeUrl = unsubscribeUrl.replace("%a", cnsMessage.getSubscriptionArn());
	    	} else {
	    		unsubscribeUrl += "?Action=Unsubscribe&SubscriptionArn=" + cnsMessage.getSubscriptionArn();
	    	}
	    	jw.key("UnSubscribeURL").value(unsubscribeUrl);
	    	jw.endObject();	    	    	
	    	writer.flush();
    	} catch (Exception e) {
    		return "";
    	} 
    	
    	return out.toString();
    }	
	
	public static boolean isPhoneNumber(String phone) {
		
	    	int size = phone.length();
	    	
	    	for (int i=0; i<size; i++) {
	    		
	    		Character c = phone.charAt(i);
	    		
	    		if ((c == '-') || (c == '+') || (c == '.') || (c == '(') || (c == ')')) {
	    			//skip
	    		} else if((c.compareTo('0') >= 0) && (c.compareTo('9') <= 0)) {
	    			//skip
	    		} else {
	    			return false;
	    		}
	    	}

	    	return true;
	    }

	/**
	 * 
	 * @param i The number of retry. Must start with 1
	 * @param maxBackOffRetries The total number of retries allowed
	 * @param minDelayTarget the minimum retry delay in sec
	 * @param maxDelayTarget the max retry delay in sec
	 * @param backOffFunction which backoff function to return
	 * @return the delay in seconds
	 */
    public static int getNextRetryDelay(int i, int maxBackOffRetries, int minDelayTarget, int maxDelayTarget, CnsBackoffFunction backOffFunction) {
        if (maxBackOffRetries == 0) {
            throw new IllegalArgumentException("maxBackOffRetries cannot be 0");
        }
        double x;
        double a;
        switch (backOffFunction) {
        case linear:
            //equation f(i) = slope*(i-1) + minDelayTarget
            //calculate slope given f(maxBackOffRetries) = maxDelayTarget = slope(maxBackOffRetries - 1) + minDelayTarget
            //=> slope = (maxDelayTarget - minDelayTarget)/ (maxBackOffRetries - 1)
            double slope = (double)(maxDelayTarget - minDelayTarget) / (double)(maxBackOffRetries - 1);
            return (int) (slope*(i-1) + minDelayTarget);
            
        case geometric:
            //figure out x using equation: x^(maxBackOffRetries - 1) + minDelayTarget - 1 = maxDelayTarget
            //=> x^(maxBackOffRetries - 1) = maxDelayTarget - minDelayTarget + 1
            //=> x = pow(maxDelayTarget - minDelayTarget + 1, 1/(maxBackOffRetries - 1))
            // and f(i) = x^(i-1) + minDelayTarget - 1
            
            x = Math.pow(maxDelayTarget - minDelayTarget + 1, 1d/(double)(maxBackOffRetries - 1));
            return (int)Math.pow(x, (i-1)) + minDelayTarget - 1;
            
        case exponential:
            //equation to use ae^(i-1) + b - a = y. where b = minDelayTarget
            //=> ae^(maxBackOffRetries -1) + minDelayTarget - a = maxDelayTarget
            //=>a(e^(maxBackOffRetries -1) -1) = maxDelayTarget - minDelayTarget
            //=> a = (maxDelayTarget - minDelayTarget) / (e^(maxBackOffRetries -1) -1)
            a = (maxDelayTarget - minDelayTarget) / (Math.pow(Math.E, maxBackOffRetries -1) - 1);
            return (int) ((a * Math.pow(Math.E, i - 1)) + minDelayTarget - a);
            
        case arithmetic:
            //arithmetic is pretty much quadratic for us given euation: ax^2 + b = y
            //f(i) = a(i-1)^2 + b
            //figure out a using b = minDelayTarget & a(maxBackOffRetries-1)^2 + minDelayTarget = maxDelayTarget
            //=> a = (maxDelayTarget-minDelayTarget)/ (maxBackOffRetries-1)^2
            a = (maxDelayTarget - minDelayTarget) / Math.pow(maxBackOffRetries - 1, 2);
            return (int) (a * Math.pow((i - 1), 2) + minDelayTarget);
            
        default:
            throw new IllegalArgumentException("Unknown backoff" + backOffFunction);
        }
    }
}
