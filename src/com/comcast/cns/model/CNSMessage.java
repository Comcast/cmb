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
package com.comcast.cns.model;

import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;

/**
 * Value class for CNSMessage
 * Note: Class has all attributes volatile but no invariants.
 * @author aseem, bwolf
 * Class is thread-safe
 */
public final class CNSMessage {
	
    public enum CNSMessageStructure {
        json;
    }
    
    public enum CNSMessageType {
    	SubscriptionConfirmation, Notification, UnsubscribeConfirmation;
    }
    
    /**
     * The message to send to the topic. 
     * If you want to send the same message to all transport protocols, include the text of the message as a String value
     * If you want to send different messages for each transport protocol, 
     *  set the value of the MessageStructure parameter to json and use a JSON object for the Message parameter
     *  Parameter is required.
     */
    private volatile String message;
    
    /**
     * Set MessageStructure to json if you want to send a different message for each protocol.
     * Not required. 
     */
    private volatile CNSMessageStructure messageStructure;
    
    /**
     * Optional parameter to be used as the "Subject" line of when the message is delivered to e-mail endpoints. 
     * This field will also be included, if present, in the standard JSON messages delivered to other endpoints.
     * Not required. 
     */
    private volatile String subject;
    
    /**
     * The topic you want to publish to. Required.
     */
    private volatile String topicArn;
    
    /**
     * The user that intends to send this message.
     */
    private volatile String userId;
    
    /**
     * This is auto-generated and returned to publisher. Should be set once.
     */
    private volatile String messageId;
    
    /**
     * Time when the message was received.
     */
    private volatile Date timestamp;
    
    /**
     * 
     */
    private volatile String subArn;
    
    /**
     * Type of message, default value is Notification
     */
    private volatile CNSMessageType messageType = CNSMessageType.Notification;
    
	public void setSubscriptionArn(String subArn) {
		this.subArn = subArn;
	}
	
	public String getSubscriptionArn() {
		return subArn;
	}
	
    public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public CNSMessageStructure getMessageStructure() {
        return messageStructure;
    }

    public void setMessageStructure(CNSMessageStructure messageStructure) {
        this.messageStructure = messageStructure;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getTopicArn() {
        return topicArn;
    }

    public void setTopicArn(String topicArn) {
        this.topicArn = topicArn;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }    

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getMessageId() {
        return messageId;
    }
    
    /**
     * Method generates a UUID for message-id
     */
    public void generateMessageId() {
        messageId = UUID.randomUUID().toString();
    }
        
    @Override
    public String toString() {
        return "topic_arn=" + topicArn + (messageStructure == null ? "" : " message_structure=" + messageStructure.name()) + (subject == null ? "" : " subject=" + subject) + " message_length=" + message.length() + " user_id=" + userId + (messageId == null ? "" : " message_id=" + messageId); 
    }

    @Override
    public boolean equals(Object ob) {
    	
        if (!(ob instanceof CNSMessage)) {
            return false;
        }
        
        CNSMessage s = (CNSMessage)ob;        
        if (Util.isEqual(message, s.message) &&
                Util.isEqual(messageStructure, s.messageStructure) &&
                Util.isEqual(subject, s.subject) &&
                Util.isEqual(topicArn, s.topicArn) &&
                Util.isEqual(userId, s.userId) &&
                Util.isEqual(messageId, s.messageId)) {
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return messageId.hashCode();
    }
    
    /**
     * Check if the message object is valid according to the spec.
     * @throws CMBException if message constraints are not met.
     */
    public void checkIsValid() throws CMBException {
    	
        if (message == null) {
        	throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "Message is null");
        }
        
        if (topicArn == null) {
        	throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "TopicArn is null");
        }
        
        if (userId == null) {
        	throw new CMBException(CMBErrorCodes.InternalError, "Must set userId of CNSMessage");
        }
        
        if (messageId == null) {
        	throw new CMBException(CMBErrorCodes.InternalError, "Must set messageId of CNSMessage");
        }
        
        if (timestamp == null) {
        	throw new CMBException(CMBErrorCodes.InternalError, "Must set timestamp of CNSMessage");
        }

        if (!Util.isValidUnicode(message)) {
            throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "Message not UTF-8 characters");
        }
        
        if (message.getBytes().length > CMBProperties.getInstance().getCNSMaxMessageSize()) {
            throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "Message greater than " + CMBProperties.getInstance().getCNSMaxMessageSize() + " bytes");
        }
        
        if (messageStructure == CNSMessageStructure.json) {
        	
            JSONObject json = null;
            
            try {
                json = new JSONObject(message);
            } catch (JSONException e) {
                throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "messageStructure set to json but Message is not a valid JSON string:" + e.getMessage());
            }
            
            //validate json-keys as either 'default' or valid protocols.
            
            if (!json.has("default")) {
                throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "Must provide 'default' key in JSON Object");
            }
            
            for (Iterator<?> it = json.keys(); it.hasNext(); ) {
            	
                String key = (String) it.next();
                
                if (key.equals("default")) {
                	continue;
                }
                
                if (key.equals("email-json")) {
                	continue;
                }
                
                try {
                    CnsSubscriptionProtocol.valueOf(key);
                } catch (Exception e) {
                    throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "messageStructure keys must either be 'default' or a valid protocol name: " + e.getMessage());
                }
            }
        }        
        
        //subject validation
        
        if (subject != null) {
            if (subject.toCharArray().length >= 100) {
                throw new CMBException(CMBErrorCodes.InvalidQueryParameter, "Subject cannot be longer than 100 characters");
            }
        }
    }
    
    public String getProtocolSpecificMessage(CnsSubscriptionProtocol protocol) throws CMBException {
    	
        //Figure out what message to send
    	
        String msg;
        
        if (getMessageStructure() == null) {
        	
            msg = getMessage();
            
        } else {
        	
            try {
            	
                JSONObject msgObj = new JSONObject(getMessage());
                
                if (protocol == null) {
                    throw new CMBException(CMBErrorCodes.InternalError, "Subscription has no protocol");
                }
                
                if (protocol == CnsSubscriptionProtocol.email_json) { //special case
                	
                    if (msgObj.has("email-json")) {
                        msg = msgObj.getString("email-json");
                    } else {
                        msg = msgObj.getString("default");
                    }
                    
                } else {
                	
                    if (msgObj.has(protocol.name())) {
                        msg = msgObj.getString(protocol.name());
                    } else {
                        msg = msgObj.getString("default");
                    }
                }         
                
            } catch (JSONException e) {
                throw new CMBException(CMBErrorCodes.InternalError, "Could not parse JSON:" + e.getMessage());
            }
        }     
        
        return msg;
    }
    
    /*public String getProtocolSpecificMessage(CnsSubscriptionProtocol prot) throws CMBException {
		return CNSMessage.getProtocolSpecificMessage(prot, this);
    }*/

    /**
     * Method processes this message object by protocol so its easier and quicker to access
     * protocol specific message.
     * Assumed: The message is valid
     * Note: Must call this method before getting messages-per-protocol
     * @throws CMBException 
     */
    /*public void processMessageToProtocols() throws CMBException {    	
        for (CnsSubscriptionProtocol prot : CnsSubscriptionProtocol.values()) {
            if (prot != CnsSubscriptionProtocol.email) {
                protocolToProcessedMessage.put(prot, com.comcast.cns.util.Util.generateMessageJson(this, prot));
            } else {
                protocolToProcessedMessage.put(prot, getProtocolSpecificMessage(prot, this));
            }
        }
    }*/
    
    /*public String getProtocolSpecificProcessedMessage(CnsSubscriptionProtocol protocol) {
        return protocolToProcessedMessage.get(protocol);
    }*/   
    
    /**
     * 
     * @return a Unicode string representing the entire job
     * Format is <subject-non-null>\n[<subject>\n]messageStructure\ntopicArn\nuserId\messageId\nnmessage
     * since subject cannot have \n in it and messageStructure, topicArn don't have \n in it
     */
    public String serialize() {
        
    	if (message == null || topicArn == null || userId == null || messageId == null || timestamp == null) {
            throw new IllegalStateException("At least one of the expected fields is null");
        }
        
        StringBuffer sb = new StringBuffer();
        
        if (subject != null) {
            sb.append("1\n").append(subject).append("\n");
        } else {
            sb.append("0\n");
        }
        
        if (messageStructure != null) {
        	sb.append(messageStructure.ordinal()).append("\n");
        } else {
        	sb.append("*\n");
        }
        
        sb.append(topicArn).append("\n")
	        .append(timestamp.getTime()).append("\n")
	        .append(userId).append("\n")
	        .append(messageId).append("\n")
	        .append(messageType.toString()).append("\n")
	        .append(message);
	        
        
        return sb.toString();
    }
    /**
     * 
     * @param str String representing the CNSPublishJob instane
     * @return the instance
     */
    public static CNSMessage parseInstance(String str) {
        
    	String []arr = str.split("\n");
        
    	if (arr.length < 5) {
            throw new IllegalArgumentException("Bad format for serialized CNSPublishJob. Expected <subject-non-null>\n[<subject>\n]messageStructure\ntopicArn\npublisherUserId\nmessage Got:" + str);
        }
        
    	CNSMessage msg = new CNSMessage();
        int i = 0;
        String subjectNonNull = arr[i++];
        
        if (subjectNonNull.equals("1")) {
            msg.setSubject(arr[i++]);
        }
        
        String messageStructure = arr[i++];
        
        if (messageStructure.equals("*")) {
        	msg.setMessageStructure(null);
        } else {
            msg.setMessageStructure(CNSMessageStructure.values()[Integer.parseInt(messageStructure)]);
        }
        
        msg.setTopicArn(arr[i++]);
        msg.setTimestamp(new Date(Long.parseLong(arr[i++])));
        msg.setUserId(arr[i++]);
        msg.setMessageId(arr[i++]);
        msg.setMessageType(CNSMessageType.valueOf(arr[i++]));
        
        StringBuffer sb = new StringBuffer("");
        
        if (i < arr.length) {
        	
        	sb = new StringBuffer(arr[i++]);

        	for (; i < arr.length; i++) {
                sb.append("\n").append(arr[i]);
            }
            
        }
        
        msg.setMessage(sb.toString());
        
        return msg;
    }

	public CNSMessageType getMessageType() {
		return messageType;
	}

	public void setMessageType(CNSMessageType messageType) {
		this.messageType = messageType;
	}
}

