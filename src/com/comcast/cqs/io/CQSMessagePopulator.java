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
package com.comcast.cqs.io;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;

import com.comcast.cqs.model.CQSBatchResultErrorEntry;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.util.CQSConstants;
/**
 * API response for messages
 * @author baosen, aseem, vvenkatraman, bwolf
 *
 */
public class CQSMessagePopulator extends CQSPopulator {

	public static String getSendMessageResponse(CQSMessage message) {
		
        StringBuffer out = new StringBuffer("<SendMessageResponse>\n");
        out.append("\t<SendMessageResult>\n");
        out.append("\t\t<MD5OfMessageBody>").append(message.getMD5OfBody()).append("</MD5OfMessageBody>\n");
        if (message.getMD5OfMessageAttributes() != null) {
        	out.append("\t\t<MD5OfMessageAttributes>").append(message.getMD5OfMessageAttributes()).append("</MD5OfMessageAttributes>\n");
        }
        out.append("\t\t<MessageId>").append(message.getMessageId()).append("</MessageId>\n"); 
        out.append("\t</SendMessageResult>\n");
        out.append("\t").append(getResponseMetadata()).append("\n");
        out.append( "</SendMessageResponse>\n");

        return out.toString();
    }
    
    public static String getSendMessageBatchResponse(List<CQSMessage> messages, List<CQSBatchResultErrorEntry> errorList) {
    	
        StringBuffer out = new StringBuffer("<SendMessageBatchResponse>\n");
        out.append("\t<SendMessageBatchResult>\n");
        
        for (CQSMessage message: messages) {
            out.append("\t\t<SendMessageBatchResultEntry>\n");
            out.append("\t\t\t<Id>").append(message.getSuppliedMessageId()).append("</Id>\n");
            out.append("\t\t\t<MessageId>").append(message.getMessageId()).append("</MessageId>\n"); 
            out.append("\t\t\t<MD5OfMessageBody>").append(message.getMD5OfBody()).append("</MD5OfMessageBody>\n");
            if (message.getMD5OfMessageAttributes() != null) {
            	out.append("\t\t<MD5OfMessageAttributes>").append(message.getMD5OfMessageAttributes()).append("</MD5OfMessageAttributes>\n");
            }
            out.append("\t\t</SendMessageBatchResultEntry>\n");
        }
        
        if (errorList.size() > 0) {
            out.append(getBatchErrorResult(errorList));
        }
        
        out.append("\t</SendMessageBatchResult>\n");
        out.append("\t").append(getResponseMetadata()).append("\n");
        out.append("</SendMessageBatchResponse>\n");

        return out.toString();
    }

    private static String getBatchErrorResult(List<CQSBatchResultErrorEntry> errorList) {
        StringBuffer out = new StringBuffer();
        for (CQSBatchResultErrorEntry error : errorList) {
            out.append("\t\t<BatchResultErrorEntry>\n");
            out.append("\t\t\t<Id>").append(error.getId()).append("</Id>\n");
            out.append("\t\t\t<SenderFault>").append(error.getSenderFault()).append("</SenderFault>\n");
            out.append("\t\t\t<Code>").append(error.getCode()).append("</Code>\n");
            out.append("\t\t\t<Message>").append(error.getMessage()).append("</Message>\n");
            out.append("\t\t</BatchResultErrorEntry>\n");
        }
        return out.toString();
    }
    
    public static String getDeleteMessageBatchResponse(List<String> ids, List<CQSBatchResultErrorEntry> errorList) {
        
    	StringBuffer out = new StringBuffer("<DeleteMessageBatchResponse>\n");
        out.append("\t<DeleteMessageBatchResult>\n");
        
        for (String id : ids) {
            out.append("\t\t<DeleteMessageBatchResultEntry>\n");
            out.append("\t\t\t<Id>").append(id).append("</Id>\n");
            out.append("\t\t</DeleteMessageBatchResultEntry>\n");            
        }
        
        if (errorList.size() > 0) {
            out.append(getBatchErrorResult(errorList));
        }
        
        out.append("\t</DeleteMessageBatchResult>\n");
        out.append("\t").append(getResponseMetadata()).append("\n");
        out.append("</DeleteMessageBatchResponse>\n");
        
        return out.toString();
    }
    
    public static String getDeleteMessageResponse() {
        String out = "<DeleteMessageResponse>\n\t" + getResponseMetadata() + "\n</DeleteMessageResponse>\n";
        return out;
    }

    public static String getPurgeQueueResponse() {
        String out = "<PurgeQueueResponse>\n\t" + getResponseMetadata() + "\n</PurgeQueueResponse>\n";
        return out;
    }

    public static String getChangeMessageVisibilityResponse() {
        String out = "<ChangeMessageVisibilityResponse>\n\t" + getResponseMetadata() + "</ChangeMessageVisibilityResponse>\n";
        return out;
    }
    
    public static String getChangeMessageVisibilityBatchResponse(List<String> ids, List<CQSBatchResultErrorEntry> errorList) {
        
    	StringBuffer out = new StringBuffer("<ChangeMessageVisibilityBatchResponse>\n");
        out.append("\t<ChangeMessageVisibilityBatchResult>\n");
        
        for (String id : ids) {
            out.append("\t\t<ChangeMessageVisibilityBatchResultEntry>\n");
            out.append("\t\t\t<Id>").append(id).append("</Id>\n");
            out.append("\t\t</ChangeMessageVisibilityBatchResultEntry>\n");                        
        }
        
        if (errorList.size() > 0) {
            out.append(getBatchErrorResult(errorList));
        }
        
        out.append("\t</ChangeMessageVisibilityBatchResult>\n");
        out.append("\t").append(getResponseMetadata()).append("\n");
        out.append("</ChangeMessageVisibilityBatchResponse>\n");

        return out.toString();
    }
    
    public static String getReceiveMessageResponseAfterSerializing(List<CQSMessage> messages, List<String> filterAttributs, List<String> filterMessageAttributes) {
        
    	StringBuffer out = new StringBuffer("<ReceiveMessageResponse>\n");
    	out.append("\t<ReceiveMessageResult>\n");

        for (CQSMessage message : messages) {
            out.append(serializeMessage(message, filterAttributs, filterMessageAttributes));
        }

        out.append("\t</ReceiveMessageResult>\n");
        out.append("\t").append(getResponseMetadata()).append("\n");
        out.append("</ReceiveMessageResponse>\n");
        
        return out.toString();
    }

    public static String serializeMessage(CQSMessage message, List<String> filterAttributes, List<String> filterMessageAttributes) {

    	StringBuffer attributesXmlFragment = fillAttributesInReturn(message, filterAttributes);
        StringBuffer messageXml = new StringBuffer("\t\t<Message>\n"); 

        messageXml.append("\t\t\t<MessageId>").append(message.getMessageId()).append("</MessageId>\n");
        messageXml.append("\t\t\t<ReceiptHandle>").append(message.getReceiptHandle()).append("</ReceiptHandle>\n");
        messageXml.append("\t\t\t<MD5OfBody>").append(message.getMD5OfBody()).append("</MD5OfBody>\n");
        messageXml.append("\t\t\t<Body>").append(StringEscapeUtils.escapeXml(message.getBody()).replaceAll("\r", "&#xD;")).append("</Body>\n");
        messageXml.append(attributesXmlFragment);
        
        if (message.getMessageAttributes() != null && message.getMessageAttributes().size() > 0) {
        	for (String key : message.getMessageAttributes().keySet()) {
                if (filterMessageAttributes.contains("All") || filterMessageAttributes.contains("all") || filterMessageAttributes.contains(key)) {
                	String type = message.getMessageAttributes().get(key).getDataType();
                	String stringValue = message.getMessageAttributes().get(key).getStringValue();
		        	messageXml.append("\t\t\t<MessageAttribute>\n");
		            messageXml.append("\t\t\t\t<Name>").append(key).append("</Name>\n");
		            messageXml.append("\t\t\t\t<Value>\n");
		            messageXml.append("\t\t\t\t\t<DataType>").append(type).append("</DataType>\n");
		            if (!type.equals("Binary")) {
		            	messageXml.append("\t\t\t\t\t<StringValue>").append(stringValue).append("</StringValue>\n");
		            } else {
		            	messageXml.append("\t\t\t\t\t<BinaryValue>").append(stringValue).append("</BinaryValue>\n");
		            }
		            messageXml.append("\t\t\t\t</Value>\n");
		        	messageXml.append("\t\t\t</MessageAttribute>\n");
                }
        	}
        	messageXml.append("\t\t\t<MD5OfMessageAttributes>").append(message.getMD5OfMessageAttributes()).append("</MD5OfMessageAttributes>\n");
        }
        
        messageXml.append("\t\t</Message>\n");
        
        return messageXml.toString();
    }
	
    private static StringBuffer fillAttributesInReturn(CQSMessage message, List<String> filterAttributes) {
    	
        StringBuffer attributesXmlFragment = new StringBuffer("");

        if (message.getAttributes() != null) {
            for (Map.Entry<String, String> entry : message.getAttributes().entrySet()) {
                if (entry.getKey() != null && entry.getKey().length() > 0 && !entry.getKey().equals(CQSConstants.DELAY_SECONDS) && (filterAttributes.isEmpty() || filterAttributes.contains("All") || filterAttributes.contains(entry.getKey()))) {
                    attributesXmlFragment.append("\t\t\t<Attribute>").append("<Name>").append(entry.getKey()).append("</Name>").append("<Value>").append(entry.getValue()).append("</Value>").append("</Attribute>\n");
                }
            }
        }
        
        return attributesXmlFragment;
    }
}
