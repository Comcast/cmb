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
package com.comcast.plaxo.cqs.io;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;

import com.comcast.plaxo.cqs.model.CQSBatchResultErrorEntry;
import com.comcast.plaxo.cqs.model.CQSMessage;
import com.comcast.plaxo.cqs.util.CQSConstants;
/**
 * API response for messages
 * @author baosen, aseem, vvenkatraman, bwolf
 *
 */
public class CQSMessagePopulator extends CQSPopulator {

	public static String getSendMessageResponse(CQSMessage message) {
        StringBuffer out = new StringBuffer("<SendMessageResponse>\n").append("    <SendMessageResult>\n").append("       <MD5OfMessageBody>").append(message.getMD5OfBody())
        .append("</MD5OfMessageBody>\n").append("       <MessageId>").append(message.getMessageId()).append("</MessageId>\n") 
        .append("    </SendMessageResult>\n").append(getResponseMetadata()).append( "</SendMessageResponse>");

        return out.toString();
    }
    
    public static String getSendMessageBatchResponse(List<CQSMessage> messages, List<CQSBatchResultErrorEntry> errorList) {
        StringBuffer out = new StringBuffer("<SendMessageBatchResponse>\n" + "    <SendMessageBatchResult>\n");
        for (CQSMessage message: messages) {
            out.append("<SendMessageBatchResultEntry>\n<Id>").append(message.getSuppliedMessageId()).append("</Id>");
            out.append("\n<MessageId>").append(message.getMessageId()).append("</MessageId>"); 
            out.append("\n<MD5OfMessageBody>").append(message.getMD5OfBody()).append("</MD5OfMessageBody>\n</SendMessageBatchResultEntry>");
        }
        if (errorList.size() > 0) {
            out.append(getBatchErrorResult(errorList));
        }
        out.append("\n</SendMessageBatchResult>\n").append(getResponseMetadata()).append("\n</SendMessageBatchResponse>");

        return out.toString();
    }

    private static String getBatchErrorResult(List<CQSBatchResultErrorEntry> errorList) {
        StringBuffer out = new StringBuffer();
        for (CQSBatchResultErrorEntry error : errorList) {
            out.append("\n<BatchResultErrorEntry>\n<Id>").append(error.getId()).append("</Id>");
            out.append("\n<SenderFault>").append(error.getSenderFault()).append("</SenderFault>");
            out.append("\n<Code>").append(error.getCode()).append("</Code>");
            out.append("\n<Message>").append(error.getMessage()).append("</Message>\n</BatchResultErrorEntry>");
        }
        return out.toString();
    }
    
    public static String getDeleteMessageBatchResponse(List<String> ids, List<CQSBatchResultErrorEntry> errorList) {
        StringBuffer out = new StringBuffer("<DeleteMessageBatchResponse>\n<DeleteMessageBatchResult>");
        for (String id : ids) {
            out.append("\n<DeleteMessageBatchResultEntry>\n<Id>").append(id).append("</Id>\n</DeleteMessageBatchResultEntry>");            
        }
        if (errorList.size() > 0) {
            out.append(getBatchErrorResult(errorList));
        }
        out.append("\n</DeleteMessageBatchResult>");
        out.append("\n").append(getResponseMetadata()).append("\n</DeleteMessageBatchResponse>");
        return out.toString();
    }
    
    public static String getDeleteMessageResponse() {
        String out = "\n<DeleteMessageResponse>\n" + getResponseMetadata() + "\n</DeleteMessageResponse>";

        return out;
    }

    public static String getClearQueueResponse() {
        String out = "\n<ClearQueueResponse>\n" + getResponseMetadata() + "\n</ClearQueueResponse>";

        return out;
    }

    public static String getChangeMessageVisibilityResponse() {
        String out = "<ChangeMessageVisibilityResponse>" + getResponseMetadata() + "</ChangeMessageVisibilityResponse>";
        return out;
    }
    
    public static String getChangeMessageVisibilityBatchResponse(List<String> ids, List<CQSBatchResultErrorEntry> errorList) {
        StringBuffer out = new StringBuffer("<ChangeMessageVisibilityBatchResponse>\n<ChangeMessageVisibilityBatchResult>");
        for (String id : ids) {
            out.append("\n<ChangeMessageVisibilityBatchResultEntry>\n<Id>").append(id).append("</Id>\n</ChangeMessageVisibilityBatchResultEntry>");                        
        }
        if (errorList.size() > 0) {
            out.append(getBatchErrorResult(errorList));
        }
        out.append("\n</ChangeMessageVisibilityBatchResult>");
        out.append("\n").append(getResponseMetadata()).append("\n</ChangeMessageVisibilityBatchResponse>");

        return out.toString();
    }
    
    public static String getReceiveMessageResponseAfterSerializing(List<CQSMessage> messages, List<String> filterAttributs) {
        String out = "<ReceiveMessageResponse>\n" + "<ReceiveMessageResult>\n";

        for (CQSMessage message : messages) {
            out += serializeMessage(message, filterAttributs) + "\n";
        }

        out += "</ReceiveMessageResult>\n" + getResponseMetadata() + "</ReceiveMessageResponse>";
        
        return out;
    }

//    public static String getReceiveMessageResponse(List<String> serializedMessages) {
//        String out = "<ReceiveMessageResponse>\n" + "<ReceiveMessageResult>\n";
//
//        for (String serializedMessage : serializedMessages) {
//            out += serializedMessage + "\n";
//        }
//
//        out += "</ReceiveMessageResult>\n" + getResponseMetadata() + "</ReceiveMessageResponse>";
//        
//        return out;
//    }
    
    public static String serializeMessage(CQSMessage message, List<String> filterAttributes) {
        StringBuffer attributesXmlFragment = fillAttributesInReturn(message, filterAttributes);
        StringBuffer messageXml = new StringBuffer("<Message>"); 

        messageXml.append("<MessageId>").append(message.getMessageId()).append("</MessageId>");
        messageXml.append("<ReceiptHandle>").append(message.getReceiptHandle()).append("</ReceiptHandle>");
        messageXml.append("<MD5OfBody>").append(message.getMD5OfBody()).append("</MD5OfBody>");
        messageXml.append("<Body>").append(StringEscapeUtils.escapeXml(message.getBody())).append("</Body>");
        messageXml.append(attributesXmlFragment);
        messageXml.append("</Message>");
        
        return messageXml.toString();
    }
	
    private static StringBuffer fillAttributesInReturn(CQSMessage message, List<String> filterAttributes) {
        StringBuffer attributesXmlFragment = new StringBuffer("");

        if (message.getAttributes() != null) {
            for (Map.Entry<String, String> entry : message.getAttributes().entrySet()) {
                if (entry.getKey() != null && entry.getKey().length() > 0 && !entry.getKey().equals(CQSConstants.DELAY_SECONDS) && (filterAttributes.isEmpty() || filterAttributes.contains("All") || filterAttributes.contains(entry.getKey()))) {
                    attributesXmlFragment.append("<Attribute>").append("<Name>").append(entry.getKey()).append("</Name>").append("<Value>").append(entry.getValue()).append("</Value>").append("</Attribute>");
                }
            }
        }
        return attributesXmlFragment;
    }
}
