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

import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
import com.comcast.cqs.util.CQSConstants;

import java.util.List;

/**
 * API response generator for queues
 * @author baosen, aseem, bwolf
 *
 */
public class CQSQueuePopulator extends CQSPopulator {

    public static String getCreateQueueResponse(CQSQueue queue) {
    	
        String out = "<CreateQueueResponse>\n";
        out += "\t<CreateQueueResult>\n";
        out += "\t\t<QueueUrl>" + queue.getAbsoluteUrl() + "</QueueUrl>\n";
        out += "\t</CreateQueueResult>\n";
        out += "\t" + getResponseMetadata() + "\n";
        out += "</CreateQueueResponse>";
        
        return out;
    }
    
    public static String getDeleteQueueResponse() {
        String out = "<DeleteQueueResponse>\n";
        out +=  "\t" + getResponseMetadata() + "\n";
        out +=  "</DeleteQueueResponse>";
        return out;
    }
    
    public static String getAddPermissionResponse() {
        String out = "<AddPermissionResponse>\n";
        out +=  "\t" + getResponseMetadata() + "\n";
        out +=  "</AddPermissionResponse>";
        return out;
    }
    
    public static String getRemovePermissionResponse() {
        String out = "<RemovePermissionResponse>\n";
        out +=  "\t" + getResponseMetadata() + "\n";
        out += "</RemovePermissionResponse>";
        return out;
    }
    
    public static String getListQueuesResponse(List<CQSQueue> queues) {
        
    	StringBuffer out = new StringBuffer("<ListQueuesResponse>\n");

        for (CQSQueue queue : queues) {
        	
            out.append("\t<ListQueuesResult>\n");
        	out.append("\t\t<QueueUrl>").append(queue.getAbsoluteUrl()).append("</QueueUrl>\n");
            out.append("\t</ListQueuesResult>\n");
        }

        out.append("\t").append(getResponseMetadata()).append("\n");
        out.append("</ListQueuesResponse>\n");
        
        return out.toString();
    }
    
    public static String getQueueAttributesResponse(CQSQueue queue, List<String> filterAttributes) {
        
    	StringBuffer out = new StringBuffer("<GetQueueAttributesResponse>\n");
    	out.append("\t<GetQueueAttributesResult>\n");

        if (filterAttributes.contains("All")) {
            out.append("\t\t").append(fillAttribute(CQSConstants.VISIBILITY_TIMEOUT, "" + queue.getVisibilityTO())).append("\n");
            out.append("\t\t").append(fillAttribute(CQSConstants.MESSAGE_RETENTION_PERIOD, "" + queue.getMsgRetentionPeriod())).append("\n");
            out.append("\t\t").append(fillAttribute(CQSConstants.POLICY, "" + queue.getPolicy())).append("\n");
            out.append("\t\t").append(fillAttribute(CQSConstants.QUEUE_ARN, "" + queue.getArn())).append("\n");
            out.append("\t\t").append(fillAttribute(CQSConstants.MAXIMUM_MESSAGE_SIZE, "" + queue.getMaxMsgSize())).append("\n");
            out.append("\t\t").append(fillAttribute(CQSConstants.DELAY_SECONDS, "" + queue.getDelaySeconds())).append("\n");
            out.append("\t\t").append(fillAttribute(CQSConstants.APPROXIMATE_NUMBER_OF_MESSAGES, "" + RedisCachedCassandraPersistence.getInstance().getQueueMessageCount(queue.getRelativeUrl(), true))).append("\n");

        } else {
        
        	for (String attributeName : filterAttributes) {
                if (attributeName.equals(CQSConstants.VISIBILITY_TIMEOUT)) {
                    out.append("\t\t").append(fillAttribute(attributeName, "" + queue.getVisibilityTO())).append("\n");
                }
                if (attributeName.equals(CQSConstants.MESSAGE_RETENTION_PERIOD)) {
                    out.append("\t\t").append(fillAttribute(attributeName, "" + queue.getMsgRetentionPeriod())).append("\n");
                }
                if (attributeName.equals(CQSConstants.MAXIMUM_MESSAGE_SIZE)) {
                    out.append("\t\t").append(fillAttribute(attributeName, "" + queue.getMaxMsgSize())).append("\n");
                }
                if (attributeName.equals(CQSConstants.POLICY)) {
                    out.append("\t\t").append(fillAttribute(attributeName, "" + queue.getPolicy())).append("\n");
                }
                if (attributeName.equals(CQSConstants.QUEUE_ARN)) {
                    out.append("\t\t").append(fillAttribute(attributeName, "" + queue.getArn())).append("\n");
                }
                if (attributeName.equals(CQSConstants.DELAY_SECONDS)) {
                    out.append("\t\t").append(fillAttribute(attributeName, "" + queue.getDelaySeconds())).append("\n");
                }
                if (attributeName.equals(CQSConstants.APPROXIMATE_NUMBER_OF_MESSAGES)) {
                	out.append("\t\t").append(fillAttribute(attributeName, "" + RedisCachedCassandraPersistence.getInstance().getQueueMessageCount(queue.getRelativeUrl(), true))).append("\n");
                }
            }
        }
        
        out.append("\t</GetQueueAttributesResult>\n");
        out.append("\t").append(getResponseMetadata()).append("\n");
        out.append("</GetQueueAttributesResponse>").append("\n");
        
        return out.toString();
    }
    
    private static String fillAttribute(String name, String value) {
        return "<Attribute><Name>" + name + "</Name><Value>" + value + "</Value></Attribute>";
    }

    public static String setQueueAttributesResponse() {
        return "<SetQueueAttributesResponse>\n\t" + getResponseMetadata() + "\n</SetQueueAttributesResponse>\n";
    }
    
    public static String getQueueUrlResponse(CQSQueue queue) {

    	String out = "<GetQueueUrlResponse>\n";
        out += "\t<GetQueueUrlResult>\n";
        out += "\t\t<QueueUrl>" + queue.getAbsoluteUrl() + "</QueueUrl>\n";
        out += "\t</GetQueueUrlResult>\n";
        out += "\t" + getResponseMetadata() + "\n";
        out += "</GetQueueUrlResponse>\n";

        return out;
    }
}
