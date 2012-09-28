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
    	
        String out = "<CreateQueueResponse>\n" +
                "    <CreateQueueResult>\n";
        if (queue.getRelativeUrl().startsWith("http")) {
        	//legacy queue
        	out += "        <QueueUrl>" + queue.getRelativeUrl() + "</QueueUrl>\n";
        } else {
        	out += "        <QueueUrl>" + queue.getAbsoluteUrl() + "</QueueUrl>\n";
        }
        
        out += "    </CreateQueueResult>\n" +
        getResponseMetadata() +
        "</CreateQueueResponse>";
        
        return out;
    }
    
    public static String getDeleteQueueResponse() {
        String out = "<DeleteQueueResponse>\n" +
                getResponseMetadata() +
                "</DeleteQueueResponse>";
        return out;
    }
    
    public static String getAddPermissionResponse() {
        String out = "<AddPermissionResponse>\n" +
                getResponseMetadata() +
                "</AddPermissionResponse>";
        return out;
    }
    
    public static String getRemovePermissionResponse() {
        String out = "<RemovePermissionResponse>\n" +
                getResponseMetadata() +
                "</RemovePermissionResponse>";
        return out;
    }
    
    public static String getListQueuesResponse(List<CQSQueue> queues) {
        
    	String out = "<ListQueuesResponse>\n";

        for (CQSQueue queue : queues) {
        	
            out += "    <ListQueuesResult>\n";
        
            if (queue.getRelativeUrl().startsWith("http")) {
            	//legacy queue
            	out += "        <QueueUrl>" + queue.getRelativeUrl() + "</QueueUrl>\n";
            } else {
            	out += "        <QueueUrl>" + queue.getAbsoluteUrl() + "</QueueUrl>\n";
            }

            out += "    </ListQueuesResult>\n";
        }

        out += getResponseMetadata() + "</ListQueuesResponse>";
        
        return out;
    }
    
    public static String getQueueAttributesResponse(CQSQueue queue, List<String> filterAttributes) {
        
    	StringBuffer out = new StringBuffer("<GetQueueAttributesResponse>\n  <GetQueueAttributesResult>\n");

        if (filterAttributes.contains("All")) {
            out.append(fillAttribute(CQSConstants.VISIBILITY_TIMEOUT, "" + queue.getVisibilityTO()));
            out.append(fillAttribute(CQSConstants.MESSAGE_RETENTION_PERIOD, "" + queue.getMsgRetentionPeriod()));
            out.append(fillAttribute(CQSConstants.POLICY, "" + queue.getPolicy()));
            out.append(fillAttribute(CQSConstants.QUEUE_ARN, "" + queue.getArn()));
            out.append(fillAttribute(CQSConstants.MAXIMUM_MESSAGE_SIZE, "" + queue.getMaxMsgSize()));
            out.append(fillAttribute(CQSConstants.DELAY_SECONDS, "" + queue.getDelaySeconds()));
            RedisCachedCassandraPersistence redisP = RedisCachedCassandraPersistence.getInstance();
            out.append(fillAttribute(CQSConstants.APPROXIMATE_NUMBER_OF_MESSAGES, "" + redisP.getQueueCount(queue.getRelativeUrl(), true)));

        } else {
        
        	for (String attributeName : filterAttributes) {
                if (attributeName.equals(CQSConstants.VISIBILITY_TIMEOUT)) {
                    out.append(fillAttribute(attributeName, "" + queue.getVisibilityTO()));
                }
                if (attributeName.equals(CQSConstants.MESSAGE_RETENTION_PERIOD)) {
                    out.append(fillAttribute(attributeName, "" + queue.getMsgRetentionPeriod()));
                }
                if (attributeName.equals(CQSConstants.MAXIMUM_MESSAGE_SIZE)) {
                    out.append(fillAttribute(attributeName, "" + queue.getMaxMsgSize()));
                }
                if (attributeName.equals(CQSConstants.POLICY)) {
                    out.append(fillAttribute(attributeName, "" + queue.getPolicy()));
                }
                if (attributeName.equals(CQSConstants.QUEUE_ARN)) {
                    out.append(fillAttribute(attributeName, "" + queue.getArn()));
                }
                if (attributeName.equals(CQSConstants.DELAY_SECONDS)) {
                    out.append(fillAttribute(attributeName, "" + queue.getDelaySeconds()));
                }
                if (attributeName.equals(CQSConstants.APPROXIMATE_NUMBER_OF_MESSAGES)) {
                	//todo: what if redis is not enabled?
                	out.append(fillAttribute(attributeName, "" + RedisCachedCassandraPersistence.getInstance().getQueueCount(queue.getRelativeUrl(), true)));
                }
            }
        }
        
        out.append("</GetQueueAttributesResult>\n");
        out.append(getResponseMetadata());
        out.append("</GetQueueAttributesResponse>");
        
        return out.toString();
    }
    
    private static String fillAttribute(String name, String value) {
        return "<Attribute>\n<Name>" + name + "</Name>\n<Value>" + value + "</Value>\n</Attribute>";
    }

    public static String setQueueAttributesResponse() {
        return "<SetQueueAttributesResponse>" + getResponseMetadata() + "</SetQueueAttributesResponse>";
    }
    
    public static String getQueueUrlResponse(CQSQueue queue) {

    	String out = "<GetQueueUrlResponse>\n" +
                "    <GetQueueUrlResult>\n";
        
        if (queue.getRelativeUrl().startsWith("http")) {
        	//legacy queue
        	out += "        <QueueUrl>" + queue.getRelativeUrl() + "</QueueUrl>\n";
        } else {
        	out += "        <QueueUrl>" + queue.getAbsoluteUrl() + "</QueueUrl>\n";
        }

        out += "    </GetQueueUrlResult>\n" +
        getResponseMetadata() +
        "</GetQueueUrlResponse>";

        return out;
    }
}
