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

import java.util.LinkedList;
import java.util.List;

import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;

/**
 * Class represents an endpoint publish job which contains the message and a list of subscribers
 * to send notification to
 * @author aseem, bwolf
 *
 * Class is immutable
 */
public class CNSEndpointPublishJob {
    //Note the reason we do this craziness instead of just using JSON is for space efficiency
    
    public CNSMessage getMessage() {
        return message;
    }

    public List<? extends CNSEndpointSubscriptionInfo> getSubInfos() {
        return subInfos;
    }

    /**
     * CNSEndpointSubscriptionInfo represents the minimum info on a subscriber used for (de)serialization
     * That is essential for CNSEndpointPublishJob serialization
     * Class is immutable
     */
    public static class CNSEndpointSubscriptionInfo {
    	
        public final CnsSubscriptionProtocol protocol;
        public final String endpoint;
        public final String subArn;
        public final boolean rawDelivery;
        
        public CNSEndpointSubscriptionInfo(CnsSubscriptionProtocol protocol, String endpoint, String subArn, boolean rawDelivery) {
            this.protocol = protocol;
            this.endpoint = endpoint;
            this.subArn = subArn;
            this.rawDelivery = rawDelivery;
        }
        
        /**
         * serialized form is <protocol-ord>|<endpoint>|<subArn>
         * @return serialized form
         */
        public String serialize() {
            StringBuffer sb = new StringBuffer();
            sb.append(protocol.ordinal()).append("|").append(endpoint).append("|").append(subArn).append("|").append(rawDelivery);
            return sb.toString();
        }
        
        /**
         * Parse the string and create new SubInfo
         * @param str serialized form
         * @return SubInfo object from the serializedform
         */
        public static CNSEndpointSubscriptionInfo parseInstance(String str) {
            String arr[] = str.split("\\|");
            if (arr.length != 4) {
                throw new IllegalArgumentException("Expected format for SubInfo is <protocol-ord>|<endpoint>|<subArn>|<rawDelivery> got:" + str);
            }
            //TODO: store raw flag as "0" or "1" instead of "true" or "false" for efficiency
            return new CNSEndpointSubscriptionInfo(CnsSubscriptionProtocol.values()[Integer.parseInt(arr[0])], arr[1], arr[2], Boolean.parseBoolean(arr[3]));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CNSEndpointSubscriptionInfo)) {
                return false;
            }
            CNSEndpointSubscriptionInfo o = (CNSEndpointSubscriptionInfo) obj;
            if (Util.isEqual(protocol, o.protocol) &&
                    Util.isEqual(endpoint, o.endpoint) &&
                    Util.isEqual(subArn, o.subArn) &&
                    Util.isEqual(rawDelivery, o.rawDelivery)) {
                return true;
            } else {
                return false;
            }
        }
    }
    
    private final CNSMessage message;
    private final List<? extends CNSEndpointSubscriptionInfo> subInfos;
    
    public CNSEndpointPublishJob(CNSMessage message, List<? extends CNSEndpointSubscriptionInfo> subInfos) {
        this.message = message;
        this.subInfos = subInfos;
    }
    
    /**
     * 
     * @return a Unicode string representing the entire job
     * <num-subinfos>\n[<sub-info>\n<sub-info>...\n]<CNSPublishJob>
     */
    public String serialize() {
        StringBuffer sb = new StringBuffer();
        sb.append(subInfos.size()).append("\n");
        for (CNSEndpointSubscriptionInfo subInfo : subInfos) {
            sb.append(subInfo.serialize()).append("\n");
        }
        sb.append(message.serialize());
        return sb.toString(); 
    }

    /**
     * Parse the serialized form and return instance of this class
     * @param str serialized form
     * @return instance from the serialized form
     * @throws CMBException
     */
    public static CNSEndpointPublishJob parseInstance(String str) throws CMBException {
        String arr[] = str.split("\n");
        if (arr.length < 2) {
            throw new IllegalArgumentException("Expected at least two tokens in CNSEndpointPublishJob serial representation. Expect4ed <num-subinfos>\n[<sub-info>\n<sub-info>...\n]<CNSPublishJob>. Got:" + str);
        }
        int numSubInfos = Integer.parseInt(arr[0]);   
        int idx = 1;
        List<CNSEndpointSubscriptionInfo> subInfos = new LinkedList<CNSEndpointPublishJob.CNSEndpointSubscriptionInfo>();
        for (int i = 0; i < numSubInfos; i++) {
            subInfos.add(CNSEndpointSubscriptionInfo.parseInstance(arr[idx++]));
        }
        
        StringBuffer sb = new StringBuffer();
        for (int j = idx; j < arr.length; j++) {
            if (j != idx) {
                sb.append("\n");
            }
            sb.append(arr[j]);
        }
        
        CNSMessage message = CNSMessage.parseInstance(sb.toString());
        message.processMessageToProtocols();
        
        return new CNSEndpointPublishJob(message, subInfos);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CNSEndpointPublishJob)) {
            return false;
        }
        CNSEndpointPublishJob o = (CNSEndpointPublishJob)obj;
        if (Util.isEqual(message, o.message) &&
                Util.isCollectionsEqual(subInfos, o.subInfos)) {
            return true;
        } else {
            return false;
        }        
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("message=").append(message.toString()).append(" subInfos=");
        for (CNSEndpointSubscriptionInfo subInfo : subInfos) {
            sb.append("subInfo=").append(subInfo.toString());
        }
        return sb.toString();
    }
}
