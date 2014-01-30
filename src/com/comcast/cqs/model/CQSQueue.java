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
package com.comcast.cqs.model;

import com.comcast.cmb.common.util.CMBProperties;

/**
 * Model for a queue
 * @author baosen, vvenkatraman, aseem, bwolf
 *
 */
public class CQSQueue {

	private String arn;
    private String name;
    private String ownerUserId;
    private String relativeUrl;
    private String region;
    private String serviceEndpoint;

	private int visibilityTO = 30; // sec
    private int maxMsgSize = 65536; //bytes
    private int msgRetentionPeriod = 345600; //sec
    private int delaySeconds = 0; //sec
    private String policy = "";
    private long createdTime;
    private long modifiedTime;
    private int numMessages;
    private int receiveMessageWaitTimeSeconds = 0;
    private int numberOfPartitions = 100;
    private int numberOfShards = 1;
    private long lastRevisibleSetProcessingTime = 0;
    private long lastVisibilityProcessingTime = 0;
    private boolean compressed = false;
    
	public CQSQueue(String name, String ownerId) {
    	
        this.ownerUserId = ownerId;
        this.region = CMBProperties.getInstance().getRegion();

        setName(name);
        
        this.visibilityTO = CMBProperties.getInstance().getCQSVisibilityTimeOut();
        this.maxMsgSize = CMBProperties.getInstance().getCQSMaxMessageSize();
        this.msgRetentionPeriod = CMBProperties.getInstance().getCQSMessageRetentionPeriod();
        this.delaySeconds = CMBProperties.getInstance().getCQSMessageDelaySeconds();
        this.numberOfPartitions = CMBProperties.getInstance().getCQSNumberOfQueuePartitions();
        this.numberOfShards = 1;
        this.compressed = false;
	}
	
	public boolean isCompressed() {
		return compressed;
	}

	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}

	@Override 
	public Object clone() throws CloneNotSupportedException {
		CQSQueue queue = (CQSQueue)super.clone();
		return queue;
	}

	public void setName(String name) {
        
		this.name = name;
        
        setArn("arn:cmb:cqs:" + region + ":" + this.ownerUserId + ":" + name);
        String serviceUrl = CMBProperties.getInstance().getCQSServiceUrl();
        
        if (serviceUrl != null && serviceUrl.endsWith("/")) {
        	serviceUrl = serviceUrl.substring(0, serviceUrl.length()-1);
        }
        
        setServiceEndpoint(serviceUrl);
        setRelativeUrl(this.ownerUserId + "/" + name);
	}
	
	public int getShardNumber() {
		if (this.name.contains(".")) {
			return Integer.parseInt(this.name.substring(this.name.indexOf(".")));
		} else {
			return 0;
		}
	}
	
    public int getNumMessages() {
        return numMessages;
    }

    public void setNumMessages(int numMessages) {
        this.numMessages = numMessages;
    }

    public String getArn() {
        return arn;
    }

    public String getName() {
        return name;
    }

    public String getOwnerUserId() {
        return ownerUserId;
    }

    public String getRelativeUrl() {
        return relativeUrl;
    }

    public String getAbsoluteUrl() {
        return serviceEndpoint + "/" + relativeUrl;
    }

    public String getRegion() {
        return region;
    }

    public int getVisibilityTO() {
        return visibilityTO;
    }

    public int getMaxMsgSize() {
        return maxMsgSize;
    }

    public int getMsgRetentionPeriod() {
        return msgRetentionPeriod;
    }

    public int getDelaySeconds() {
        return delaySeconds;
    }

    public String getPolicy() {
        return this.policy;
    }
    
    public long getCreatedTime() {
        return createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }
    
    public void setRegion(String region) {
        this.region =region;
    }

    public void setVisibilityTO(int visibilityTO) {
        this.visibilityTO = visibilityTO;
    }

    public void setMaxMsgSize(int maxMsgSize) {
        this.maxMsgSize = maxMsgSize;
    }

    public void setMsgRetentionPeriod(int msgRetentionPeriod) {
        this.msgRetentionPeriod = msgRetentionPeriod;
    }

    public void setDelaySeconds(int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    public void setPolicy(String policy) {
        this.policy = policy;
    }
    
    public void setCreatedTime(long createdTime) {
    	this.createdTime = createdTime;
    }

	public void setRelativeUrl(String relativeUrl) {
		this.relativeUrl = relativeUrl;
	}

	public void setArn(String arn) {
		this.arn = arn;
	}

	public String getServiceEndpoint() {
		return serviceEndpoint;
	}

	public void setServiceEndpoint(String serviceEndpoint) {
		this.serviceEndpoint = serviceEndpoint;
	}

	public int getReceiveMessageWaitTimeSeconds() {
		return receiveMessageWaitTimeSeconds;
	}

	public void setReceiveMessageWaitTimeSeconds(int receiveMessageWaitTimeSeconds) {
		this.receiveMessageWaitTimeSeconds = receiveMessageWaitTimeSeconds;
	}
	
	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}

	public int getNumberOfShards() {
		return numberOfShards;
	}

	public void setNumberOfShards(int numberOfShards) {
		this.numberOfShards = numberOfShards;
	}
	
	public void setLastRevisibleSetProcessingTime(long time) {
		this.lastRevisibleSetProcessingTime = time;
	}
	
	public long getLastRevisibleSetProcessingTime() {
		return lastRevisibleSetProcessingTime;
	}
	
	public void setLastVisibilityProcessingTime(long time) {
		this.lastVisibilityProcessingTime = time;
	}
	
	public long getLastVisibilityProcessingTime() {
		return lastVisibilityProcessingTime;
	}
	
}
