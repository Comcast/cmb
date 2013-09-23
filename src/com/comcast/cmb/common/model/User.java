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
package com.comcast.cmb.common.model;

/**
 * @author michael, bwolf
 * Class ia immutable
 */
public final class User {

	// system generated user ID, must be globally unique
	private final String userId;          
	
	// user name, must be globally unique
    private final String userName;        
    
    // hashed password    
    private final String hashedPassword;  
    private final String accessKey;       
    private final String accessSecret;
    private Boolean isAdmin;
    private String description = "";
    
    // some stats about the user
    
    private long numQueues;
	private long numTopics;
    
    public User(String userId, String userName, String hashedPassword, String accessKey, String accessSecret, Boolean isAdmin) {
        this.userId = userId;
        this.userName = userName;
        this.hashedPassword = hashedPassword;
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.isAdmin = isAdmin;
        this.setDescription("");
    }
    
    public User(String userId, String userName, String hashedPassword, String accessKey, String accessSecret) {
        this.userId = userId;
        this.userName = userName;
        this.hashedPassword = hashedPassword;
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.isAdmin = false;
        this.setDescription("");
    }
    
    public User(String userId, String userName, String hashedPassword, String accessKey, String accessSecret, Boolean isAdmin, String description) {
        this.userId = userId;
        this.userName = userName;
        this.hashedPassword = hashedPassword;
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.isAdmin = isAdmin;
        this.setDescription(description);
    }
    
    public String getUserId() {
        return userId;
    }

    public String getUserName() {
        return userName;
    }

    public String getHashPassword() {
        return hashedPassword;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getAccessSecret() {
        return accessSecret;
    }

    @Override
    public String toString() {
        return "user_id=" + userId + " user_name=" + userName;
    }
    
    public long getNumQueues() {
		return numQueues;
	}

	public void setNumQueues(long numQueues) {
		this.numQueues = numQueues;
	}

	public long getNumTopics() {
		return numTopics;
	}

	public void setNumTopics(long numTopics) {
		this.numTopics = numTopics;
	}

	public Boolean getIsAdmin() {
		return isAdmin;
	}
	
	public void setIsAdmin(Boolean isAdmin) {
		this.isAdmin = isAdmin;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
