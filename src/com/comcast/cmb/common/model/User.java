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
package com.comcast.plaxo.cmb.common.model;

/**
 * @author michael, bwolf
 * Class ia immutable
 */
public final class User {

	// system generated user ID, must be globally unique
	final private String userId;          
	
	// user name, must be globally unique
    final private String userName;        
    
    // hashed password    
    final private String hashedPassword;  
    final private String accessKey;       
    final private String accessSecret;
    
    public User(String userId, String userName, String hashedPassword, String accessKey, String accessSecret) {
        this.userId = userId;
        this.userName = userName;
        this.hashedPassword = hashedPassword;
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
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
}
