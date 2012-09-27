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
package com.comcast.plaxo.cqs.model;

/**
 * 
 * @author baosen
 *
 */
public class CQSBatchResultErrorEntry {

	private String id;
    private boolean senderFault;
    private String code;
    private String message;
    
    public CQSBatchResultErrorEntry(String id, boolean senderFault, String code, String message) {
        this.id = id;
        this.senderFault = senderFault;
        this.code = code;
        this.message = message;
    }
    
    public void setId(String id) {
        this.id = id;
    }

    public void setSenderFault(boolean bool) {
        this.senderFault = bool;
    }

    public void setMessage(String str) {
        this.message = str;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getId() {
        return id;
    }

    public boolean getSenderFault() {
        return senderFault;
    }

    public String getCode() {
        return code;
    }
    
    public String getMessage() {
        return message;
    }
}
