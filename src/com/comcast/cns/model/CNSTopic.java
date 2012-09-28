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

import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.Util;

/**
 * Represents a Topic
 * @author bwolf, jorge
 *
 * Class is not thread-safe. Caller must ensure thread-safety
 */
public class CNSTopic {
	
	private String arn;
	private String name;
	private String displayName;
	private String userId;
	//private CNSTopicAttributes attributes;
	
	public CNSTopic(String arn, String name, String displayName, String userId) {
		this.arn = arn;
		this.name = name;
		this.displayName = displayName;
		this.userId = userId;
	}

	public String getUserId() {
		return userId;
	}
	
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	public String getArn() {
		return arn;
	}
	
	public void setArn(String arn) {
		this.arn = arn;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getDisplayName() {
		return displayName;
	}
	
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	
	public void checkIsValid() throws CMBException {

		if (arn == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set arn for subscription");
		}
		
		if (!com.comcast.cns.util.Util.isValidTopicArn(arn)) {
			throw new CMBException(CMBErrorCodes.InternalError, "Invalid topic arn");
		}

		if (name == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set name for subscription");
		}
		
		if (userId == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set user id for subscription");
		}
	}
	@Override
	public String toString() {
		return "arn=" + getArn() + " name=" + getName() + " display_name=" + getDisplayName() + " user_id=" + getUserId();
	}
	@Override
	public boolean equals(Object o) {
		
		if (!(o instanceof CNSTopic)) {
			return false;
		}
		
		CNSTopic t = (CNSTopic)o;
		
		if (Util.isEqual(getArn(), t.getArn()) &&
				Util.isEqual(getName(), t.getName()) &&
				Util.isEqual(getDisplayName(), t.getDisplayName()) &&
				Util.isEqual(getUserId(), t.getUserId())) {
			return true;
		}
		
		return false;
	}
}
