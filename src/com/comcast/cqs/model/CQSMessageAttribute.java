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

import java.nio.ByteBuffer;
import com.sun.jersey.core.util.Base64;
import com.comcast.cmb.common.util.CMBException;

public class CQSMessageAttribute {
	
	String Value;
	String DataType;

	public CQSMessageAttribute(String Value, String DataType) {
		this.Value = Value;
		if (DataType.toLowerCase().equals("string")) {
			DataType = "String";
		} else if (DataType.toLowerCase().equals("number")) {
			DataType = "Number";
		} else if (DataType.toLowerCase().equals("binary")) {
			DataType = "Binary";
		}
		this.DataType = DataType;
		
	}
	
	public String getStringValue() {
		return Value;
	}
	
	public ByteBuffer getBinaryValue() {
		if (DataType.equals("Binary")) {
			return ByteBuffer.wrap(Base64.decode(Value));
		} else {
			return ByteBuffer.wrap(Value.getBytes());
		}
	}
	
	public String getDataType() {
		return DataType;
	}
	
	public int size() {
		if (Value == null) {
			return 0;
		}
		return Value.length();
	}
	
	public void validate() throws CMBException {
		if (DataType.equals("String")) {
			//TODO
		} else if (DataType.equals("Number")) {
			//TODO
		} else if (DataType.equals("Binary")) {
			//TODO
		}
	}
}
