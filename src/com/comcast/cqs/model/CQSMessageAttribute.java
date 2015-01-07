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
		if (DataType.equals("String") || DataType.equals("Number")) {
			return Value;
		} else {
			return null;
		}
	}
	
	public String getBinaryValue() {
		if (DataType.equals("Binary")) {
			return Value;
		} else {
			return null;
		}
	}
	
	public String getValue() {
		return Value;
	}

	public String getDataType() {
		return DataType;
	}
}
