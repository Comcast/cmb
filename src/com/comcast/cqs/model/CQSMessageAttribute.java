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
