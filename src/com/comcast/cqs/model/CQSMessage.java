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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;

import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.util.CQSConstants;

/**
 * Model for a cqs message
 * @author aseem, baosen, vvenkatraman
 *
 */
public final class CQSMessage implements Serializable {
	
	private static final long serialVersionUID = 1L;

	transient private String messageId;

	transient private String receiptHandle;

	transient private String mD5OfBody;

	private String body;

	transient private Map<String, String> attributes;

	transient private String suppliedMessageId;

	private transient Object timebasedId; 

	public CQSMessage(String body, Map<String, String> attributes) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		this.messageId = UUID.randomUUID().toString();
		
		setBody(body);
		setAttributes(attributes);
		MessageDigest digest = MessageDigest.getInstance("MD5");
		setMD5OfBody(Base64.encodeBase64String(digest.digest(body.getBytes("UTF-8"))));
	}

	public CQSMessage(String messageId, String body) throws NoSuchAlgorithmException, UnsupportedEncodingException {

		this.messageId = messageId;
		this.body = body;

		if (body != null) {
			MessageDigest digest = MessageDigest.getInstance("MD5");
			setMD5OfBody(Base64.encodeBase64String(digest.digest(body.getBytes("UTF-8"))));
		}
	}

	public String getMessageId() {
		return this.messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
		setReceiptHandle(messageId);
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, String> attributes) {
		this.attributes = attributes;
	}

	public String getReceiptHandle() {
		return receiptHandle;
	}

	public void setReceiptHandle(String receiptHandle) {
		this.receiptHandle = receiptHandle;
	}

	public String getMD5OfBody() {
		return mD5OfBody;
	}

	public void setMD5OfBody(String mD5OfBody) {
		this.mD5OfBody = mD5OfBody;
	}

	public java.lang.String getBody() {
		return body;
	}

	public void setBody(java.lang.String body) {
		this.body = body;
	}

	public Integer getDelaySeconds() {
		
		if (attributes != null && attributes.containsKey(CQSConstants.DELAY_SECONDS)) {
			return Integer.parseInt(attributes.get(CQSConstants.DELAY_SECONDS));
		}
		
		return null;		
	}

	public String getSuppliedMessageId() {
		return suppliedMessageId;
	}

	public void setSuppliedMessageId(String suppliedMessageId) {
		this.suppliedMessageId = suppliedMessageId;
	}

	public Object getTimebasedId() {
		return timebasedId;
	}

	public void setTimebasedId(Object timebasedId) {
		this.timebasedId = timebasedId;
	}

	/**
	 * The persistent state of the CQSMessage
	 * Note: Object returned does not have the messageId or the receiptHandle set. caller must set it
	 * @param aInputStream
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws NoSuchAlgorithmException 
	 */
	private void readObject(ObjectInputStream aInputStream) throws IOException, ClassNotFoundException, NoSuchAlgorithmException {
		
		//always perform the default de-serialization first
		
		aInputStream.defaultReadObject(); //should parse body
		int attCount = aInputStream.readInt();
		attributes = new HashMap<String, String>();

		for (int i = 0; i < attCount; i++) {
			String key = aInputStream.readUTF();
			String val = aInputStream.readUTF();
			attributes.put(key, val);
		}

		MessageDigest digest = MessageDigest.getInstance("MD5");
		this.setMD5OfBody(Base64.encodeBase64String(digest.digest(body.getBytes("UTF-8"))));
	}

	private void writeObject(ObjectOutputStream aOutputStream) throws IOException {

		//perform the default serialization for all non-transient, non-static fields
		
		aOutputStream.defaultWriteObject(); //should serialize body
		aOutputStream.writeInt(attributes.size());
		
		for (Entry<String, String> keyVal : attributes.entrySet()) {
			aOutputStream.writeUTF(keyVal.getKey());
			aOutputStream.writeUTF(keyVal.getValue());
		}
	}

	@Override
	public boolean equals(Object obj) {
		
		if (!(obj instanceof CQSMessage)) {
			return false;
		}
		
		CQSMessage msg = (CQSMessage) obj;
		
		if (Util.isEqual(messageId, msg.messageId) &&
				Util.isEqual(receiptHandle, msg.receiptHandle) &&
				Util.isEqual(mD5OfBody, msg.mD5OfBody) &&
				Util.isEqual(suppliedMessageId, msg.suppliedMessageId) &&
				Util.isEqual(timebasedId, msg.timebasedId) &&
				Util.isMapsEquals(attributes, msg.attributes)) {
			return true;	        
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return messageId.hashCode();
	}
	
	@Override
	public String toString() {
		return "message_id=" + messageId + ", receipt_handle=" + receiptHandle + ", md5=" + mD5OfBody + ", body=" + body;
	}
} 