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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.amazonaws.services.sqs.model.Message;
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
	
	transient private String md5OfMessageAttributes;

	private String body;

	transient private Map<String, String> attributes;
	
	transient private Map<String, CQSMessageAttribute> messageAttributes;

	transient private String suppliedMessageId;

	private transient Object timebasedId; 

	public CQSMessage(String body, Map<String, String> attributes) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		this.messageId = UUID.randomUUID().toString();
		this.receiptHandle = this.messageId;
		this.body = body;
		this.attributes = attributes;

		if (body != null) {
			setMD5OfBody(getMD5(body));
		}
	}
	
	public CQSMessage(String body, Map<String, String> attributes, Map<String, CQSMessageAttribute> messageAttributes) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		this.messageId = UUID.randomUUID().toString();
		this.receiptHandle = this.messageId;
		this.body = body;
		this.attributes = attributes;

		if (body != null) {
			setMD5OfBody(getMD5(body));
		}
		
		if (messageAttributes != null && messageAttributes.size() > 0) {
			this.messageAttributes = messageAttributes;
			this.md5OfMessageAttributes = getMD5(this.messageAttributes);
		}
	}

	public CQSMessage() {
	}

	public CQSMessage(String messageId, String body) throws NoSuchAlgorithmException, UnsupportedEncodingException {

		this.messageId = messageId;
		this.receiptHandle = messageId;
		this.body = body;

		if (body != null) {
			setMD5OfBody(getMD5(body));
		}
	}
	
	public CQSMessage(Message message) {
		
		this.messageId = message.getMessageId();
		this.receiptHandle = message.getReceiptHandle();
		this.body = message.getBody();
		this.mD5OfBody = message.getMD5OfBody();
	}

	public String getMessageId() {
		return this.messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
		this.receiptHandle = messageId;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public Map<String, CQSMessageAttribute> getMessageAttributes() {
		return messageAttributes;
	}

	public void setMessageAttributes(Map<String, CQSMessageAttribute> messageAttributes) {
		this.messageAttributes = messageAttributes;
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

	public String getMD5OfMessageAttributes() {
		return md5OfMessageAttributes;
	}

	public void setMD5OfMessageAttributes(String md5OfMessageAttributes) {
		this.md5OfMessageAttributes = md5OfMessageAttributes;
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
	
	private String getMD5(String message) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
		MessageDigest digest = MessageDigest.getInstance("MD5");
		byte bytes[] = digest.digest(message.getBytes("UTF-8"));
        
        StringBuilder md5 = new StringBuilder(bytes.length*2);

        for (int i = 0; i < bytes.length; i++) {
        	String hexByte = Integer.toHexString(bytes[i]);
        	if (hexByte.length() == 1) {
                md5.append("0");
            } else if (hexByte.length() == 8) {
            	hexByte = hexByte.substring(6);
            }
        	md5.append(hexByte);
        }
        
        String result = md5.toString().toLowerCase();
        return result;
	}

	private String getMD5(Map<String, CQSMessageAttribute> messageAttributes) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		
        if (messageAttributes == null && messageAttributes.size() == 0) {
        	return null;
        }

        List<String> sortedKeys = new ArrayList<String>(messageAttributes.keySet());
        Collections.sort(sortedKeys);
        
        MessageDigest digest = MessageDigest.getInstance("MD5");
	
        for (String attrName : sortedKeys) {
	        CQSMessageAttribute ma = messageAttributes.get(attrName);
	        setLengthBytes(digest, attrName);
	        setLengthBytes(digest, ma.getDataType());
	        if (ma.getStringValue() != null) {
	        	digest.update((byte)1);
	        	setLengthBytes(digest, ma.getStringValue());
	        } else if (ma.getBinaryValue() != null) {
	        	digest.update((byte)2);
	        	setLengthBytes(digest, ma.getBinaryValue());
	        } 
	        /*else if (ma.getStringListValues() != null) {
		        digest.update((byte)3);
		        for (String strListMember : ma.getStringListValues()) {
		        setLengthBytes(md5Digest, strListMember);
		        }
	        } else if (ma.getBinaryListValues() != null) {
		        md5Digest.update((byte)4);
		        for (ByteBuffer byteListMember : ma.getBinaryListValues()) {
		        setLengthBytes(md5Digest, byteListMember);
		        }
	        }*/
        }

        byte bytes[] = digest.digest();
        StringBuilder md5 = new StringBuilder(bytes.length*2);

        for (int i = 0; i < bytes.length; i++) {
        	String hexByte = Integer.toHexString(bytes[i]);
        	if (hexByte.length() == 1) {
                md5.append("0");
            } else if (hexByte.length() == 8) {
            	hexByte = hexByte.substring(6);
            }
        	md5.append(hexByte);
        }
        
        String result = md5.toString().toLowerCase();
        return result;
	}
	
	private void setLengthBytes(MessageDigest digest, String str) throws UnsupportedEncodingException {
		byte[] bytes = str.getBytes(Charset.forName("UTF-8"));
		ByteBuffer lengthBytes = ByteBuffer.allocate(4).putInt(bytes.length);
		digest.update(lengthBytes.array());
		digest.update(bytes);
	}

	/*private void setLengthBytes(MessageDigest digest, ByteBuffer binaryValue) {
		binaryValue.rewind();
		int size = binaryValue.remaining();
		ByteBuffer lengthBytes = ByteBuffer.allocate(4).putInt(size);
		digest.update(lengthBytes.array());
		digest.update(binaryValue);
	}*/

	/**
	 * The persistent state of the CQSMessage
	 * Note: Object returned does not have the messageId or the receiptHandle set. caller must set it
	 * @param aInputStream
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 * @throws NoSuchAlgorithmException 
	 */
	private void readObject(ObjectInputStream aInputStream) throws IOException, ClassNotFoundException, NoSuchAlgorithmException {
		
		// always perform the default de-serialization first
		
		aInputStream.defaultReadObject(); //should parse body
		int attCount = aInputStream.readInt();
		attributes = new HashMap<String, String>();

		for (int i = 0; i < attCount; i++) {
			String key = aInputStream.readUTF();
			String val = aInputStream.readUTF();
			attributes.put(key, val);
		}

		setMD5OfBody(getMD5(body));
	}

	private void writeObject(ObjectOutputStream aOutputStream) throws IOException {

		// perform the default serialization for all non-transient, non-static fields
		
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