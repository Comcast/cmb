package com.comcast.cns.io;

import java.util.Map;

import com.comcast.cmb.common.model.User;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cqs.model.CQSMessageAttribute;

public abstract class AbstractEndpointPublisher implements IEndpointPublisher {

	protected String endpoint;
	protected CNSMessage message;
	protected User user;
	protected String subject;
	protected Boolean rawMessageDelivery = false;
	protected String messageType;
	protected String messageId;
	protected String topicArn;
	protected String subscriptionArn;
	protected Map<String, CQSMessageAttribute> messageAttributes; 

	@Override
	public Map<String, CQSMessageAttribute> getMessageAttributes() {
		return messageAttributes;
	}

	@Override
	public void setMessageAttributes(
			Map<String, CQSMessageAttribute> messageAttributes) {
		this.messageAttributes = messageAttributes;
	}

	@Override
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public void setMessage(CNSMessage message) {
		this.message = message;     
	}

	@Override
	public String getEndpoint() {
		return endpoint;
	}

	@Override
	public CNSMessage getMessage() {        
		return message;
	}

	@Override
	public void setUser(User user) {
		this.user = user;       
	}

	@Override
	public User getUser() {
		return user;
	}

	@Override
	public void setSubject(String subject) {
		this.subject = subject;
	}

	@Override
	public String getSubject() {
		return this.subject;
	}
	@Override
	public void setRawMessageDelivery(Boolean rawMessageDelivery){
		this.rawMessageDelivery = rawMessageDelivery;
	}
	
	@Override
	public Boolean getRawMessageDelivery(){
		return this.rawMessageDelivery;
	}
	
	@Override
	public String getMessageType() {
		return messageType;
	}

	@Override
	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}

	@Override
	public String getMessageId() {
		return messageId;
	}

	@Override
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	@Override
	public String getTopicArn() {
		return topicArn;
	}

	@Override
	public void setTopicArn(String topicArn) {
		this.topicArn = topicArn;
	}

	@Override
	public String getSubscriptionArn() {
		return subscriptionArn;
	}
	
	@Override
	public void setSubscriptionArn(String subscriptionArn) {
		this.subscriptionArn = subscriptionArn;
	}

	@Override
	abstract public void send() throws Exception;
}
