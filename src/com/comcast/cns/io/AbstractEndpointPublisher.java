package com.comcast.cns.io;

import com.comcast.cmb.common.model.User;

public abstract class AbstractEndpointPublisher implements IEndpointPublisher {

	protected String endpoint;
	protected String message;
	protected User user;
	protected String subject;
	protected Boolean rawMessageDelivery = false;
	

	@Override
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public void setMessage(String message) {
		this.message = message;     
	}

	@Override
	public String getEndpoint() {
		return endpoint;
	}

	@Override
	public String getMessage() {        
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
	abstract public void send() throws Exception;

}
