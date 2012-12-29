package com.comcast.cns.io;

public interface IHttpPostReactor {
	
	public void onSuccess();
	
	public void onFailure(int status);
	
	public void onExpire();
}
