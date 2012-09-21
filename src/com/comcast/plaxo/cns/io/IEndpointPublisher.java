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
package com.comcast.plaxo.cns.io;

import com.comcast.plaxo.cmb.common.model.User;

/**
 * The interface is for sending messages to a particular endpoint, first set the endpoint, and the message, and optionally the
 * subject and user, and send the message to the endpoint using send().
 * 
 * @author aseem, jorge, bwolf
 */
public interface IEndpointPublisher {
	
	 /**
	  * Set the endpoitn we are going to send the message to.
	  * @param endpoint
	  */
	 public void setEndpoint(String endpoint);
	 
	 /**
	  * Get the enpoint we are goint to send the message to.
	  * @return the enpoint we set
	  */
	 public String getEndpoint();
	 
	 /**
	  * Set the message we are going to send to the endpoint
	  * @param message
	  */
	 public void setMessage(String message);
	 
	 public String getMessage();

	 /**
	  * The user is only used for CQS to send the message to the queue
	  * In which case user is the publisher
	  * @param user
	  */
	 public void setUser(User user);
	 
	 public User getUser();
	 
	 /**
	  * The subject is only used for email
	  * @param subject
	  */
	 public void setSubject(String subject);
	 
	 public String getSubject();
	 

	 /**
	  * Sends the message to the affiliated endpoint
	  * Issues with sending the message will lead to an exception
	  * The response can be retrieved in the callback.
	  */
	 public void send() throws Exception;
}
