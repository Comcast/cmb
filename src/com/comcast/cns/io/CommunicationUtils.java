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
package com.comcast.cns.io;

import java.util.Map;

import com.comcast.cmb.common.model.User;
import com.comcast.cns.model.CNSSubscription;

/**
 * Utility functions for 
 * @author aseem, jorge, bwolf
 *
 */
public class CommunicationUtils {

	/**
	 * addParam adds a parameter with name name, and value val to the set of parameters
	 * params
	 * @param params, the list of parameters
	 * @param name, the name of the parameter to add
	 * @param val, the value of the parameter to add
	 */
	public static void addParam(Map<String, String[]> params, String name, String val) {        
		String[] paramVals = new String[1];
		paramVals[0] = val;
		params.put(name, paramVals);        
	}
	
    /*
     * Send a message using the appropriate method for the given protocol to the endpoint in endPoint
     * @param user, the user that is sending the message
     * @param protocol, the protocol for the
     */
	public static String sendMessage(User user, CNSSubscription.CnsSubscriptionProtocol protocol, String endPoint, String message) throws Exception {	
		
		IEndpointPublisher publisher =  EndpointPublisherFactory.getPublisherInstance(protocol);
		publisher.setUser(user);
		publisher.setEndpoint(endPoint);
		publisher.setMessage(message);
		
		if(protocol == CNSSubscription.CnsSubscriptionProtocol.email || protocol == CNSSubscription.CnsSubscriptionProtocol.email_json) {
			publisher.setSubject("CMB Notification Message");
		}
		
		publisher.send();
		
		return "success";
	}
}
