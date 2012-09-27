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
package com.comcast.cmb.test.tools;

import java.io.CharArrayWriter;
import java.util.jar.Attributes;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;


public class TopicAttributeParser  extends org.xml.sax.helpers.DefaultHandler {

	private CharArrayWriter content = new CharArrayWriter();
	private Keys key;
	private String effectiveDeliveryPolicy;
	private String owner;
	private int subscriptionsPending;
	private String policy;
	private int subscriptionsConfirmed;
	private int subscriptionsDeleted;
	private String topicArn;
	private String displayName;
	private String deliveryPolicy;
	
	
	public enum Keys {EffectiveDeliveryPolicy, Owner, SubscriptionsPending, Policy, SubscriptionsConfirmed, TopicArn, SubscriptionsDeleted, DeliveryPolicy, DisplayName}
	
	private static Logger logger = Logger.getLogger(SubscriptionAttributeParser.class);
	
	public TopicAttributeParser() {
		
	}
	 
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		 content.reset();
        logger.info("Start Element QName:" + qName);
		 if (qName.equals("Atrributes")) {
			 //logger.debug("Atrributes Start");
		 } else if (qName.equals("entry")) {
			 
		 } if(qName.equals("key")) {
			
			
		 } else if(qName.equals("value")) {
			 
		 }
	 }
	 
	 public void endElement (String uri, String localName, String qName) {
		 //System.out.println("End Element QName:" + qName);
		 if (qName.equals("Atrributes")) {
			 //logger.debug("Atrributes End");
		 } else if (qName.equals("entry")) {
			 //logger.debug("entry End");
		 } else if (qName.equals("key")) {
			 String keyStr = content.toString();
			 key = Keys.valueOf(keyStr);
			 //logger.debug("key is:" +key);
		 } else if (qName.equals("value")) {
			 //logger.debug("value is:" + content.toString());
			 switch(key) {
			 case EffectiveDeliveryPolicy: effectiveDeliveryPolicy = content.toString();
			 	break;
			 case Owner: owner = content.toString();
			 	break;
			 case SubscriptionsPending: setSubscriptionsPending(Integer.parseInt(content.toString()));
			 	break;
			 case SubscriptionsConfirmed: setSubscriptionsConfirmed(Integer.parseInt(content.toString()));
			 	break;
			 case SubscriptionsDeleted: setSubscriptionsDeleted(Integer.parseInt(content.toString()));
			 	break;
			 case Policy: setPolicy(content.toString());
			 	break;
			 case TopicArn: topicArn = content.toString();
			 	break;
			 case DeliveryPolicy: setDeliveryPolicy(content.toString());
			    break;
			 case DisplayName: setDisplayName(content.toString());
			    break;
			 }
			
		 } 
		 content.reset();  
	 }
	 
	 public void characters( char[] ch, int start, int length ) {
		 //System.out.println("Characters");
		 content.write( ch, start, length );
	 }
	 
	 public String getEffectiveDeliveryPolicy() {
		 return effectiveDeliveryPolicy;
	 }
	 
	 public String getOwner() {
		 return owner;
	 }
	 

	 public String getTopicArn() {
		 return topicArn;
	 }

	public int getSubscriptionsPending() {
		return subscriptionsPending;
	}

	public void setSubscriptionsPending(int subscriptionsPending) {
		this.subscriptionsPending = subscriptionsPending;
	}

	public String getPolicy() {
		return policy;
	}

	public void setPolicy(String policy) {
		this.policy = policy;
	}

	public int getSubscriptionsConfirmed() {
		return subscriptionsConfirmed;
	}

	public void setSubscriptionsConfirmed(int subscriptionsConfirmed) {
		this.subscriptionsConfirmed = subscriptionsConfirmed;
	}

	public int getSubscriptionsDeleted() {
		return subscriptionsDeleted;
	}

	public void setSubscriptionsDeleted(int subscriptionsDeleted) {
		this.subscriptionsDeleted = subscriptionsDeleted;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public String getDeliveryPolicy() {
		return deliveryPolicy;
	}

	public void setDeliveryPolicy(String deliveryPolicy) {
		this.deliveryPolicy = deliveryPolicy;
	}

}



