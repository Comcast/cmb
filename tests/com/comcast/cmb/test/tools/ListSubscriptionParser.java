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
import java.util.Vector;
import java.util.jar.Attributes;

import org.xml.sax.SAXException;

import com.comcast.cns.test.unit.CNSSubscriptionTest;

public class ListSubscriptionParser  extends org.xml.sax.helpers.DefaultHandler {
	
	private Vector<CNSSubscriptionTest> subscriptions;
	private CNSSubscriptionTest sub;
	private String requestId;
	private CharArrayWriter content = new CharArrayWriter();
	private int count = 0;
	private String nextToken;
	
	public ListSubscriptionParser() {
		subscriptions = new Vector<CNSSubscriptionTest>();
		nextToken = null;
	}
	 
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		 content.reset();  
		 //System.out.println("Start Element QName:" + qName);
		 if(qName.equals("Topics")) {
			
			
		 } else if(qName.equals("member")) {
			 
		 }
	 }
	 
	 public void endElement (String uri, String localName, String qName) {
		 //System.out.println("End Element QName:" + qName);
		 if(sub == null) {
			 sub = new CNSSubscriptionTest();
		 }
		 if (qName.equals("TopicArn")) {
			 String topicArn = content.toString();
			 //System.out.println("topicArn:" + topicArn);
			 sub.setTopicArn(topicArn);
			
		 } else if (qName.equals("Protocol")) {
			 String protocol = content.toString();
			 sub.setProtocol(protocol);
			
		 } else if (qName.equals("SubscriptionArn")) {
			 String subscriptionArn = content.toString();
			 sub.setSubscriptionArn(subscriptionArn);
			
		 } else if (qName.equals("Endpoint")) {
			 String endpoint = content.toString();
			 //System.out.println("endpoint:" + endpoint);
			 sub.setEndpoint(endpoint);
			
		 } else if (qName.equals("Owner")) {
			 String owner = content.toString();
			 //System.out.println("owner:" + owner);
			 sub.setOwner(owner);
			
		 } else  if (qName.equals("member")) {
			 count++;
			 subscriptions.add(sub);
			 sub = null;
			 
		 } else if(qName.equals("Topics")) {
			//nothing
		 } else if(qName.equals("NextToken")) {
				nextToken = content.toString();
			 }
		 content.reset();  
	 }
	 
	 public void characters( char[] ch, int start, int length ) {
		 //System.out.println("Characters");
		 content.write( ch, start, length );
	 }
	 
	 public Vector<CNSSubscriptionTest> getSubscriptions() {
		 return subscriptions;
	 }
	 
	 public String getRequestId() {
		 return requestId;
	 }
	 
	 public int getCount() {
		 return count;
	 }
	 
	 public String getNextToken() {
		 return nextToken;
	 }
}



