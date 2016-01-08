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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONWriter;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.controller.CNSControllerServlet;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.persistence.CNSSubscriptionCassandraPersistence;
import com.comcast.cns.persistence.ICNSSubscriptionPersistence;
import com.comcast.cns.test.unit.CNSSubscriptionTest;
import com.comcast.cqs.controller.CQSControllerServlet;

public class CNSTestingUtils {

	private static Logger logger = Logger.getLogger(CNSTestingUtils.class);

	public static List<CNSSubscription> confirmPendingSubscriptionsByTopic(String topicArn, String userId, CnsSubscriptionProtocol protocol) throws Exception {

		String nextToken = null;
		int round = 0;

		List<CNSSubscription> confirmedSubscriptions = new ArrayList<CNSSubscription>();
		ICNSSubscriptionPersistence subscriptionHandler = new CNSSubscriptionCassandraPersistence();

		while (round == 0 || nextToken != null) {

			List<CNSSubscription> subscriptions = subscriptionHandler.listAllSubscriptionsByTopic(nextToken, topicArn, protocol);

			if (subscriptions.size() > 0) {
				nextToken = subscriptions.get(subscriptions.size()-1).getArn();
			} else {
				nextToken = null;
			}

			for (CNSSubscription s : subscriptions) {

				if (!s.isConfirmed()) {
					CNSSubscription confirmedSubscription = subscriptionHandler.confirmSubscription(false, s.getToken(), topicArn);
					confirmedSubscriptions.add(confirmedSubscription);
				}
			}

			round++;
		}

		return confirmedSubscriptions;
	}

	public static Vector<String> getArnsFromString(String res) {
		//System.out.println("Starting parsing: res is:" + res);
		javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		javax.xml.parsers.SAXParser saxParser;

		ListTopicsResponseParser pl = new ListTopicsResponseParser();

		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(res.getBytes()), pl);

		} catch (Exception ex) {
			logger.error("Exception parsing", ex);
		}
		return pl.getArns();
	}

	public static ListSubscriptionParser getSubscriptionDataFromString(String res) {
		//System.out.println("Starting parsing: res is:" + res);
		javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		javax.xml.parsers.SAXParser saxParser;

		ListSubscriptionParser pl = new ListSubscriptionParser();

		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(res.getBytes()), pl);

		} catch (Exception ex) {

			logger.error("Exception parsing", ex);
		}
		return pl;
	}

	public static Vector<CNSSubscriptionTest> getSubscriptionsFromString(String res) {
		ListSubscriptionParser pl = getSubscriptionDataFromString(res);
		return pl.getSubscriptions();
	}

	public static String getSubscriptionArnFromString(String res) {
		javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		javax.xml.parsers.SAXParser saxParser;

		SubscribeParser p = new SubscribeParser();

		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(res.getBytes()), p);

		} catch (Exception ex) {
			logger.error("Exception parsing", ex);
		}
		String arn = p.getSubscriptionArn();
		return arn;
	}

	public static SubscriptionAttributeParser getSubscriptionAttributesFromString(String res) {
		javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		javax.xml.parsers.SAXParser saxParser;

		SubscriptionAttributeParser p = new SubscriptionAttributeParser();

		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(res.getBytes()), p);

		} catch (Exception ex) {
			logger.error("Exception parsing", ex);
		}

		return p;
	}

	public static TopicAttributeParser getTopicAttributesFromString(String res) {
		javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		javax.xml.parsers.SAXParser saxParser;

		TopicAttributeParser p = new TopicAttributeParser();

		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(res.getBytes()), p);

		} catch (Exception ex) {
			logger.error("Exception parsing", ex);
		}

		return p;
	}

	public static String getArnFromString(String res) {
		javax.xml.parsers.SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		javax.xml.parsers.SAXParser saxParser;

		CreateTopicResponseParser p = new CreateTopicResponseParser();

		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(res.getBytes()), p);

		} catch (Exception ex) {
			logger.error("Exception parsing", ex);

		}
		String arn = p.getTopicArn();
		return arn;
	}

	public static boolean verifyErrorResponse(String res, String code, String message) {
		
		SAXParserFactory fac = new org.apache.xerces.jaxp.SAXParserFactoryImpl();
		SAXParser saxParser;

		ErrorParser p = new ErrorParser();
		res = res.trim();

		try {
			saxParser = fac.newSAXParser();
			saxParser.parse(new ByteArrayInputStream(res.getBytes()), p);
		} catch (Exception ex) {
			logger.error("Exception parsing error response", ex);
		}

		if (code != null) {
			
			String rescode = p.getCode();
			
			if (!code.equals(rescode)) {
				logger.error("Wrong error code");
				return false;
			}
		}
		
		if (message != null) {
			
			String resmessage = p.getMessage();
			
			if (!resmessage.equals(message)) {
				logger.error("Wrong error messahe");
				return false;
			}
		}
		
		return true;
	}

	public static void addParam(Map<String, String[]> params, String name, String val) {        
		String[] paramVals = new String[1];
		paramVals[0] = val;
		params.put(name, paramVals);        
	}

	public static void addTopic(CNSControllerServlet cns, User user, OutputStream out, String topicName) throws Exception{		
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params,"Action", "CreateTopic");
		if(topicName != null) addParam(params, "Name", topicName);
		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();

		response.setOutputStream(out);
		cns.doGet(request, response);		
		response.getWriter().flush();
	}

	public static void addPermission(CNSControllerServlet cns, User user, OutputStream out, String topicArn, String account, String permission, String label) throws Exception{		

	    SimpleHttpServletRequest request = new SimpleHttpServletRequest();
	    Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params,"Action", "AddPermission");
		addParam(params, "TopicArn", topicArn);
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		addParam(params, "AWSAccountId.member.1", account);
		addParam(params, "ActionName.member.1", permission);
		addParam(params, "Label", label);

		request.setParameterMap(params);

		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		response.setOutputStream(out);
		cns.doGet(request, response);		
		response.getWriter().flush();
	}


	public static void listTopics(CNSControllerServlet cns, User user, OutputStream out, String token) throws Exception{
		HttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params,"Action", "ListTopics");
		if(token!= null) addParam(params,"NextToken", token);
		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();
		logger.debug("Response: " + out.toString());
	}

	public static void testCommand(CNSControllerServlet cns, User user, OutputStream out, String command) throws Exception{
		HttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params,"Action", command);

		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();
		//System.out.println("Out:" + out.toString());
	}

	public static void confirmSubscription(CNSControllerServlet cns, User user, OutputStream out, String topicArn, String token, String authOnSub) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		//System.out.println("Deleting Topic arn is:" + arn);
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params,"Action", "ConfirmSubscription");
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		if(topicArn != null) addParam(params,"TopicArn", topicArn);
		if(token != null) addParam(params,"Token", token);
		if(authOnSub != null) addParam(params,"AuthenticateOnUnsubscribe", authOnSub);

		request.setParameterMap(params);			
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		response.setOutputStream(out);
		logger.debug("confirmSubscription");
		cns.doGet(request,response);	
		//System.out.println("Done Delete Topic Arn:" + arn);

		response.getWriter().flush();
		logger.debug("confirmSubscription Out:" + out.toString());

	}

	public static void deleteTopic(CNSControllerServlet cns, User user, OutputStream out, String arn) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		//System.out.println("Deleting Topic arn is:" + arn);
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params,"Action", "DeleteTopic");
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		addParam(params,"TopicArn", arn);

		request.setParameterMap(params);			
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		response.setOutputStream(out);
		//System.out.println("Do Delete Topic Arn:" + arn);
		cns.doGet(request,response);	
		//System.out.println("Done Delete Topic Arn:" + arn);

		response.getWriter().flush();
		//System.out.println("Out:" + out.toString());

	}

	public static void deleteallSubscriptions(CNSControllerServlet cns, User user, OutputStream out) {
		//Get all topics

		try {
			listSubscriptions(cns,user,out, null);
		} catch (Exception e) {
			logger.error("Exception", e);
			assertTrue(false);
		}
		String res = out.toString();
		Vector<CNSSubscriptionTest> subs = CNSTestingUtils.getSubscriptionsFromString(res);

		//Delete all topics
		//System.out.println("Deleting arns size:" + arns.size());

		try {
			for(CNSSubscriptionTest sub:subs) {
				//System.out.println("Deleting arn:" + arn);
				String subscriptionArn = sub.getSubscriptionArn();
				if(!subscriptionArn.equals("PendingConfirmation")) {
					CNSTestingUtils.unSubscribe(cns, user, out, subscriptionArn);
				}

			}
		} catch (Exception e) {
			logger.error("Exception", e);
			assertFalse(true);
		}
	}

	public static void deleteallTopics(CNSControllerServlet cns, User user, OutputStream out) {
		//Get all topics

		try {
			listTopics(cns,user,out,null);
		} catch (Exception e) {
			logger.error("Exception", e);
			assertTrue(false);
		}
		String res = out.toString();
		Vector<String> arns = getArnsFromString(res);

		//Delete all topics
		//System.out.println("Deleting arns size:" + arns.size());

		try {
			for(String arn:arns) {
				//System.out.println("Deleting arn:" + arn);
				deleteTopic(cns,user,out,arn);

			}
		} catch (Exception e) {
			logger.error("Exception", e);
			assertFalse(true);
		}

	}

	public static void subscribe(CNSControllerServlet cns, User user, OutputStream out, String endpoint, String protocol, String arn) throws Exception {
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		//System.out.println("Deleting Topic arn is:" + arn);
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params,"Action", "Subscribe");
		addParam(params,"AWSAccessKeyId", user.getAccessKey());
		if(endpoint !=null)	addParam(params,"Endpoint", endpoint);
		if(protocol !=null)	addParam(params,"Protocol", protocol);
		if(arn !=null)	addParam(params,"TopicArn", arn);


		request.setParameterMap(params);			
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		response.setOutputStream(out);
		//System.out.println("Do Delete Topic Arn:" + arn);
		cns.doGet(request,response);	
		//System.out.println("Done Delete Topic Arn:" + arn);

		response.getWriter().flush();
	}

	public static void unSubscribe(CNSControllerServlet cns, User user, OutputStream out, String subscriptionArn) throws Exception {
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		//System.out.println("Deleting Topic arn is:" + arn);
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params,"Action", "Unsubscribe");
		addParam(params,"AWSAccessKeyId", user.getAccessKey());
		if(subscriptionArn != null) addParam(params,"SubscriptionArn", subscriptionArn);


		request.setParameterMap(params);			
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		response.setOutputStream(out);
		//System.out.println("Do Delete Topic Arn:" + arn);
		cns.doGet(request,response);	
		//System.out.println("Done Delete Topic Arn:" + arn);

		response.getWriter().flush();
	}

	public static String getQueueUrl(String response) {
		int beginLoc = response.indexOf("<QueueUrl>");
		int endLoc = response.indexOf("</QueueUrl>");

		return response.substring(beginLoc + 10, endLoc);
	}

	public static Vector<String> getQueueUrlfromList(String response) {
		Vector<String> res = new Vector<String>();
		int beginLoc = response.indexOf("<QueueUrl>");
		int endLoc = response.indexOf("</QueueUrl>");
		while(beginLoc != -1) {
			res.add(response.substring(beginLoc + 10, endLoc));
			beginLoc = response.indexOf("<QueueUrl>", endLoc);
			if(beginLoc == -1) break;
			endLoc = response.indexOf("</QueueUrl>", beginLoc);
		}
		return res;
	}

	public static String getReceiptHandle(String response) {
		int beginLoc = response.indexOf("<ReceiptHandle>");
		int endLoc = response.indexOf("</ReceiptHandle>");
		if(beginLoc != -1)
			return response.substring(beginLoc + 15, endLoc);
		else 
			return null;
	}

	/*
	 * Get the body of the message from the CQS response
	 * @param response, the response from receiveMessage on CQS
	 * @return the content inside the Body tag.
	 */
	public static String getMessage(String response) {

		int beginLoc = response.indexOf("<Body>");
		int endLoc = response.indexOf("</Body>");

		if (beginLoc != -1) {
			return StringEscapeUtils.unescapeXml(response.substring(beginLoc + 6, endLoc));
		} else { 
			return null;
		}
	}

	public static String receiveMessage(CQSControllerServlet cqs, User user, String queueURL, String numberOfMessages) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();

		request.setRequestUrl(queueURL);
		//System.out.println("Queue URL: " + queueURL);
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params, "Action", "ReceiveMessage");
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		addParam(params, "MaxNumberOfMessages", numberOfMessages);

		request.setParameterMap(params);
		logger.debug("Sending request:" + request.toString());
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();

		OutputStream out = new ByteArrayOutputStream();

		response.setOutputStream(out);

		cqs.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}


	public static String setPermissionForUser(CQSControllerServlet cqs, User user, User user2, String queueURL) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();

		request.setRequestUrl(queueURL);
		//System.out.println("Queue URL: " + queueURL);
		Map<String, String[]> params = new HashMap<String, String[]>();


		addParam(params, "Action", "AddPermission");
		addParam(params, "Label", "testLabel");
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		addParam(params, "AWSAccountId.1", user2.getUserId());
		addParam(params, "ActionName.1", "SendMessage");

		request.setParameterMap(params);
		logger.debug("Sending request:" + request.toString());
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();

		OutputStream out = new ByteArrayOutputStream();

		response.setOutputStream(out);

		cqs.doGet(request, response);
		response.getWriter().flush();
		logger.debug("Add Permissions Response:" + out.toString());
		return out.toString();
	}

	public static String deleteMessage(CQSControllerServlet cqs, User user, String queueURL, String receiptHandle) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();

		request.setRequestUrl(queueURL);
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params, "Action", "DeleteMessage");
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		addParam(params, "ReceiptHandle", receiptHandle);

		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();

		OutputStream out = new ByteArrayOutputStream();

		response.setOutputStream(out);

		cqs.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}


	public static void listSubscriptions(CNSControllerServlet cns, User user, OutputStream out, String nextToken) throws Exception{
		HttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params,"Action", "ListSubscriptions");
		if(nextToken != null) {
			addParam(params,"NextToken", nextToken);
		}
		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();
		//System.out.println("Out:" + out.toString());
	}

	public static void listSubscriptionsByTopic(CNSControllerServlet cns, User user, OutputStream out, String topicArn, String nextToken) throws Exception{
		HttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params,"Action", "ListSubscriptionsByTopic");
		addParam(params, "TopicArn", topicArn);
		if(nextToken != null) {
			addParam(params,"NextToken", nextToken);
		}
		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();
		//System.out.println("Out:" + out.toString());
	}


	public static String listQueues(CQSControllerServlet cqs, User user) throws Exception {
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		request.setRequestUrl(CMBProperties.getInstance().getCQSServiceUrl());
		addParam(params, "Action", "ListQueues");
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		addParam(params, "QueueNamePrefix", "");


		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();

		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);

		cqs.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}


	public static String deleteQueue(CQSControllerServlet cqs, User user, String qUrl) throws Exception {
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		request.setRequestUrl(qUrl);

		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "Action", "DeleteQueue");
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		request.setParameterMap(params);

		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);

		cqs.doGet(request, response);

		response.getWriter().flush();

		return out.toString();
	}

	public static String addQueue(CQSControllerServlet cqs, User user, String qName) throws Exception {

		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		request.setRequestUrl(CMBProperties.getInstance().getCQSServiceUrl());
		Map<String, String[]> params = new HashMap<String, String[]>();


		addParam(params, "Action", "CreateQueue");
		addParam(params, "AWSAccessKeyId", user.getAccessKey());
		addParam(params, "QueueName", qName);

		addParam(params, "Attribute.1.Name", "VisibilityTimeout");
		addParam(params, "Attribute.1.Value", "30");
		addParam(params, "Attribute.2.Name", "Policy");
		addParam(params, "Attribute.2.Value", "");
		addParam(params, "Attribute.3.Name", "MaximumMessageSize");
		addParam(params, "Attribute.3.Value", "65536");
		addParam(params, "Attribute.4.Name", "MessageRetentionPeriod");
		addParam(params, "Attribute.4.Value", "600");
		addParam(params, "Attribute.5.Name", "DelaySeconds");
		addParam(params, "Attribute.5.Value", "0");

		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();

		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);

		cqs.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}

	public static String doPublish(CNSControllerServlet cns, User user, OutputStream out, String message, String messageStructure, String subject, String topicArn) throws Exception{
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params, "Action", "Publish");
		if(message != null) {
			addParam(params, "Message", message);
		}
		if(messageStructure != null) {
			addParam(params, "MessageStructure", messageStructure);
		}
		if(subject != null) {
			addParam(params, "Subject", subject);
		}
		if(topicArn != null) {
			addParam(params, "TopicArn", topicArn);
		}
		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		request.setParameterMap(params);
		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}

	public static String doGetSubscriptionAttributes(CNSControllerServlet cns, User user, OutputStream out, String subscriptionArn) throws Exception{
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params, "Action", "GetSubscriptionAttributes");
		if(subscriptionArn != null) {
			addParam(params, "SubscriptionArn", subscriptionArn);
		}

		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		request.setParameterMap(params);
		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}

	public static String doSetSubscriptionAttributes(CNSControllerServlet cns, User user, OutputStream out, String attributeName, String attributeValue, String subscriptionArn) throws Exception{
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params, "Action", "SetSubscriptionAttributes");
		if(subscriptionArn != null) {
			addParam(params, "SubscriptionArn", subscriptionArn);
		}
		if(attributeName != null) {
			addParam(params, "AttributeName", attributeName);
		}

		if(attributeValue != null) {
			addParam(params, "AttributeValue", attributeValue);
		}

		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		request.setParameterMap(params);
		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}


	public static String doGetTopicAttributes(CNSControllerServlet cns, User user, OutputStream out, String topicArn) throws Exception{
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params, "Action", "GetTopicAttributes");
		if(topicArn != null) {
			addParam(params, "TopicArn", topicArn);
		}

		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		request.setParameterMap(params);
		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}

	public static String doSetTopicAttributes(CNSControllerServlet cns, User user, OutputStream out, String attributeName, String attributeValue, String topicArn) throws Exception{
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();

		addParam(params, "Action", "SetTopicAttributes");
		if(topicArn != null) {
			addParam(params, "TopicArn", topicArn);
		}
		if(attributeName != null) {
			addParam(params, "AttributeName", attributeName);
		}

		if(attributeValue != null) {
			addParam(params, "AttributeValue", attributeValue);
		}

		addParam(params, "AWSAccessKeyId", user.getAccessKey());

		request.setParameterMap(params);
		((SimpleHttpServletRequest) request).setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();	
		response.setOutputStream(out);

		cns.doGet(request, response);
		response.getWriter().flush();

		return out.toString();
	}


	/*
	 * (non-Javadoc)
	 * @see com.comcast.cns.io.IEndpointPublisher#send()
	 * We will send the message using the URL and HttpUrlConnection classes
	 */
	public static String sendHttpMessage(String endpoint, String message) throws Exception {

		if ((message == null) || (endpoint == null)) {
			throw new Exception("Message and Endpoint must both be set");
		}

		String newPostBody = message;
		byte newPostBodyBytes[] = newPostBody.getBytes();

		URL url = new URL(endpoint);

		logger.info(">> "+url.toString());

		HttpURLConnection con = (HttpURLConnection)url.openConnection();

		con.setRequestMethod("GET"); // POST no matter what
		con.setDoOutput(true);
		con.setDoInput(true);
		con.setFollowRedirects(false);
		con.setUseCaches(true);

		logger.info(">> " + "GET");

		//con.setRequestProperty("content-length", newPostBody.length() + "");
		con.setRequestProperty("host", url.getHost());

		con.connect();

		logger.info(">> " + newPostBody);

		int statusCode = con.getResponseCode();

		BufferedInputStream responseStream;

		logger.info("StatusCode:" + statusCode);

		if (statusCode != 200 && statusCode != 201) {

			responseStream = new BufferedInputStream(con.getErrorStream());
		} else {
			responseStream = new BufferedInputStream(con.getInputStream());
		}

		int b;
		String response = "";

		while ((b = responseStream.read()) != -1) {
			response += (char)b;
		}

		logger.info("response:" + response);
		return response;
	}

	public static String getBodyFromHTML(String response) {
		int beginLoc = response.indexOf("<body>");
		int endLoc = response.indexOf("</body>");
		if(beginLoc != -1)
			return response.substring(beginLoc + 6, endLoc).trim();
		else 
			return null;
	}

	/*
	 * Generate the Json string with the message in token
	 * @param message The message you want to encode
	 * @return the Json String 
	 */
	public static String generateMessageJson(String message) {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Writer writer = new PrintWriter(out); 
		JSONWriter jw = new JSONWriter(writer);

		try {
			jw = jw.object();	    	
			jw.key("Token").value(message);
			jw.endObject();	    	    	
			writer.flush();

		} catch(Exception e) {
			return "";
		}   	    	
		return out.toString();
	}

	/**
	 * Generate the Json string for sending different messages to different endpoitns
	 * @param emailMessage  The message to send to the email endpoints
	 * @param emailJson   The message to send to the email-json endpoints
	 * @param defaultMessage   The default message to send to the endpoints if there is no other specified
	 * @param httpMessage   The message to send to the http endpoints
	 * @param httpsMessage   The message to send to the https endpoints
	 * @param cqsMessage   The message to send to the cqs endpoints
	 * @return the JSON string to send to the publish method 
	 */
	public static String generateMultiendpointMessageJson(String emailMessage, String emailJson, String defaultMessage, String httpMessage, String httpsMessage, String cqsMessage) {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Writer writer = new PrintWriter(out); 
		JSONWriter jw = new JSONWriter(writer);

		try {
			jw = jw.object();
			if(defaultMessage != null) {
				jw.key("default").value(defaultMessage);
			}
			if(emailMessage != null) {
				jw.key("email").value(emailMessage);
			}
			if(emailJson != null) {
				jw.key("email-json").value(emailJson);
			}
			if(httpMessage != null) {
				jw.key("http").value(httpMessage);
			}
			if(httpsMessage != null) {
				jw.key("https").value(httpsMessage);
			}
			if(cqsMessage != null) {
				jw.key("cqs").value(cqsMessage);
			}
			jw.endObject();	    	    	
			writer.flush();

		} catch(Exception e) {
			return "";
		}   	    	
		return out.toString();
	}
}
