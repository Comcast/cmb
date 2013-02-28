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
package com.comcast.cqs.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.XmlUtil;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.util.Util;

/**
 * Admin page for showing messages in a queue
 * @author aseem, vvenkatraman, bwolf, baosen
 *
 */
public class CQSQueueMessagesPageServlet extends AdminServletBase {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CQSQueueMessagesPageServlet.class);
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String userId = request.getParameter("userId");
		String queueName = request.getParameter("queueName");
		String msgStr = request.getParameter("message");
		String prevHandle = request.getParameter("prevHandle");
		String nextHandle = request.getParameter("nextHandle");
		String receiptHandle = request.getParameter("receiptHandle");
		Map<?, ?> parameters = request.getParameterMap();

		String queueUrl = Util.getAbsoluteQueueUrlForName(queueName, userId);

		connect(userId);
		
		if (parameters.containsKey("Send")) {
			
			try {
			
				SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, msgStr);
				sqs.sendMessage(sendMessageRequest);
				logger.debug("event=send_message queue_url= " + queueUrl + " user_id=" + userId);
			
			} catch (Exception ex) {
				logger.error("event=send_message queue_url= " + queueUrl + " user_id=" + userId, ex);
				throw new ServletException(ex);
			}
			
		} else if (parameters.containsKey("Delete")) {
			
			try {
				
				DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, receiptHandle);
				sqs.deleteMessage(deleteMessageRequest);
				logger.debug("event=delete_message queue_url= " + queueUrl + " receipt_handle=" + receiptHandle);
			
			} catch (Exception ex) {
				logger.error("event=delete_message queue_url= " + queueUrl + " receipt_handle=" + receiptHandle, ex);
				throw new ServletException(ex);
			}
		}
		
		out.println("<html>");
		
		header(request, out, "Peek Messages for Queue " + queueName);
		
		out.println("<body>");
		out.println("<h2>Peek Messages for Queue " + queueName + "</h2>");
		
		if (user != null) {
			out.println("<table><tr><td><b>User Name:</b></td><td>"+ user.getUserName()+"</td></tr>");
			out.println("<tr><td><b>User ID:</b></td><td>"+ user.getUserId()+"</td></tr>");
			out.println("<tr><td><b>Access Key:</b></td><td>"+user.getAccessKey()+"</td></tr>");
			out.println("<tr><td><b>Access Secret:</b></td><td>"+user.getAccessSecret()+"</td></tr>");
			out.println("<tr><td><b>Queue Name:</b></td><td>"+queueName+"</td></tr></table>");
		}
		
        out.println("<p><table><tr><td><b>Send message:</b></td><td></td></tr>");
        out.println("<form action=\""+response.encodeURL("MESSAGE") + "?userId="+userId+"&queueName="+queueName+"\" method=POST>");
        out.println("<tr><td><textarea rows='3' cols='50' name='message'></textarea><input type='hidden' name='userId' value='"+ userId + "'></td><td valign='bottom'><input type='submit' value='Send' name='Send' /></td></tr></form></table></p>");

        List<CQSMessage> messages = null;
        
		try {
			
			if (queueUrl != null) {
				
				String peekRequestUrl = cqsServiceBaseUrl + user.getUserId() + "/" + queueName + "?Action=PeekMessage&AWSAccessKeyId=" + user.getAccessKey() + "&MaxNumberOfMessages=10";

				if (prevHandle != null) {
					peekRequestUrl += "&PreviousReceiptHandle=" + prevHandle; 
				} else if (nextHandle != null) {
					peekRequestUrl += "&NextReceiptHandle=" + nextHandle; 
				}

				String peekXml = httpGet(peekRequestUrl);
				Element root = XmlUtil.buildDoc(peekXml);
				
				List<Element> messageElements = XmlUtil.getCurrentLevelChildNodes(XmlUtil.getCurrentLevelChildNodes(root, "ReceiveMessageResult").get(0), "Message");
				
				for (Element messageElement : messageElements) {
					
					if (messages == null) {
						messages = new ArrayList<CQSMessage>();
					}
					
					String body = XmlUtil.getCurrentLevelTextValue(messageElement, "Body");
					String id = XmlUtil.getCurrentLevelTextValue(messageElement, "MessageId").trim();
					String handle = XmlUtil.getCurrentLevelTextValue(messageElement, "ReceiptHandle").trim();
					
					CQSMessage msg = new CQSMessage(id, body);
					msg.setReceiptHandle(handle);
					
					List<Element> attributeElements = XmlUtil.getCurrentLevelChildNodes(messageElement, "Attribute");
					Map<String, String> attributes = new HashMap<String, String>();
					
					
					for (Element attribute : attributeElements) {
						
						String name = XmlUtil.getCurrentLevelTextValue(attribute, "Name");
						String value = XmlUtil.getCurrentLevelTextValue(attribute, "Value");
						
						if (name != null && value != null) {
							attributes.put(name, value);
						}
					}
					
					msg.setAttributes(attributes);
					
					messages.add(msg);
				}
			}
		
		} catch (Exception ex) {
			logger.error("event=peek_message queue_url=" + queueUrl, ex);
			throw new ServletException(ex);
		}

		String previousHandle = null;
		nextHandle = null;
        
		for (int i = 0; messages != null && i < messages.size(); i++) {
        
			CQSMessage message = messages.get(i);
        	
			Map<String, String> attributes = message.getAttributes();
        	
        	String timeSent = "";
        	
        	if (attributes.get("SentTimestamp") != null) {
        		try { timeSent = new Date(Long.parseLong(attributes.get("SentTimestamp"))).toString(); } catch (Exception ex) {}
        	}
        	
        	String timeReceived = "";
        	
        	if (attributes.get("ApproximateFirstReceiveTimestamp") != null) {
        		try { timeReceived = new Date(Long.parseLong(attributes.get("ApproximateFirstReceiveTimestamp"))).toString(); } catch (Exception ex) {}
        	}
        	
        	if (i == 0) {
        		out.println("<p><hr width='100%' align='left' /><p><span class='content'><table>");
        		out.println("<tr><th></th><th>Receipt Handle</th><th>MD5</th><th>Body</th><th>Time Sent</th><th>Time First Received (Appr.)</th><th>Receive Count (Appr.)</th><th>Sender</th><th>&nbsp;</th></tr>");
        		previousHandle = message.getReceiptHandle();
        	}
        	
        	out.println("<tr>");
        	out.println("<td>" + i + "</td>");
        	out.println("<td>" + message.getReceiptHandle() + "</td>");
        	out.println("<td>" + message.getMD5OfBody() + "</td>");
        	out.println("<td>" + message.getBody() + "</td>");
        	out.println("<td>"+ timeSent + "</td>");
        	out.println("<td>"+ timeReceived + "</td>");
        	out.println("<td>"+ attributes.get("ApproximateReceiveCount") + "</td>");
        	out.println("<td>"+ attributes.get("SenderId") + "</td>");
		    out.println("<td><form action=\"MESSAGE?userId="+user.getUserId()+"&queueName="+queueName+"&receiptHandle="+message.getReceiptHandle()+"\" method=POST><input type='submit' value='Delete' name='Delete'/><input type='hidden' name='queueUrl' value='"+ queueUrl+ "' /></form></td></tr>");
		    
		    if (i == messages.size() - 1) {
		    	nextHandle = message.getReceiptHandle();
		    }
        }
		
        out.println("</table></span>");
        
        if (prevHandle != null) {
        	
        	if (previousHandle != null) {
        		out.println("<a style='float:left;' href='" + response.encodeURL("MESSAGE") + "?userId="+user.getUserId()+"&queueName="+queueName+"&nextHandle="+previousHandle+"'>Prev</a>");
        	} else {
        		out.println("<a style='float:left;' href='javascript:history.back()'>Prev</a>");
        	}
        }
        
        if (messages != null && messages.size() > 0) {
        	out.println("<a style='float:right;' href='" + response.encodeURL("MESSAGE") + "?userId="+user.getUserId()+"&queueName="+queueName+"&prevHandle="+nextHandle+"'>Next</a>");
        }
        
        out.println("<h5 style='text-align:center;'><a href='/ADMIN'>ADMIN HOME</a>");
        out.println("<a href='/CQSUser?userId="+userId+"'>BACK TO QUEUE</a></h5>");
        
        out.println("</body></html>");

        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
