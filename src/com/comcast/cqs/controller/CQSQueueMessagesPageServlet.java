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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
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
		Message receivedMessage=null;
		int shard = 0;
		
		if (request.getParameter("shard") != null) {
			shard = Integer.parseInt(request.getParameter("shard"));
		}

		String queueUrl = Util.getAbsoluteQueueUrlForName(queueName, userId);

		connect(request);
		
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
		} else if (parameters.containsKey("Receive")) {
			
			try {
			
				ReceiveMessageResult result = sqs.receiveMessage(new ReceiveMessageRequest(queueUrl));
				logger.debug("event=receive_message queue_url= " + queueUrl + " user_id=" + userId);
				List<Message> receivedMessages=null;
				
				if (result!=null){
					receivedMessages=result.getMessages();
					if(receivedMessages!=null&&receivedMessages.size()>0){
						receivedMessage=receivedMessages.get(0);
					}
				}
			} catch (Exception ex) {
				logger.error("event=receive_message queue_url= " + queueUrl + " user_id=" + userId, ex);
				throw new ServletException(ex);
			}
			
		}
		
		int numberOfShards = 1;
		
		try {	
			
			GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(queueUrl);
			getQueueAttributesRequest.setAttributeNames(Arrays.asList("NumberOfShards"));
			GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(getQueueAttributesRequest);
			Map<String, String> attributes = getQueueAttributesResult.getAttributes();
        	numberOfShards = Integer.parseInt(attributes.get("NumberOfShards"));
			
		} catch (Exception ex) {
			logger.error("event=get_queue_attributes url=" + queueUrl);
		}
		
		out.println("<html>");
		
		out.println("<script type='text/javascript' language='javascript'>");
		out.println("function setVisibility(id, buttonid) {");
		out.println("if(document.getElementById(buttonid).value=='Less'){");
		out.println("document.getElementById(buttonid).value = 'More';");
		out.println("document.getElementById(id).style.display = 'none';");
		out.println("}else{");
		out.println("document.getElementById(buttonid).value = 'Less';");
		out.println("document.getElementById(id).style.display = 'inline';");
		out.println("}");
		out.println("}");
		out.println("</script>");
		
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
        out.println("<form id='frm1' action=\"/webui/cqsuser/message/?userId="+userId+"&queueName="+queueName+"\" method=POST>");
        out.println("<tr><td><textarea rows='3' cols='50' name='message'></textarea><input type='hidden' name='userId' value='"+ userId + "'></td><td valign='bottom'><input type='submit' value='Send' name='Send' /></td></tr></form>");
        out.println("<tr><td>&nbsp;</td></tr>");
        
		if (numberOfShards > 1) {
			out.println("<form id='frm2' action=\"/webui/cqsuser/message/?userId="+userId+"&queueName="+queueName+"\" method=POST>");
			out.println("<tr><td>Shard: <select name='shard' onChange='document.getElementById(\"frm2\").submit();'>");
			for (int i=0; i<numberOfShards; i++) {
				out.print("<option value='" + i + "'");
				if (shard == i) {
					out.print(" selected");
				}
				out.println(">" + i + "</option>");
			}
			out.println("</form></select></td></tr></table></p>");
		} else {
			out.println("</table></p>");
		}
		
		// for receive message
		
        out.println("<form id='formsendmessage' action=\"/webui/cqsuser/message/?userId="+userId+"&queueName="+queueName+"\" method=POST>");
		out.println("<table><tr><td><b>Receive Message:</b></td>");
        out.println("<td><input type='hidden' name='userId' value='"+ userId + "'></td><td valign='bottom'><input type='submit' value='Receive Message' name='Receive' /></td>");
        out.println("</form></tr></table>");
        
        // showing received message
        
   		out.println("<p><hr width='100%' align='left' /><p>");
   		out.println("<h3>Received Messages</h3>");

   		if (receivedMessage!=null) {
        	
			Map<String, String> attributes = receivedMessage.getAttributes();
        	
        	String timeSent = "";
        	
        	if (attributes.get("SentTimestamp") != null) {
        		try { timeSent = new Date(Long.parseLong(attributes.get("SentTimestamp"))).toString(); } catch (Exception ex) {}
        	}
        	
        	String timeReceived = "";
        	
        	if (attributes.get("ApproximateFirstReceiveTimestamp") != null) {
        		try { timeReceived = new Date(Long.parseLong(attributes.get("ApproximateFirstReceiveTimestamp"))).toString(); } catch (Exception ex) {}
        	}
        	
       		out.println("<table class = 'alternatecolortable'>");
       		out.println("<tr><th></th><th>Receipt Handle</th><th>MD5</th><th>Body</th><th>Time Sent</th><th>Time First Received (Appr.)</th><th>Receive Count (Appr.)</th><th>Sender</th><th>&nbsp;</th></tr>");
        	
        	out.println("<tr>");
        	out.println("<td>0</td>");
        	out.println("<td>" + receivedMessage.getReceiptHandle() + "</td>");
        	out.println("<td>" + receivedMessage.getMD5OfBody() + "</td>");
        	String messageBody=receivedMessage.getBody();
        	String messageBodyPart1=null;
        	String messageBodyPart2=null;
        	
        	if ((messageBody!=null) && (messageBody.length()>300)) {
        		
        		messageBodyPart1=messageBody.substring(0, 299);
        		messageBodyPart2=messageBody.substring(299);

        		out.println("<td>");
        		out.println(messageBodyPart1);
        		out.println("<div id='rdetail' style=\"display: none;\">"+messageBodyPart2+"</div>");
        		out.println("<input type=button name=type id='rbt' value='More' onclick=\"setVisibility('rdetail', 'rbt');\";> ");
        		out.println("</td>");
        	} else {
        		out.println("<td>"+ receivedMessage.getBody() + "</td>");
        	}
        	
        	out.println("<td>"+ timeSent + "</td>");
        	out.println("<td>"+ timeReceived + "</td>");
        	out.println("<td>"+ attributes.get("ApproximateReceiveCount") + "</td>");
        	out.println("<td>"+ attributes.get("SenderId") + "</td>");
		    out.println("<td><form action=\"/webui/cqsuser/message/?userId="+user.getUserId()+"&queueName="+queueName+"&receiptHandle="+receivedMessage.getReceiptHandle()+"\" method=POST><input type='submit' value='Delete' name='Delete'/><input type='hidden' name='queueUrl' value='"+ queueUrl+ "' /></form></td></tr>");
	        out.println("</table>");
		    
        } else {
        	out.println("<p><i>no messages</i></p>");
        }
		
        List<CQSMessage> availableMessages = null;
        String queueFirstMessageHandle = null;
        
		try {
			
			if (queueUrl != null) {
				
				String peekRequestUrl = cqsServiceBaseUrl + user.getUserId() + "/" + queueName + "?Action=PeekMessage&AWSAccessKeyId=" + user.getAccessKey() + "&MaxNumberOfMessages=10&Shard=" + shard;

				if (prevHandle != null) {
					peekRequestUrl += "&PreviousReceiptHandle=" + prevHandle; 
				} else if (nextHandle != null) {
					peekRequestUrl += "&NextReceiptHandle=" + nextHandle; 
				}


				AWSCredentials awsCredentials=new BasicAWSCredentials(user.getAccessKey(),user.getAccessSecret());
				String peekXml = httpPOST(cqsServiceBaseUrl, peekRequestUrl,awsCredentials);
				Element root = XmlUtil.buildDoc(peekXml);
				
				List<Element> messageElements = XmlUtil.getCurrentLevelChildNodes(XmlUtil.getCurrentLevelChildNodes(root, "ReceiveMessageResult").get(0), "Message");
				
				for (Element messageElement : messageElements) {
					
					if (availableMessages == null) {
						availableMessages = new ArrayList<CQSMessage>();
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
					
					availableMessages.add(msg);
				}
				
				//retrieve first messageId from queue. this is for pagination
				String peekRequestFirstMessageUrl = cqsServiceBaseUrl + user.getUserId() + "/" + queueName + "?Action=PeekMessage&AWSAccessKeyId=" + user.getAccessKey() + "&MaxNumberOfMessages=1&Shard=" + shard;
				String peekFirstMessageXml = httpPOST(cqsServiceBaseUrl, peekRequestFirstMessageUrl,awsCredentials);
				Element rootFirstMessage = XmlUtil.buildDoc(peekFirstMessageXml);
				
				List<Element> messageElementsForFirstMessage = XmlUtil.getCurrentLevelChildNodes(XmlUtil.getCurrentLevelChildNodes(rootFirstMessage, "ReceiveMessageResult").get(0), "Message");
				if (messageElementsForFirstMessage.size() == 1){
					queueFirstMessageHandle = XmlUtil.getCurrentLevelTextValue(messageElementsForFirstMessage.get(0), "ReceiptHandle").trim();
				}
			}
		
		} catch (Exception ex) {
			logger.error("event=peek_message queue_url=" + queueUrl, ex);
			throw new ServletException(ex);
		}

		String previousHandle = null;
		nextHandle = null;
		
		out.println("<p><hr width='100%' align='left' /><p>");
		out.println("<h3>Available Messages</h3>");

		if ((availableMessages==null) || (availableMessages.size()==0)) {
			out.println("<p><i>no messages</i></p>");
		}

		for (int i = 0; availableMessages != null && i < availableMessages.size(); i++) {
        
			CQSMessage message = availableMessages.get(i);
        	
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
        		out.println("<table class = 'alternatecolortable'>");
        		out.println("<tr><th></th><th>Receipt Handle</th><th>MD5</th><th>Body</th><th>Time Sent</th><th>Time First Received (Appr.)</th><th>Receive Count (Appr.)</th><th>Sender</th><th>&nbsp;</th></tr>");
        		previousHandle = message.getReceiptHandle();
        	}
        	
        	out.println("<tr>");
        	out.println("<td>" + i + "</td>");
        	out.println("<td>" + message.getReceiptHandle() + "</td>");
        	out.println("<td>" + message.getMD5OfBody() + "</td>");
        	String messageBody=message.getBody();
        	String messageBodyPart1=null;
        	String messageBodyPart2=null;
        	if((messageBody!=null)&&(messageBody.length()>300)){
        		messageBodyPart1=messageBody.substring(0, 299);
        		messageBodyPart2=messageBody.substring(299);

        		out.println("<td>");
        		out.println(messageBodyPart1);
        		out.println("<div id='detail"+i+"' style=\"display: none;\">"+messageBodyPart2+"</div>");
        		out.println("<input type=button name=type id='bt"+i+"' value='More' onclick=\"setVisibility('detail"+ i+"', 'bt"+i+"');\";> ");
        		out.println("</td>");
        	} else {
        		out.println("<td>"+ message.getBody() + "</td>");
        	}
        	out.println("<td>"+ timeSent + "</td>");
        	out.println("<td>"+ timeReceived + "</td>");
        	out.println("<td>"+ attributes.get("ApproximateReceiveCount") + "</td>");
        	out.println("<td>"+ attributes.get("SenderId") + "</td>");
		    out.println("<td></td></tr>");
		    
		    if (i == availableMessages.size() - 1) {
		    	nextHandle = message.getReceiptHandle();
		    }
        }
		
        out.println("</table>");
        
        //This case is for click on "Next" button of the previous page
        if (prevHandle != null) { 
        	
        	if ((previousHandle) != null) {
        		out.println("<a style='float:left;' href='/webui/cqsuser/message/?userId="+user.getUserId()+"&queueName="+queueName+"&nextHandle="+previousHandle+"'>Prev</a>");
        	} else {
        		out.println("<a style='float:left;' href='javascript:history.back()'>Prev</a>");
        	}
        } else if ( (previousHandle != null) && (!previousHandle.equals(queueFirstMessageHandle))){ //this is for all other cases
        	out.println("<a style='float:left;' href='/webui/cqsuser/message/?userId="+user.getUserId()+"&queueName="+queueName+"&nextHandle="+previousHandle+"'>Prev</a>");
        }
        
        if (availableMessages != null && availableMessages.size() > 0) {
        	out.println("<a style='float:right;' href='/webui/cqsuser/message/?userId="+user.getUserId()+"&queueName="+queueName+"&prevHandle="+nextHandle+"'>Next</a>");
        }
        
        out.println("<h5 style='text-align:center;'><a href='/webui'>ADMIN HOME</a>");
        out.println("<a href='/webui/cqsuser?userId="+userId+"'>BACK TO QUEUE</a></h5>");
        
        out.println("</body></html>");

        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
