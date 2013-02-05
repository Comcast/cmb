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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.comcast.cmb.common.controller.AdminServlet;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.util.Util;

/**
 * Admin page for cqs users
 * @author bwolf, tina, baosen, vvenkatraman
 *
 */
public class CQSUserPageServlet extends AdminServletBase {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CQSUserPageServlet.class);
	private String userId;
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		if (!CMBProperties.getInstance().getCQSServiceEnabled()) {
			throw new ServletException("CQS service disabled");
		}
		
		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		
		Map<?, ?> parameters = request.getParameterMap();
		userId = request.getParameter("userId");
		String queueName = request.getParameter("queueName");
		String queueUrl = request.getParameter("qUrl");
		
        List<String> queueUrls = new ArrayList<String>();
        
        boolean showQueueAttributes = false;
		boolean useQueueNamePrefix = false;
        
		connect(userId);
		
		if (parameters.containsKey("Create")) {
			
			try {
				
				CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
				CreateQueueResult createQueueResult = sqs.createQueue(createQueueRequest);
				
				queueUrl = createQueueResult.getQueueUrl();
				
				logger.debug("event=created_queue url=" + queueUrl);

			} catch (Exception ex) {
				logger.error("event=createQueue status=failed userId= " + userId, ex);
				throw new ServletException(ex);
			}
		
		} else if (parameters.containsKey("Search")) {
			
			useQueueNamePrefix = true; 
			
			if (request.getParameter("ShowAttributes") != null) {
				showQueueAttributes = true;
			}

		} else if (parameters.containsKey("Delete")) {
			
			try {
				DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(queueUrl);
				sqs.deleteQueue(deleteQueueRequest);
				logger.debug("event=deleted_queue url=" + queueUrl);
			} catch (Exception ex) {
				logger.error("event=deletedQueue status=failed userId= " + userId, ex);
				throw new ServletException(ex);
			}

		} else if (parameters.containsKey("DeleteAll")) {
			
	        try {
	        	
	 			ListQueuesRequest listQueuesRequest = new ListQueuesRequest();

	 			if (queueName != null && !queueName.equals("")) {
					listQueuesRequest.setQueueNamePrefix(queueName);
				}
				
	 			ListQueuesResult listQueuesResult = sqs.listQueues(listQueuesRequest);
				queueUrls = listQueuesResult.getQueueUrls();
				logger.debug("event=list_queues count=" + queueUrls != null ? queueUrls.size() : 0);

	        } catch (Exception ex) {
				logger.error("event=list_queues status=failed userId= " + userId, ex);
				throw new ServletException(ex);
			}

			for (int i = 0; queueUrls != null && i < queueUrls.size(); i++) {

				try {
					DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(queueUrls.get(i));
					sqs.deleteQueue(deleteQueueRequest);
					logger.debug("event=deleted_queue url=" + queueUrl);
				} catch (Exception ex) {
					logger.error("event=deletedQueue status=failed userId= " + userId, ex);
				}
			}			
		}
		
		out.println("<html>");
		
		header(request, out, "Queues");
		
		out.println("<body>");

		out.println("<h2>Queues</h2>");

		if (user != null) {
			out.println("<table><tr><td><b>User Name:</b></td><td>"+ user.getUserName()+"</td></tr>");
			out.println("<tr><td><b>User ID:</b></td><td>"+ user.getUserId()+"</td></tr>");
			out.println("<tr><td><b>Access Key:</b></td><td>"+user.getAccessKey()+"</td></tr>");
			out.println("<tr><td><b>Access Secret:</b></td><td>"+user.getAccessSecret()+"</td></tr></table>");
		}
        
		out.println("<p><table>");
		
		out.println("<tr><td>Search queues with name prefix:</td><td></td></tr>");
        out.println("<tr><form action=\"/CQSUser?userId="+user.getUserId() + "\" " + "method=POST>");
        out.println("<td><input type='text' name='queueName' value='"+(useQueueNamePrefix?queueName:"")+"'/><input type='hidden' name='userId' value='"+ userId + "'/><input type='checkbox' name='ShowAttributes' value='ShowAttributes'>Show Attributes</input></td><td><input type='submit' value='Search' name='Search' /></td></form></tr>");

        out.println("<tr><td>Create queue with name:</td><td></td></tr>");
        out.println("<tr><form action=\"/CQSUser?userId="+user.getUserId() + "\" " + "method=POST>");
        out.println("<td><input type='text' name='queueName' /><input type='hidden' name='userId' value='"+ userId + "'></td><td><input type='submit' value='Create' name='Create' /></td></form></tr>");

        out.println("<tr><td>Delete all queues:</td><td></td></tr>");
        out.println("<tr><form action=\"/CQSUser?userId="+user.getUserId() + "\" " + "method=POST><td><input type='hidden' name='userId' value='"+ userId + "'/><input type='hidden' name='queueName' value='"+(queueName != null ? queueName : "")+"'/></td><td><input type='submit' value='DeleteAll' name='DeleteAll'/></td></form></tr>");
        
        out.println("</table></p>");

        try {
 		
        	ListQueuesRequest listQueuesRequest = new ListQueuesRequest();
 			
 			if (useQueueNamePrefix) {
 				listQueuesRequest.setQueueNamePrefix(queueName);
 			}
 			
			ListQueuesResult listQueuesResult = sqs.listQueues(listQueuesRequest);
			queueUrls = listQueuesResult.getQueueUrls();
			logger.debug("event=list_queues count=" + queueUrls != null ? queueUrls.size() : 0);
		
        } catch (Exception ex) {
			logger.error("event=list_queues status=failed userId= " + userId, ex);
			throw new ServletException(ex);
		}

		out.println("<p><hr width='100%' align='left' /></p>");
		
		out.println("<p><span class='content'><table border='1'>");
		out.println("<tr><th>&nbsp;</th>");
		out.println("<th>Queue Url</th>");
		out.println("<th>Queue Arn</th>");
		out.println("<th>Queue Name</th>");
		out.println("<th>User ID</th>");
		out.println("<th>Region</th>");
		out.println("<th>Visibility TO</th>");
		out.println("<th>Max Msg Size</th>");
		out.println("<th>Msg Rention Period</th>");
		out.println("<th>Delay Seconds</th>");
		out.println("<th>Approx Num Msg</th>");
		out.println("<th>&nbsp;</th><th>&nbsp;</th><th>&nbsp;</th></tr>");

		for (int i = 0; queueUrls != null && i < queueUrls.size(); i++) {
			
			Map<String, String> attributes = new HashMap<String, String>();
			
			if (showQueueAttributes) {
				try {	
				
					GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(queueUrls.get(i));
					getQueueAttributesRequest.setAttributeNames(Arrays.asList("VisibilityTimeout", "MaximumMessageSize", "MessageRetentionPeriod", "DelaySeconds", "ApproximateNumberOfMessages"));
					GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(getQueueAttributesRequest);
					attributes = getQueueAttributesResult.getAttributes();
		        	
				} catch (Exception ex) {
					logger.error("event=damaged_queue url=" + queueUrls.get(i));
				}
			}

			out.println("<form action=\"/CQSUser?userId="+user.getUserId()+"\" method=POST>");
            out.println("<tr>");
        	out.println("<td>"+i+"</td>");
            out.println("<td>"+queueUrls.get(i)+"<input type='hidden' name='qUrl' value="+queueUrls.get(i)+"></td>");
        	out.println("<td>"+Util.getArnForAbsoluteQueueUrl(queueUrls.get(i)) +"<input type='hidden' name='arn' value="+Util.getArnForAbsoluteQueueUrl(queueUrls.get(i))+"></td>");
        	out.println("<td>"+Util.getNameForAbsoluteQueueUrl(queueUrls.get(i))+"</td>");
        	out.println("<td>"+user.getUserId()+"<input type='hidden' name='userId' value="+user.getUserId()+"></td>");
        	out.println("<td>"+CMBProperties.getInstance().getRegion()+"</td>");
        	
        	out.println("<td>"+(attributes.get("VisibilityTimeout") != null ? attributes.get("VisibilityTimeout"):"")+"</td>");
        	out.println("<td>"+(attributes.get("MaximumMessageSize") != null ? attributes.get("MaximumMessageSize"):"")+"</td>");
        	out.println("<td>"+(attributes.get("MessageRetentionPeriod") != null ? attributes.get("MessageRetentionPeriod"):"")+"</td>");
        	out.println("<td>"+(attributes.get("DelaySeconds") != null ? attributes.get("DelaySeconds"):"")+"</td>");
        	out.println("<td>"+(attributes.get("ApproximateNumberOfMessages") != null ? attributes.get("ApproximateNumberOfMessages"):"")+"</td>");
        	
        	out.println("<td><a href='/CQSUser/MESSAGE?userId=" + user.getUserId()+ "&queueName=" + Util.getNameForAbsoluteQueueUrl(queueUrls.get(i)) + "'>Messages</a></td>");
        	out.println("<td><a href='/CQSUser/PERMISSIONS?userId="+ user.getUserId() + "&queueName="+ Util.getNameForAbsoluteQueueUrl(queueUrls.get(i)) + "'>Permissions</a></td>");
		    out.println("<td><input type='submit' value='Delete' name='Delete'/></td></tr></form>");
        }
		
        out.println("</table></span></p>");
        out.println("<h5 style='text-align:center;'><a href='/ADMIN'>ADMIN HOME</a></h5>");
        out.println("</body></html>");
        
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		doGet(request, response);
	}
}
