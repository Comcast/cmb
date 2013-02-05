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
package com.comcast.cns.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.SetTopicAttributesRequest;
import com.amazonaws.services.sns.model.Topic;
import com.comcast.cmb.common.controller.AdminServlet;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.util.Util;

/**
 * User admin page
 * @author tina, bwolf, aseem, baosen
 *
 */
public class CNSUserPageServlet extends AdminServletBase {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CNSUserPageServlet.class);
	private String userId;
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		if (!CMBProperties.getInstance().getCNSServiceEnabled()) {
			throw new ServletException("CNS service disabled");
		}
		
		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();

		Map<?, ?> parameters = request.getParameterMap();
		userId = request.getParameter("userId");
		String topicName = request.getParameter("topic");
		String arn = request.getParameter("arn");
		String displayName = request.getParameter("display");
		
        List<Topic> topics = new ArrayList<Topic>();
		
		connect(userId);

		if (parameters.containsKey("Create")) {
		
			try {
				
				CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
				CreateTopicResult createTopicResult = sns.createTopic(createTopicRequest);
				
				arn = createTopicResult.getTopicArn();
				
				SetTopicAttributesRequest setTopicAttributesRequest = new SetTopicAttributesRequest(arn, "DisplayName", displayName);
				sns.setTopicAttributes(setTopicAttributesRequest);
				
				logger.debug("event=created_topic topic_name=" + topicName);

			} catch (Exception ex) {
				logger.error("event=createTopic status=failed userId= " + userId, ex);
				throw new ServletException(ex);
			}
		
		} else if (parameters.containsKey("Delete")) {

			try {
				DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(arn);
				sns.deleteTopic(deleteTopicRequest);
				logger.debug("event=deleted_topic topic_name=" + topicName);
			} catch (Exception ex) {
				logger.error("event=deletedTopic status=failed userId= " + userId, ex);
				throw new ServletException(ex);
			}

		} else if (parameters.containsKey("DeleteAll")) {
			
	        try {
	        	
				ListTopicsRequest createTopicRequest = new ListTopicsRequest();
				ListTopicsResult createTopicResult = sns.listTopics(createTopicRequest);
				topics = createTopicResult.getTopics();
				
				//todo: use next token

				logger.debug("event=list_topics count=" + topics != null ? topics.size() : 0);
			} catch (Exception ex) {
				logger.error("event=listTopics status=failed userId= " + userId, ex);
				throw new ServletException(ex);
			}
			
			for (int i = 0; topics != null && i < topics.size(); i++) {
	        	
	        	Topic t = topics.get(i);

	        	try {
					DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(t.getTopicArn());
					sns.deleteTopic(deleteTopicRequest);
					logger.debug("event=deleted_topic topic_name=" + topicName);
				} catch (Exception ex) {
					logger.error("event=deletedTopic status=failed userId= " + userId, ex);
				}
			}			
		}
		
		out.println("<html>");
		
		header(request, out, "Topics");
		
		out.println("<body>");

		out.println("<h2>Topics</h2>");
		
		if (user != null) {
			out.println("<table><tr><td><b>User Name:</b></td><td>"+ user.getUserName()+"</td></tr>");
			out.println("<tr><td><b>User ID:</b></td><td>"+ user.getUserId()+"</td></tr>");
			out.println("<tr><td><b>Access Key:</b></td><td>"+user.getAccessKey()+"</td></tr>");
			out.println("<tr><td><b>Access Secret:</b></td><td>"+user.getAccessSecret()+"</td></tr></table>");
		}
        
		out.println("<p><table>");
		out.println("<tr><td><b>Topic Name</b></td><td><b>Topic Display Name</b></td><td></td></tr>");
        out.println("<form action=\"/CNSUser?userId=" + userId + "\" " + "method=POST>");
        out.println("<tr><td><input type='text' name='topic' /></td><td><input type='text' name='display'><input type='hidden' name='userId' value='"+ userId + "'></td><td><input type='submit' value='Create' name='Create' /></td></tr></form></table></p>");
		
		out.println("<p><table>");
        out.println("<form action=\"/CNSUser?userId=" + userId + "\" " + "method=POST>");
        out.println("<tr><td><input type='hidden' name='userId' value='"+ userId + "'></td><td><input type='submit' value='DeleteAll' name='DeleteAll'/></td></tr></form></table></p>");

        try {
        	
			ListTopicsRequest createTopicRequest = new ListTopicsRequest();
			ListTopicsResult createTopicResult = sns.listTopics(createTopicRequest);
			topics = createTopicResult.getTopics();
			
			//todo: use next token

			logger.debug("event=list_topics count=" + topics != null ? topics.size() : 0);
		} catch (Exception ex) {
			logger.error("event=listTopics status=failed userId= " + userId, ex);
			throw new ServletException(ex);
		}
        
		out.println("<p><hr width='100%' align='left' /></p>");
		out.println("<p><span class='content'><table border='1'>");
		out.println("<tr><th>&nbsp;</th>");
		out.println("<th>Topic Arn</th>");
		out.println("<th>Topic Name</th>");
		out.println("<th>Topic Display Name</th>");
		out.println("<th>User ID</th>");
		out.println("<th>&nbsp;</th>");
		out.println("<th>&nbsp;</th>");
		out.println("<th>&nbsp;</th>");
		out.println("<th>&nbsp;</th>");
		out.println("<th>&nbsp;</th></tr>");

		for (int i = 0; topics != null && i < topics.size(); i++) {
        	
        	Topic t = topics.get(i);
        	
			GetTopicAttributesRequest getTopicAttributesRequest = new GetTopicAttributesRequest(t.getTopicArn());
			GetTopicAttributesResult getTopicAttributesResult = sns.getTopicAttributes(getTopicAttributesRequest);
			Map<String, String> attributes = getTopicAttributesResult.getAttributes();

			out.println("<form action=\"/CNSUser?userId="+userId+"\" method=POST>");
        	out.println("<tr><td>"+i+"</td>");
        	out.println("<td>"+t.getTopicArn() +"<input type='hidden' name='arn' value="+t.getTopicArn()+"></td>");
        	out.println("<td>"+Util.getNameFromTopicArn(t.getTopicArn())+"</td>");
        	out.println("<td><a href='' onclick=\"window.open('/CNSUser/EditDisplayName?topicArn="+ t.getTopicArn() + "&userId="+userId+"', 'EditDisplayName', 'height=300,width=700,toolbar=no')\">"+(attributes.get("DisplayName") == null ? "{unset}" : attributes.get("DisplayName"))+"</a></td>");
        	out.println("<td>"+user.getUserId()+"<input type='hidden' name='userId' value="+user.getUserId()+"></td>");
        	out.println("<td><a href='/CNSUser/SUBSCRIPTION?userId="+ userId + "&topicArn=" + t.getTopicArn() + "'>Subscriptions</a></td>");
        	out.println("<td><a href='/CNSUser/Publish?userId="+ userId + "&topicArn="+ t.getTopicArn() + "' target='_blank'>Publish</a></td>");
        	out.println("<td><a href='' onclick=\"window.open('/CNSUser/EditDeliveryPolicy?topicArn="+ t.getTopicArn() + "&userId="+userId+"', 'EditDeliveryPolicy', 'height=630,width=580,toolbar=no')\">View/Edit Topic Delivery Policy</a></td>");
		    out.println("<td><a href='/CNSUser/Permission?topicArn="+ t.getTopicArn() + "&userId=" + userId + "'>Permission</a></td>");
        	out.println("<td><input type='submit' value='Delete' name='Delete' /></td></tr></form>");
        }
        
        out.println("</table></span></p>");
        out.println("<h5 style='text-align:center;'><a href='/ADMIN'>ADMIN HOME</a></h5>");
        out.println("</body></html>");
        
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		doGet(request, response);
	}
}
