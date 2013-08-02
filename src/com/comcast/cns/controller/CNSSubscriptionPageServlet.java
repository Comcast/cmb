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

import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.persistence.ICNSTopicPersistence;

import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.UnsubscribeRequest;

/**
 * Subscriptions admin page
 * @author tina, bwolf, aseem, michael
 *
 */
public class CNSSubscriptionPageServlet extends AdminServletBase {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CNSSubscriptionPageServlet.class);
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		
		Map<?, ?> parameters = request.getParameterMap();
		
		String userId = request.getParameter("userId");
		String topicArn = request.getParameter("topicArn");
		String endPoint = request.getParameter("endPoint");
		String protocol = request.getParameter("protocol");
		String arn = request.getParameter("arn");
		String nextToken = request.getParameter("nextToken");
		
		connect(request);
		
		if (parameters.containsKey("Subscribe")) {
			
			try {
				
				SubscribeRequest subscribeRequest = new SubscribeRequest(topicArn, protocol.toLowerCase(), endPoint);
				sns.subscribe(subscribeRequest);
				
			} catch (Exception ex) {
				logger.error("event=subscribe", ex);
				throw new ServletException(ex);
			}

		} else if (parameters.containsKey("Unsubscribe")) {
			
			try {
				UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest(arn);
				sns.unsubscribe(unsubscribeRequest);
			} catch (Exception ex) {
				logger.error("event=unsubscribe arn=" + arn , ex);
				throw new ServletException(ex);
			}
		}
		
		List<Subscription> subscriptions = new ArrayList<Subscription>();
		ListSubscriptionsByTopicResult listSubscriptionsByTopicResult = null;
		
		try {
			
			listSubscriptionsByTopicResult = sns.listSubscriptionsByTopic(new ListSubscriptionsByTopicRequest(topicArn, nextToken));
			subscriptions = listSubscriptionsByTopicResult.getSubscriptions();
			
		} catch (Exception ex) {
			logger.error("event=listAllSubscriptionsByTopic topic_arn=" + topicArn, ex);
			throw new ServletException(ex);
		}
		
		ICNSTopicPersistence topicHandler = PersistenceFactory.getTopicPersistence();
		CNSTopic topic = null;
		
		try {
			topic = topicHandler.getTopic(topicArn);
		} catch (Exception ex) {
			logger.error("event=getTopic topic_arn=" + topicArn, ex);
			throw new ServletException(ex);
		}
		
		out.println("<html>");
		out.println("<script type='text/javascript' language='javascript'>");
		out.println("function changeEndpointHint(protocol){ ");
		out.println(" if (protocol == 'HTTP' || protocol == 'HTTPS') { ");
		out.println(" document.getElementById('endPoint').placeholder = 'e.g. http://company.com'; }");
		out.println(" else if (protocol == 'EMAIL' || protocol == 'EMAIL_JSON') { ");
		out.println(" document.getElementById('endPoint').placeholder = 'e.g. user@domain.com'; }");
		out.println(" else if (protocol == 'CQS' || protocol == 'SQS') { ");
		out.println(" document.getElementById('endPoint').placeholder = 'e.g. arn:aws:cqs:ccp:555555555555:my-queue'; } ");
		out.println("}");
		out.println("</script>");
		
		header(request, out, "Subscriptions for Topic "+ ((topic != null) ? topic.getName():""));
		
		out.println("<body>");

		out.println("<h2>Subscriptions for Topic "+ ((topic != null) ? topic.getName():"") + "</h2>");
		
		if (user != null) {
			out.println("<table><tr><td><b>User Name:</b></td><td>"+ user.getUserName()+"</td></tr>");
			out.println("<tr><td><b>User ID:</b></td><td>"+ user.getUserId()+"</td></tr>");
			out.println("<tr><td><b>Access Key:</b></td><td>"+user.getAccessKey()+"</td></tr>");
			out.println("<tr><td><b>Access Secret:</b></td><td>"+user.getAccessSecret()+"</td></tr>");
			out.println("<tr><td><b>Topic Name:</b></td><td>"+ topic.getName()+"</td></tr>");
			out.println("<tr><td><b>Topic Display Name:</b></td><td>" + topic.getDisplayName()+ "</td></tr>");
			out.println("<tr><td><b>Topic Arn:</b></td><td>" + topic.getArn()+ "</td></tr>");
			out.println("<tr><td><b>Num Subscriptions:</b></td><td>" + subscriptions.size()+ "</td></tr></table>");
		}
		
        out.println("<p><table><tr><td><b>Protocol</b></td><td><b>End Point</b></td><td>&nbsp;</td></tr>");
        out.println("<form action=\"/webui/cnsuser/subscription/?userId="+userId+"&topicArn="+topicArn+"\" method=POST>");
        out.println("<tr><td><select name='protocol' onchange='changeEndpointHint(this.value)'><option value='HTTP'>HTTP</option><option value='HTTPS'>HTTPS</option><option value='EMAIL'>EMAIL</option><option value='EMAIL_JSON'>EMAIL_JSON</option><option value='CQS'>CQS</option><option value='SQS'>SQS</option></select></td>");
        out.println("<td><input type='text' name='endPoint' id = 'endPoint' size='65' placeholder='e.g. http://company.com'><input type='hidden' name='userId' value='"+ userId + "'></td><td><input type='submit' value='Subscribe' name='Subscribe' /></td></tr>");
        out.println("</form></table>");
       
		out.println("<p><hr width='100%' align='left' />");
		out.println("<p><span class='content'><table border='1'>");
		out.println("<tr><th>Row</th>");
		out.println("<th>Arn</th>");
		out.println("<th>Protocol</th>");
		out.println("<th>End Point</th>");
		out.println("<th>&nbsp;</th>");
		out.println("<th>&nbsp;</th></tr>");

		for (int i = 0; subscriptions != null && i < subscriptions.size(); i++) {
        
        	Subscription s = subscriptions.get(i);
        	out.println("<form action=\"/webui/cnsuser/subscription/?userId="+user.getUserId()+"&arn="+s.getSubscriptionArn()+"&topicArn="+topicArn+"\" method=POST>");
        	out.println("<tr><td>"+i+"</td>");
        	out.println("<td>"+s.getSubscriptionArn() +"<input type='hidden' name='arn' value="+s.getSubscriptionArn()+"></td>");
        	out.println("<td>"+s.getProtocol()+"</td>");
        	out.println("<td>"+s.getEndpoint()+"</td>");
        	
        	if (s.getProtocol().toString().equals("http") && !s.getSubscriptionArn().equals("PendingConfirmation")) {
        		out.println("<td><a href='' onclick=\"window.open('/webui/cnsuser/subscription/editdeliverypolicy?subscriptionArn="+ s.getSubscriptionArn() + "&userId=" + userId + "', 'EditDeliveryPolicy', 'height=630,width=580,toolbar=no')\">View/Edit Delivery Policy</a></td>");
        	} else {
        		out.println("<td>&nbsp;</td>");
        	}
		    
        	if (s.getSubscriptionArn().equals("PendingConfirmation")) {
        		out.println("<td>&nbsp;</td>");
        	} else {
        		out.println("<td><input type='submit' value='Unsubscribe' name='Unsubscribe'/></td>");
        	}
        	
        	out.println("</tr></form>");
        }
        
        out.println("</table></span></p>");
        
        if (listSubscriptionsByTopicResult != null && listSubscriptionsByTopicResult.getNextToken() != null) {
        	out.println("<p><a href='/webui/cnsuser/subscription/?userId="+userId+"&topicArn="+topicArn+"&nextToken="+response.encodeURL(listSubscriptionsByTopicResult.getNextToken())+"'>next&nbsp;&gt;</a></p>");
        }
        
        
        out.println("<h5 style='text-align:center;'><a href='/webui'>ADMIN HOME</a>");
        out.println("<a href='/webui/cnsuser?userId="+userId+"&topicArn="+topicArn+"'>BACK TO TOPIC</a></h5>");
        out.println("</body></html>");
        
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
