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
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.services.sns.model.GetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;

/**
 * Admin page for editing subscription delivery policy
 * @author tina, aseem, bwolf
 *
 */
public class CNSRawMessageDeliveryPolicyPage extends AdminServletBase {
	
    private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger(CNSRawMessageDeliveryPolicyPage.class);
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		if (redirectUnauthenticatedUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
	    response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String subArn = request.getParameter("subscriptionArn");
		String userId = request.getParameter("userId");
		Map<?, ?> params = request.getParameterMap();
		
		connect(request);
		
		out.println("<html>");
		
		simpleHeader(request, out, "Raw Message Delivery Policy");
		
		if (params.containsKey("Update")) {
			String rawMessageDeliveryParam = request.getParameter("rawmessage");
			Boolean rawMessageDelivery = false;
			
			
			if (rawMessageDeliveryParam.trim().length() > 0) {
				rawMessageDelivery = Boolean.parseBoolean(rawMessageDeliveryParam.trim());
			} 
			
			
			try {

				SetSubscriptionAttributesRequest setSubscriptionAttributesRequest = new SetSubscriptionAttributesRequest(subArn, "RawMessageDelivery", rawMessageDelivery.toString());
				sns.setSubscriptionAttributes(setSubscriptionAttributesRequest);
				
				logger.debug("event=set_raw_message_delivery_policy sub_arn=" + subArn + " user_id= " + userId);

			} catch (Exception ex) {
				logger.error("event=set_raw_message_delivery_policy sub_arn=" + subArn + " user_id= " + userId, ex);
				throw new ServletException(ex);
			}
			out.println("<body onload='javascript:window.opener.location.reload();window.close();'>");
			
		} else {
			Boolean rawMessageDelivery = false;
			if (subArn != null) {
				
				Map<String, String> attributes = null;
				
				try {
					GetSubscriptionAttributesRequest getSubscriptionAttributesRequest = new GetSubscriptionAttributesRequest(subArn);
					GetSubscriptionAttributesResult getSubscriptionAttributesResult = sns.getSubscriptionAttributes(getSubscriptionAttributesRequest);
					attributes = getSubscriptionAttributesResult.getAttributes();
					String rawMessageDeliveryStr = attributes.get("RawMessageDelivery");
					if(rawMessageDeliveryStr != null && !rawMessageDeliveryStr.isEmpty()){
						rawMessageDelivery = Boolean.parseBoolean(rawMessageDeliveryStr);
					}
				} catch (Exception ex) {
					logger.error("event=get_raw_message_delivery_attribute sub_arn=" + subArn + " user_id= " + userId, ex);
					throw new ServletException(ex);
				}
			}
			
			out.println("<body>");
			out.println("<h1>Raw Message Delivery Policy</h1>");
			out.println("<form action=\"/webui/cnsuser/subscription/rawmessagedeliverypolicy?subscriptionArn="+subArn+"\" method=POST>");
            out.println("<input type='hidden' name='userId' value='"+ userId +"'>");
			out.println("<table width='98%'");
			out.println("<tr><td colspan=2><b><font color='orange'>Raw Message Delivery</font></b></td></tr>");
			out.println("<tr><td ><input type='radio' name='rawmessage' value='true' " + (rawMessageDelivery?"checked='true'":"")  + ">True</td>");
			out.println("<td ><input type='radio' name='rawmessage' value='false'  " + (rawMessageDelivery?"":"checked='true'")  + ">False</td></tr>");
			out.println("<tr><td>&nbsp;</td><td>&nbsp;</td></tr>");
			
			
			out.println("<tr><td colspan=2><hr/></td></tr>");
			out.println("<tr><td colspan=2 align=right><input type='button' onclick='window.close()' value='Cancel'><input type='submit' name='Update' value='Update'></td></tr></table></form>");
		}
		
		out.println("</body></html>");
		
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
