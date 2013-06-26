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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.util.Util;

public class CQSEditQueueAttributePage extends AdminServletBase {

		private static final long serialVersionUID = 1L;
	    private static Logger logger = Logger.getLogger(CQSEditQueueAttributePage.class);
		
	    @Override
		public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
			if (redirectUnauthenticatedUser(request, response)) {
				return;
			}

			CMBControllerServlet.valueAccumulator.initializeAllCounters();
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			String userId =  request.getParameter("userId");
			Map<?, ?> params = request.getParameterMap();
			String queueName = request.getParameter("queueName");
			String queueUrl = Util.getAbsoluteQueueUrlForName(queueName, userId);
			
			connect(userId);
			
			out.println("<html>");
			
			simpleHeader(request, out, "View/Edit Queue Attributes");
			
			if (params.containsKey("Update")) {
			
				String visibilityTimeout = request.getParameter("visibilityTimeout");
				String maximumMessageSize = request.getParameter("maximumMessageSize");
				String messageRetentionPeriod = request.getParameter("messageRetentionPeriod");
				String delaySeconds = request.getParameter("delaySeconds");
				String receiveMessageWaitTimeSeconds = request.getParameter("receiveMessageWaitTimeSeconds");
				String numberOfPartitions = request.getParameter("numberOfPartitions");
				String numberOfShards = request.getParameter("numberOfShards");
				
				try {

					Map<String, String> attributes = new HashMap<String, String>();
					
					if (visibilityTimeout != null && !visibilityTimeout.equals("")) {
						attributes.put("VisibilityTimeout", visibilityTimeout);
					}
					
					if (maximumMessageSize != null && !maximumMessageSize.equals("")) {
						attributes.put("MaximumMessageSize", maximumMessageSize);
					}

					if (messageRetentionPeriod != null && !messageRetentionPeriod.equals("")) {
						attributes.put("MessageRetentionPeriod", messageRetentionPeriod);
					}

					if (delaySeconds != null && !delaySeconds.equals("")) {
						attributes.put("DelaySeconds", delaySeconds);
					}

					if (receiveMessageWaitTimeSeconds != null && !receiveMessageWaitTimeSeconds.equals("")) {
						attributes.put("ReceiveMessageWaitTimeSeconds", receiveMessageWaitTimeSeconds);
					}
					
					if (numberOfPartitions != null && !numberOfPartitions.equals("")) {
						attributes.put("NumberOfPartitions", numberOfPartitions);
					}

					if (numberOfPartitions != null && !numberOfPartitions.equals("")) {
						attributes.put("NumberOfShards", numberOfShards);
					}

					SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest(queueUrl, attributes);
					sqs.setQueueAttributes(setQueueAttributesRequest);
					
					logger.debug("event=set_queue_attributes queue_ulr=" + queueUrl + " user_id= " + userId);

				} catch (Exception ex) {
					logger.error("event=set_queue_attributes queue_ulr=" + queueUrl + " user_id= " + userId, ex);
					throw new ServletException(ex);
				}
				
				out.println("<body onload='javascript:window.opener.location.reload();window.close();'>");
				
			} else {

				String visibilityTimeout = "";
				String maximumMessageSize = "";
				String messageRetentionPeriod = "";
				String delaySeconds = "";
				String receiveMessageWaitTimeSeconds = "";
				String numberOfPartitions = "";
				String numberOfShards = "";
				
				if (queueUrl != null) {
				
					Map<String, String> attributes = null;
					
					try {
						GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(queueUrl);
						getQueueAttributesRequest.setAttributeNames(Arrays.asList("VisibilityTimeout", "MaximumMessageSize", "MessageRetentionPeriod", "DelaySeconds", "ReceiveMessageWaitTimeSeconds", "NumberOfPartitions", "NumberOfShards"));
						GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(getQueueAttributesRequest);
						attributes = getQueueAttributesResult.getAttributes();
						
						visibilityTimeout = attributes.get("VisibilityTimeout");
						maximumMessageSize = attributes.get("MaximumMessageSize");
						messageRetentionPeriod = attributes.get("MessageRetentionPeriod");
						delaySeconds = attributes.get("DelaySeconds");
						receiveMessageWaitTimeSeconds = attributes.get("ReceiveMessageWaitTimeSeconds");
						numberOfPartitions = attributes.get("NumberOfPartitions");
						numberOfShards = attributes.get("NumberOfShards");
						
					} catch (Exception ex) {
						logger.error("event=failed_to_get_attributes queue_url=" + queueUrl, ex);
						throw new ServletException(ex);
					}
				}
				
				out.println("<body>");
				out.println("<h1>View/Edit Queue Attributes</h1>");
				out.println("<h3>"+queueUrl+"</h3>");
				out.println("<form action=\"/webui/cqsuser/editqueueattributes?queueName="+queueName+"\" method=POST>");
	            out.println("<input type='hidden' name='userId' value='"+ userId +"'>");
				out.println("<table>");
				out.println("<tr><td colspan=2><b><font color='orange'>Queue Attributes</font></b></td></tr>");
				out.println("<tr><td colspan=2><b>Apply these attributes to the queue:</b></td></tr>");

				out.println("<tr><td>Visibility Timeout:</td><td><input type='text' name='visibilityTimeout' size='50' value='" + visibilityTimeout + "'></td></tr>");
				out.println("<tr><td>&nbsp;</td><td><I><font color='grey'>Default "+CMBProperties.getInstance().getCQSVisibilityTimeOut()+" sec</font></I></td></tr>");
				
				out.println("<tr><td>Maximum Message Size:</td><td><input type='text' name='maximumMessageSize' size='50' value='" + maximumMessageSize + "'></td></tr>");
				out.println("<tr><td>&nbsp;</td><td><I><font color='grey'>Default "+CMBProperties.getInstance().getCQSMaxMessageSize()+" bytes</font></I></td></tr>");

				out.println("<tr><td>Message Retention Period:</td><td><input type='text' name='messageRetentionPeriod' size='50' value='" + messageRetentionPeriod + "'></td></tr>");
				out.println("<tr><td>&nbsp;</td><td><I><font color='grey'>Default "+CMBProperties.getInstance().getCQSMessageRetentionPeriod()+" sec</font></I></td></tr>");

				out.println("<tr><td>Delay Seconds:</td><td><input type='text' name='delaySeconds' size='50' value='" + delaySeconds + "'></td></tr>");
				out.println("<tr><td>&nbsp;</td><td><I><font color='grey'>Default "+CMBProperties.getInstance().getCQSMessageDelaySeconds()+" sec</font></I></td></tr>");

				out.println("<tr><td>Receive Message Wait Time Seconds:</td><td><input type='text' name='receiveMessageWaitTimeSeconds' size='50' value='" + receiveMessageWaitTimeSeconds + "'></td></tr>");
				out.println("<tr><td>&nbsp;</td><td><I><font color='grey'>Default 0 sec, max 20 sec</font></I></td></tr>");

				out.println("<tr><td>Number Of Partitions:</td><td><input type='text' name='numberOfPartitions' size='50' value='" + numberOfPartitions + "'></td></tr>");
				out.println("<tr><td>&nbsp;</td><td><I><font color='grey'>Default 100, minimum 1 partition(s)</font></I></td></tr>");

				out.println("<tr><td>Number Of Shards:</td><td><input type='text' name='numberOfShards' size='50' value='" + numberOfShards + "'></td></tr>");
				out.println("<tr><td>&nbsp;</td><td><I><font color='grey'>Default 1, maximum 100 shards</font></I></td></tr>");

				out.println("<tr><td>&nbsp;</td><td>&nbsp;</td></tr>");
				
				out.println("<tr><td colspan=2><hr/></td></tr>");
				out.println("<tr><td colspan=2 align=right><input type='button' onclick='window.close()' value='Cancel'><input type='submit' name='Update' value='Update'></td></tr></table></form>");
			}
			
			out.println("</body></html>");
			
			CMBControllerServlet.valueAccumulator.deleteAllCounters();
		}
	    
		@Override
		public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			doGet(request, response);
		}
	}
