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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;

import com.comcast.cmb.common.controller.AdminServletBase;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.XmlUtil;

/**
 * Subscriptions admin page
 * @author bwolf
 *
 */
public class CNSWorkerStatePageServlet extends AdminServletBase {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CNSWorkerStatePageServlet.class);
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		if (redirectNonAdminUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		
		Map<?, ?> parameters = request.getParameterMap();

		//String userId = request.getParameter("userId");
		//connect(userId);
		
		IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
		User cnsAdminUser;
		try {
			cnsAdminUser = userHandler.getUserByName(CMBProperties.getInstance().getCnsUserName());
		} catch (PersistenceException ex) {
			throw new ServletException(ex);
		}

		if (parameters.containsKey("ClearQueues")) {
			
			try {
				String host = request.getParameter("Host");
				httpGet(cnsServiceBaseUrl + "?Action=ManageWorker&Host="+host+"&Task=ClearQueues&AWSAccessKeyId=" + cnsAdminUser.getAccessKey());
			} catch (Exception ex) {
				logger.error("event=failed_to_clear_queues", ex);
				throw new ServletException(ex);
			}
	
		} else if (parameters.containsKey("RemoveRecord")) {
			
			try {
				String host = request.getParameter("Host");
				httpGet(cnsServiceBaseUrl + "?Action=ManageWorker&Host="+host+"&Task=RemoveRecord&AWSAccessKeyId=" + cnsAdminUser.getAccessKey());
			} catch (Exception ex) {
				logger.error("event=failed_to_clear_queues", ex);
				throw new ServletException(ex);
			}
		}
		
		out.println("<html>");
		
		this.header(request, out, "CNS Worker State");
		
		out.println("<body>");

		out.println("<h2><a href='/ADMIN'>All Users</a></h2>");

		try {

			String workerStateXml = httpGet(cnsServiceBaseUrl + "?Action=GetWorkerStats&AWSAccessKeyId=" + cnsAdminUser.getAccessKey());
			
			Element root = XmlUtil.buildDoc(workerStateXml);
			
			List<Element> statsList = XmlUtil.getCurrentLevelChildNodes(XmlUtil.getCurrentLevelChildNodes(root, "GetWorkerStatsResult").get(0), "Stats");
			
			out.println("<h2 align='left'>CNS Worker Stats</h2>");
			
			out.println("<span class='simple'><table border='1'>");
			out.println("<tr><th>Host</th><th>Jmx Port</th><th>Mode</th><th>Msg Published</th>");
			out.println("<th>Producer Heartbeat</th><th>Active</th><th>Consumer Heartbeat</th><th>Active</th>");
			out.println("<th>Delivery Queue Size</th><th>Redelivery Queue Size</th><th>Consumer Overloaded</th><th>Cqs Service Available</th><th>Http Pool Size</th><th></th><th></th></tr>");

			String alarmColor = " bgcolor='#C00000'";
			String okColor = " bgcolor='#00C000'";
					
			int deliveryQueueMaxSize = CMBProperties.getInstance().getDeliveryHandlerJobQueueLimit();
			int redeliveryQueueMaxSize = CMBProperties.getInstance().getReDeliveryHandlerJobQueueLimit();
			
			Map<String, Integer> endpointErrorCounts = new HashMap<String, Integer>();

			for (Element stats : statsList) {
				
				out.println("<tr>");
				String host = XmlUtil.getCurrentLevelTextValue(stats, "IpAddress");
				out.println("<td>"+host+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "JmxPort")+"</td>");
				String mode = XmlUtil.getCurrentLevelTextValue(stats, "Mode");
				out.println("<td>"+mode+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "NumPublishedMessages")+"</td>");
				out.println("<td>"+new Date(Long.parseLong(XmlUtil.getCurrentLevelTextValue(stats, "ProducerTimestamp")))+"</td>");
				boolean activeProducer = Boolean.parseBoolean(XmlUtil.getCurrentLevelTextValue(stats, "ActiveProducer"));
				out.println("<td"+(!activeProducer && mode.contains("Producer") ? alarmColor : okColor)+">"+activeProducer+"</td>");
				out.println("<td>"+new Date(Long.parseLong(XmlUtil.getCurrentLevelTextValue(stats, "ConsumerTimestamp")))+"</td>");
				boolean activeConsumer = Boolean.parseBoolean(XmlUtil.getCurrentLevelTextValue(stats, "ActiveConsumer"));
				out.println("<td"+(!activeConsumer && mode.contains("Consumer") ? alarmColor : okColor)+">"+activeConsumer+"</td>");
				int deliveryQueueSize = Integer.parseInt(XmlUtil.getCurrentLevelTextValue(stats, "DeliveryQueueSize"));
				out.println("<td"+(1.0*deliveryQueueSize/deliveryQueueMaxSize >= 0.75 ? alarmColor : okColor)+">"+deliveryQueueSize+"</td>");
				int redeliveryQueueSize = Integer.parseInt(XmlUtil.getCurrentLevelTextValue(stats, "RedeliveryQueueSize"));
				out.println("<td"+(1.0*redeliveryQueueSize/redeliveryQueueMaxSize >= 0.75 ? alarmColor : okColor)+">"+redeliveryQueueSize+"</td>");
				boolean consumerOverloaded = Boolean.parseBoolean(XmlUtil.getCurrentLevelTextValue(stats, "ConsumerOverloaded"));
				out.println("<td"+(consumerOverloaded ? alarmColor : okColor)+">"+consumerOverloaded+"</td>");
				boolean cqsServiceAvailable = Boolean.parseBoolean(XmlUtil.getCurrentLevelTextValue(stats, "CqsServiceAvailable"));
				out.println("<td"+((!cqsServiceAvailable && (activeProducer || activeConsumer))? alarmColor : okColor)+">"+cqsServiceAvailable+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "NumPooledHttpConnections")+"</td>");
				out.println("<td><form action=\"\" method=\"POST\"><input type='hidden' name='Host' value='"+host+"'><input type='submit' value='Clear Queues' name='ClearQueues'/></form></td>");
				out.println("<td><form action=\"\" method=\"POST\"><input type='hidden' name='Host' value='"+host+"'><input type='submit' value='Remove Record' name='RemoveRecord'/></form></td>");
				out.println("</tr>");
				
				List<Element> errorCounts = XmlUtil.getChildNodes(stats, "ErrorCountForEndpoints");
				
				if (errorCounts.size() == 1) {
					
					for (Element errorCount : XmlUtil.getChildNodes(errorCounts.get(0), "Error")) {
						
						String endpoint = XmlUtil.getCurrentLevelAttributeTextValue(errorCount, "endpoint");
						Integer count = XmlUtil.getCurrentLevelAttributeIntValue(errorCount, "count");
						
						if (!endpointErrorCounts.containsKey(endpoint)) {
							endpointErrorCounts.put(endpoint, count);
						} else {
							endpointErrorCounts.put(endpoint, endpointErrorCounts.get(endpoint) + count);
						}
					}
				}
			}
			
			out.println("</table><span>");
			
			if (endpointErrorCounts.size() > 0) {
			
				out.println("<h2 align='left'>Failed or timed out Responses during past 60 Seconds by Endpoint</h2>");
				
				out.println("<span class='simple'><table border='1'>");
				out.println("<tr><th>Endpoint</th><th>Error Count</th><th><th>");
				
				for (String endpoint : endpointErrorCounts.keySet()) {

					out.println("<tr>");
					out.println("<td>"+endpoint+"</td>");
					out.println("<td>"+endpointErrorCounts.get(endpoint)+"</td>");
					
					int failureSuspensionThreshold = CMBProperties.getInstance().getEndpointFailureCountToSuspensionThreshold();
					
					if (failureSuspensionThreshold != 0 && endpointErrorCounts.get(endpoint) > failureSuspensionThreshold) {
						out.println("<td bgcolor='#C00000'><i>suspended</i></td>");
					} else {
						out.println("<td>&nbsp;</td>");
					}
					
					out.println("</tr>");
				}
	
				out.println("</table></span>");
			
			}

		} catch (Exception ex) {
			logger.error("", ex);
			throw new ServletException(ex);
		}
		
        out.println("</body></html>");
        
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
