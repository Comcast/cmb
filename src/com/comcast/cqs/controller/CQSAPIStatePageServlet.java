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
public class CQSAPIStatePageServlet extends AdminServletBase {

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CQSAPIStatePageServlet.class);
	
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

		if (parameters.containsKey("ClearCache")) {
			
			try {
				httpGet(cqsServiceBaseUrl + "?Action=ManageService&Task=ClearCache&AWSAccessKeyId=" + cnsAdminUser.getAccessKey());
			} catch (Exception ex) {
				logger.error("event=failed_to_clear_queues", ex);
				throw new ServletException(ex);
			}
	
		} else if (parameters.containsKey("RemoveRecord")) {
			
		} else if (parameters.containsKey("ClearAPIStats")) {
			
			try {
				String url = request.getParameter("Url");
				httpGet(url + "?Action=ManageService&Task=ClearAPIStats&AWSAccessKeyId=" + cnsAdminUser.getAccessKey());
			} catch (Exception ex) {
				logger.error("event=failed_to_clear_queues", ex);
				throw new ServletException(ex);
			}
		}
		
		out.println("<html>");
		
		this.header(request, out, "CQS API State");
		
		out.println("<body>");

		try {

			String apiStateXml = httpGet(cqsServiceBaseUrl + "?Action=GetAPIStats&AWSAccessKeyId=" + cnsAdminUser.getAccessKey());
			
			Element root = XmlUtil.buildDoc(apiStateXml);
			
			List<Element> statsList = XmlUtil.getCurrentLevelChildNodes(XmlUtil.getCurrentLevelChildNodes(root, "GetAPIStatsResult").get(0), "Stats");
			
			out.println("<h2 align='left'>CQS API Stats</h2>");
			
			out.println("<span class='simple'><table border='1'>");
			out.println("<tr><th>Ip Address</th><th>Url</th><th>JMX Port</th><th>Long Poll Port</th><th>Data Center</th><th>Time Stamp</th><th>Num Long Poll Receives</th><th>Num Redis Keys</th><th>Num Redis Shards</th><th></th><th></th></tr>");

			Map<String, Long> aggregateCallStats = new HashMap<String, Long>();
			Map<String, Long> aggregateCallFailureStats = new HashMap<String, Long>();
			
			for (Element stats : statsList) {
				
				out.println("<tr>");
				String host = XmlUtil.getCurrentLevelTextValue(stats, "IpAddress");
				out.println("<td>" + host + "</td>");
				String url = XmlUtil.getCurrentLevelTextValue(stats, "ServiceUrl");
				out.println("<td>"+url+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "JmxPort")+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "LongPollPort")+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "DataCenter")+"</td>");
				out.println("<td>"+new Date(Long.parseLong(XmlUtil.getCurrentLevelTextValue(stats, "Timestamp")))+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "NumberOfLongPollReceives")+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "NumberOfRedisKeys")+"</td>");
				out.println("<td>"+XmlUtil.getCurrentLevelTextValue(stats, "NumberOfRedisShards")+"</td>");
				out.println("<td><form action=\"\" method=\"POST\"><input type='submit' value='Clear Cache' name='ClearCache'/></form></td>");
				out.println("<td><form action=\"\" method=\"POST\"><input type='hidden' name='Url' value='"+url+"'><input type='submit' value='Clear API Stats' name='ClearAPIStats'/></form></td>");
				out.println("</tr>");
				
				Element callStats = XmlUtil.getChildNodes(stats, "CallStats").get(0);
				
				for (Element action : XmlUtil.getChildNodes(callStats)) {
					String actionName = action.getNodeName();
					if (!aggregateCallStats.containsKey(actionName)) {
						aggregateCallStats.put(actionName, Long.parseLong(XmlUtil.getCurrentLevelTextValue(callStats, actionName)));
					} else {
						aggregateCallStats.put(actionName, aggregateCallStats.get(actionName) + Long.parseLong(XmlUtil.getCurrentLevelTextValue(callStats, actionName)));
					}
				}

				Element callFailureStats = XmlUtil.getChildNodes(stats, "CallFailureStats").get(0);
				
				for (Element action : XmlUtil.getChildNodes(callFailureStats)) {
					String actionName = action.getNodeName();
					if (!aggregateCallFailureStats.containsKey(actionName)) {
						aggregateCallFailureStats.put(actionName, Long.parseLong(XmlUtil.getCurrentLevelTextValue(callFailureStats, actionName)));
					} else {
						aggregateCallFailureStats.put(actionName, aggregateCallFailureStats.get(actionName) + Long.parseLong(XmlUtil.getCurrentLevelTextValue(callFailureStats, actionName)));
					}
				}
			}
			
			out.println("</table></span>");
			
			out.println("<h2 align='left'>CQS Call Stats</h2>");
			out.println("<span class='simple'><table border='1'>");

			for (String action : aggregateCallStats.keySet()) {
				out.println("<tr><td>"+action+"</td><td>"+aggregateCallStats.get(action)+"</td></tr>");
			}
			
			out.println("</table></span>");

			out.println("<h2 align='left'>CQS Call Failure Stats</h2>");
			out.println("<span class='simple'><table border='1'>");

			for (String action : aggregateCallFailureStats.keySet()) {
				out.println("<tr><td>"+action+"</td><td>"+aggregateCallFailureStats.get(action)+"</td></tr>");
			}
			
			out.println("</table></span>");
			
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
