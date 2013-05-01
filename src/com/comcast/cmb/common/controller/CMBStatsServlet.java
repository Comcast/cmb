package com.comcast.cmb.common.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.util.CMBProperties;

public class CMBStatsServlet extends AdminServletBase {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CMBStatsServlet.class);
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		if (redirectNonAdminUser(request, response)) {
			return;
		}

		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		
		Map<?, ?> parameters = request.getParameterMap();
		
		out.println("<html>");
		
		this.header(request, out, "CMB API Stats");
		
		out.println("<body>");
		
		if (!CMBProperties.getInstance().isCMBStatsEnabled()) {
			out.println("<p>Stats tracking currently disabled. Enable in cmb.properties</p></body></html>");
			return;
		}

		int rs = 10;
		
		if (parameters.containsKey("rs")) {
			rs = Integer.parseInt(request.getParameter("rs"));
		}
		
		String ac = null;
		
		if (CMBControllerServlet.callStats.containsKey("ReceiveMessage")) {
			ac = "ReceiveMessage";
		} else if (CMBControllerServlet.callStats.size() > 0) {
			ac = (String)CMBControllerServlet.callStats.keySet().toArray()[0];
		}
		
		if (parameters.containsKey("ac")) {
			ac = request.getParameter("ac");
		}

		out.println("<h2 align='left'>API Response Time Percentiles [" + rs + " MS]</h2>");
		out.println("<p><a href='?rs=1&ac="+ac+"'>1ms</a>&nbsp;<a href='?rs=10&ac="+ac+"'>10ms</a>&nbsp;<a href='?rs=100&ac="+ac+"'>100ms</a>&nbsp;<a href='?rs=1000&ac="+ac+"'>1000ms</a></p>");
		out.println("<p><img src='/webui/cmbvisualizer/responsetimeimg?rs="+rs+"'></p>");

		out.println("<h2 align='left'>API Response Time Percentiles [" + ac + "]</h2>");
		out.print("<p>");
		for (String a : CMBControllerServlet.callStats.keySet()) {
			out.print("<a href='?ac="+a+"&rs="+rs+"'>"+a+"</a>&nbsp;");
		}
		out.println("</p>");
		out.println("<p><img src='/webui/cmbvisualizer/responsetimeimg?ac="+ac+"'></p>");

		out.println("<h2 align='left'>Redis Response Time Percentiles</h2>");
		out.println("<p><img src='/webui/cmbvisualizer/responsetimeimg?redis=true'></p>");

		out.println("<h2 align='left'>Cassandra Response Time Percentiles</h2>");
		out.println("<p><img src='/webui/cmbvisualizer/responsetimeimg?cassandra=true'></p>");

		out.println("<h2 align='left'>API Call Distribution</h2>");
		out.println("<p><img src='/webui/cmbvisualizer/calldistributionimg'></p>");
		
		if (CMBControllerServlet.callStats.keySet().size() > 0) {
			out.println("<h2 align='left'>API Call Counts</h2>");
			out.println("<span class='simple'><table border='1'>");
			for (String action : CMBControllerServlet.callStats.keySet()) {
				out.println("<tr><td>"+action+"</td><td>"+CMBControllerServlet.callStats.get(action)+"</td></tr>");
			}
			out.println("</table></span>");
		}

		if (CMBControllerServlet.callFailureStats.keySet().size() > 0) {
			out.println("<h2 align='left'>API Failure Counts</h2>");
			out.println("<span class='simple'><table border='1'>");
			for (String action : CMBControllerServlet.callFailureStats.keySet()) {
				out.println("<tr><td>"+action+"</td><td>"+CMBControllerServlet.callFailureStats.get(action)+"</td></tr>");
			}
			out.println("</table></span>");
			out.println("<h2 align='left'>Recent Errors</h2>");
			out.println("<span class='simple'><table border='1'>");
			for (int i=0; i<CMBControllerServlet.recentErrors.length; i++) {
				String detail = CMBControllerServlet.recentErrors[i];
				if (detail != null) {
					String elements[] = detail.split("\\|");
					if (elements.length >= 3) {
						out.println("<tr><td>"+elements[0]+"</td></tr>");
						out.println("<tr><td>"+elements[1]+"</td></tr>");
						out.println("<tr><td>"+elements[2]+"</td></tr>");
					}
				}
			}
			out.println("</table></span>");
		}
		
        out.println("</body></html>");
        
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
