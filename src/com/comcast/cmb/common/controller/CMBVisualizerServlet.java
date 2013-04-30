package com.comcast.cmb.common.controller;

import java.awt.Color;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickMarkPosition;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.labels.StandardXYItemLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StackedXYBarRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.time.Hour;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeTableXYDataset;
import org.jfree.ui.RectangleEdge;

public class CMBVisualizerServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
    	String pathInfo = request.getPathInfo();

    	if (pathInfo == null) {
            return;
    	}

        if (pathInfo.toLowerCase().contains("responsetimeimg")) {
            doResponseTimeChart(request, response);
        } else if (pathInfo.toLowerCase().contains("calldistributionimg")) {
        	doApiPieChart(request, response);
        }
	}
	
    protected void doResponseTimeChart(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    	int resolutionMS = 10;
    	String action = null;
    	
    	if (request.getParameter("ac") != null) {
    		action = request.getParameter("ac");
    	} else if (request.getParameter("rs") != null) {
    		resolutionMS = Integer.parseInt(request.getParameter("rs"));
    	}
    	
    	byte b[] = generateResponseTimeChart(resolutionMS, action);
    	
    	response.setContentLength(b.length);
    	response.setContentType("image/jpeg");
    	response.getOutputStream().write(b);
    	response.flushBuffer();
    }

    protected void doApiPieChart(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    	byte b[] = generateApiDistributionPieChart();
    	
    	response.setContentLength(b.length);
    	response.setContentType("image/jpeg");
    	response.getOutputStream().write(b);
    	response.flushBuffer();
    }
	
    private byte[] generateApiDistributionPieChart() throws IOException {
    	
    	DefaultPieDataset piedataset = new DefaultPieDataset();
    	List<String> apis = new ArrayList<String>();
    	List<Long> counts = new ArrayList<Long>();
    	long total = 0;
    	String topApi = null;
    	long topCount = 0;

    	for (String api : CMBControllerServlet.callStats.keySet()) {
    		apis.add(api);
    		long count = CMBControllerServlet.callStats.get(api).longValue();
    		counts.add(new Long(count));
    		total += count;
    		if (count > topCount) {
    			topCount = count;
    			topApi = api;
    		}
    	}

    	for (int i=0; i<apis.size(); i++) {
    		piedataset.setValue(apis.get(i), 1.0*counts.get(i)/total);
    	}

    	JFreeChart chart = ChartFactory.createPieChart("API Call Distribution", piedataset, true, true, false);
    	PiePlot pieplot = (PiePlot)chart.getPlot();
    	
    	List<Color> palette = new ArrayList<Color>();
    	
    	palette.add(new Color(208,56,20));
    	palette.add(new Color(120,54,210));
    	palette.add(new Color(47,133,18));
    	palette.add(new Color(253,30,19));
    	palette.add(new Color(151,195,30));
    	palette.add(new Color(253,249,50));
    	palette.add(new Color(253,191,35));
    	palette.add(new Color(253,123,26));
    	palette.add(new Color(216,106,20));
    	palette.add(new Color(181,97,28));
    	
    	for (int i=0; i<apis.size(); i++) {
    		pieplot.setSectionPaint(apis.get(i), palette.get(i%palette.size()));
    	}
    	
    	pieplot.setNoDataMessage("No data available");
    	pieplot.setExplodePercent(topApi, 0.20000000000000001D);
    	
    	pieplot.setLabelGenerator(new StandardPieSectionLabelGenerator("{0} ({2} percent)"));
    	pieplot.setLabelBackgroundPaint(new Color(220, 220, 220));
    	pieplot.setLegendLabelToolTipGenerator(new StandardPieSectionLabelGenerator("Tooltip for legend item {0}"));
    	pieplot.setSimpleLabels(true);
    	pieplot.setInteriorGap(0.0D);

    	// generate jpeg

    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
    	ChartUtilities.writeChartAsJPEG(bos, chart, 2400, 400);

    	return bos.toByteArray();
    }

    private byte[] generateResponseTimeChart(int resolutionMS, String action) throws IOException {
    	
    	AtomicLong[][] responseTimesMS;
    	
    	if (action != null) {
    		
    		responseTimesMS = CMBControllerServlet.callResponseTimesByApi.get(action);
    		
    	} else {

    		if (resolutionMS == 1) {
	    		responseTimesMS = CMBControllerServlet.callResponseTimesMS;
	    	} else if (resolutionMS == 10) {
	    		responseTimesMS = CMBControllerServlet.callResponseTimes10MS;
	    	} else if (resolutionMS == 100) {
	    		responseTimesMS = CMBControllerServlet.callResponseTimes100MS;
	    	} else if (resolutionMS == 1000) {
	    		responseTimesMS = CMBControllerServlet.callResponseTimes1000MS;
	    	} else {
	    		responseTimesMS = CMBControllerServlet.callResponseTimes10MS;
	    	}
    	}

    	// clone and normalize data

    	long[][] rt = new long[CMBControllerServlet.NUM_MINUTES][CMBControllerServlet.NUM_BUCKETS];
    	long[] totals = new long[CMBControllerServlet.NUM_MINUTES];
    	long grandTotal = 0;
    	int activeMinutes = 0;
    	
    	for (int i=0; i<CMBControllerServlet.NUM_MINUTES; i++) {
    		for (int k=0; k<CMBControllerServlet.NUM_BUCKETS; k++) {
    			rt[i][k] = responseTimesMS[i][k].longValue();
    			totals[i] += rt[i][k];
    		}
			if (totals[i] > 0) {
				grandTotal += totals[i];
				activeMinutes++;
			}
    	}
    	
    	for (int i=0; i<CMBControllerServlet.NUM_MINUTES; i++) {
    		for (int k=0; k<CMBControllerServlet.NUM_BUCKETS; k++) {
    			if (totals[i] == 0) {
    				rt[i][k] = 0;
    			} else {
    				rt[i][k] = 100*rt[i][k]/totals[i]; 
    			}
    			
    		}
    	}
    	
    	// convert data for rendering
    	
        TimeTableXYDataset dataset = new TimeTableXYDataset();
        
    	for (int i=0; i<CMBControllerServlet.NUM_MINUTES; i++) {
    		for (int k=0; k<CMBControllerServlet.NUM_BUCKETS; k++) {
    			dataset.add(new Minute(i, new Hour()), rt[i][k], (resolutionMS*k)+"ms");
    		}
    	}
    	
    	// generate chart
    	
    	DateAxis domainAxis = new DateAxis("Minute");
    	domainAxis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
    	domainAxis.setLowerMargin(0.01);
    	domainAxis.setUpperMargin(0.01);
    	NumberAxis rangeAxis = new NumberAxis("Response Time");
    	rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
    	rangeAxis.setUpperMargin(0.10); // leave some space for item labels
    	StackedXYBarRenderer renderer = new StackedXYBarRenderer(0.15);
    	renderer.setDrawBarOutline(true);
    	renderer.setBaseItemLabelsVisible(true);
    	renderer.setBaseItemLabelGenerator(new StandardXYItemLabelGenerator());
    	
    	/*int r=0, g=200, b=0;
    	int inc = 200 / CMBControllerServlet.NUM_BUCKETS;
    	
    	for (int i=0; i<CMBControllerServlet.NUM_BUCKETS; i++) {
    		renderer.setSeriesPaint(i, new Color(r,g,b));
    		r+=inc;
    		g-=inc;
    	}*/
    	
    	renderer.setSeriesPaint(0, new Color(47,133,18));
    	renderer.setSeriesPaint(1, new Color(151,195,30));
    	renderer.setSeriesPaint(2, new Color(253,249,50));
    	renderer.setSeriesPaint(3, new Color(253,191,35));
    	renderer.setSeriesPaint(4, new Color(253,123,26));
    	renderer.setSeriesPaint(5, new Color(216,106,20));
    	renderer.setSeriesPaint(6, new Color(181,97,28));
    	renderer.setSeriesPaint(7, new Color(208,56,20));
    	renderer.setSeriesPaint(8, new Color(253,30,19));
    	renderer.setSeriesPaint(9, new Color(120,54,210));

    	XYPlot plot = new XYPlot(dataset, domainAxis, rangeAxis, renderer);
    	String label = "Response Time Percentiles";
    	
    	if (activeMinutes > 0) {
    		label = "Response Time Percentiles [" + grandTotal/(activeMinutes*60) + "msg/sec " + activeMinutes + " min " + grandTotal + " msgs]";
    	}
    	
    	JFreeChart chart = new JFreeChart(label, plot);
    	chart.removeLegend();
    	// chart.addSubtitle(new TextTitle(""));
    	LegendTitle legend = new LegendTitle(plot);
    	legend.setFrame(new BlockBorder());
    	legend.setPosition(RectangleEdge.BOTTOM);
    	chart.addSubtitle(legend);
    	
    	// generate jpeg
    	
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
    	ChartUtilities.writeChartAsJPEG(bos, chart, 2400, 400);
    	return bos.toByteArray();
    }
}
