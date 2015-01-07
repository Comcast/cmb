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
package com.comcast.cmb.test.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import com.comcast.cmb.common.util.Util;

public class QueueDepthSimulator {
	
    private static Logger logger = Logger.getLogger(QueueDepthSimulator.class);
    
    private static double deviationPct = 0.05;
    private static double transferFrequency = 0.1;
    private static int currentTime = 0;
    private static int duration = 3600;
    private static Random rand = new Random();
    
    private static long allAOut = 0;
    private static long allBOut = 0;
    
    public static Map<Integer, List<Event>> events = new HashMap<Integer, List<Event>>();
    public static List<SimpleQueue> queues = new ArrayList<SimpleQueue>();
    public static Map<String, List<Double>> logs = new HashMap<String, List<Double>>();
    public static TransferStrategy transferStrategy = new NopTransferStrategy();
    
    public static class Event {
    	public int sendRateChange;
    	public int receiveRateChange;
    	public SimpleQueue q;
    	public Event(SimpleQueue q, int sendRateChange, int receiveRateChange) {
    		this.q = q;
    		this.sendRateChange = sendRateChange;
    		this.receiveRateChange = receiveRateChange;
    	}
    }
    
    private static void clear() {
    	currentTime = 0;
    	allAOut = 0;
    	allBOut = 0;
    	events = new HashMap<Integer, List<Event>>();
    	queues = new ArrayList<SimpleQueue>();
    	logs = new HashMap<String, List<Double>>();
    	transferStrategy = new NopTransferStrategy();
    }
    
    public static abstract class TransferStrategy {
    	public abstract void transfer(SimpleQueue fromQ, SimpleQueue toQ);
    }
    
    public static class NopTransferStrategy extends TransferStrategy {
		@Override
		public void transfer(SimpleQueue fromQ, SimpleQueue toQ) {
		}
    }
    
    public static class DepthOnlyTransferStrategy extends TransferStrategy {
    	private static int MAX = 1000;
		@Override
		public void transfer(SimpleQueue fromQ, SimpleQueue toQ) {
			if (fromQ.numMsg >= 10*toQ.numMsg) {
				fromQ.lastTransferOut = Math.min(MAX, (int)(0.5*(fromQ.numMsg-toQ.numMsg)));
				fromQ.numMsg -= fromQ.lastTransferOut;
				toQ.lastTransferIn = fromQ.lastTransferOut;
				toQ.numMsg += toQ.lastTransferIn;
			}
		}
    }

    public static class DepthAndRateStrategy extends TransferStrategy {
    	private static int MAX = 1000;
		@Override
		public void transfer(SimpleQueue fromQ, SimpleQueue toQ) {
			if (fromQ.numMsg >= 10*toQ.numMsg) {
				if (fromQ.lastSend/(fromQ.lastReceive+1.0)>1.1 && fromQ.lastSend/(fromQ.lastReceive+1.0)>toQ.lastSend/(toQ.lastReceive+1.0)) {
					fromQ.lastTransferOut = Math.min(MAX, (int)(0.5*(fromQ.numMsg-toQ.numMsg)));
					fromQ.numMsg -= fromQ.lastTransferOut;
					toQ.lastTransferIn = fromQ.lastTransferOut;
					toQ.numMsg += toQ.lastTransferIn;
				}
			}
		}
    }

    public static class SimpleQueue {
    	
    	public String name = null;
    	public int numMsg = 0;
    	public int sendRate = 0;
    	public int receiveRate = 0;

    	public int lastTransferIn = 0;
    	public int lastTransferOut = 0;
    	private int lastSend = 0;
    	private int lastReceive = 0;
    	
    	private int jitter(int rate) {
    		int jitter = rand.nextInt((int)(deviationPct*rate+1));
    		if (rand.nextDouble() <= 0.5) {
    			jitter = -1*jitter;
    		}
    		return rate+(int)jitter;
    	}
    	
    	public void iterate() {
    		lastSend = jitter(sendRate);
			numMsg += lastSend;
			lastReceive = jitter(receiveRate);
			if (numMsg < lastReceive) {
				lastReceive = numMsg;
			}
			numMsg -= lastReceive;
    	}
    	
    	public void log(int ts) {
    		logger.info("event=" + name + " num_msg=" + numMsg + " last_send=" + lastSend + " last_receive=" + lastReceive + " change_rate=" + (lastSend/(lastReceive+1.0)) + " send_rate=" + sendRate + " receiveRate=" + receiveRate + " ts=" + ts + " t_in=" + lastTransferIn + " t_out=" + lastTransferOut);
    		if (logs.containsKey(name + "_depth")) {
    			logs.get(name + "_depth").add((double)numMsg);
    		}
    		if (logs.containsKey(name + "_lastsend")) {
    			logs.get(name + "_lastsend").add((double)lastSend);
    		}
    		if (logs.containsKey(name + "_lastreceive")) {
    			logs.get(name + "_lastreceive").add((double)lastReceive);
    		}
    		if (logs.containsKey(name + "_tout")) {
    			logs.get(name + "_tout").add((double)lastTransferOut);
    		}
    		if (logs.containsKey(name + "_rate")) {
    			logs.get(name + "_rate").add(lastSend/(lastReceive+1.0));
    		}
    		if (name.equals("qA")) {
    			allAOut += lastTransferOut;
    		}
    		if (name.equals("qB")) {
    			allBOut += lastTransferOut;
    		}
    	}
    }
    
    private static void addEvent(int ts, Event e) { 
    	if (!events.containsKey(ts)) {
    		events.put(ts, new ArrayList<Event>());
    	}
    	events.get(ts).add(e);
    }
    
    public static void simulate() {
		for (;currentTime<=duration;currentTime++) {
			List<Event> l = events.get(currentTime);
			if (l != null) {
				for (Event e : l) {
					e.q.sendRate += e.sendRateChange;
					e.q.receiveRate += e.receiveRateChange;
				}
			}
	    	if (queues.size() == 2) {
	    		if (rand.nextDouble() <= transferFrequency) {
	    			transferStrategy.transfer(queues.get(0), queues.get(1));
	    		} else if (rand.nextDouble() <= transferFrequency) {
	    			transferStrategy.transfer(queues.get(1), queues.get(0));
	    		} else {
	    			queues.get(0).lastTransferIn = 0;
	    			queues.get(0).lastTransferOut = 0;
	    			queues.get(1).lastTransferIn = 0;
	    			queues.get(1).lastTransferOut = 0;
	    		}
	    	}
	    	for (SimpleQueue q : queues) {
				q.iterate();
				q.log(currentTime);
	    	}
		}
    }
    
	public static void scatterPlot(Map<String, List<Double>> datasets, String filename, String title, String labelX, String labelY) throws IOException {
		XYSeriesCollection dataset = new XYSeriesCollection();
		for (String label : datasets.keySet()) {
			XYSeries data = new XYSeries(label);
			int cnt = 0;
			for (Double d : datasets.get(label)) {
				data.add(cnt, d);
				cnt++;
			}
			dataset.addSeries(data);
		}
	    JFreeChart chart = ChartFactory.createScatterPlot(title, labelX, labelY, dataset, PlotOrientation.VERTICAL, true, true, false);
	    ChartUtilities.saveChartAsPNG(new File(filename), chart, 1024, 768);
	}
	
	public static void plotLineChart(Map<String, List<Double>> series, String filename, String title, String labelX, String labelY) throws IOException {
		XYSeriesCollection dataset = new XYSeriesCollection();
		for (String label : series.keySet()) {
			XYSeries data = new XYSeries(label);
			int t = 0;
			for (Double d : series.get(label)) {
				data.add(t, d);
				t++;
			}
			dataset.addSeries(data);
		}
        JFreeChart chart = ChartFactory.createXYLineChart(title, labelX, labelY, dataset, PlotOrientation.VERTICAL, true, true, false);
        XYPlot plot = chart.getXYPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesLinesVisible(0, true);
        renderer.setSeriesShapesVisible(0, true);
        plot.setRenderer(renderer);
    	ChartUtilities.saveChartAsPNG(new File(filename), chart, 1024, 768);   	
	}

	private static void configureQueueLogging(String queueName) {
		//logs.put(queueName + "_depth", new ArrayList<Double>());
		logs.put(queueName + "_lastsend", new ArrayList<Double>());
		logs.put(queueName + "_lastreceive", new ArrayList<Double>());
		logs.put(queueName + "_tout", new ArrayList<Double>());
		//logs.put(queueName + "_rate", new ArrayList<Double>());
	}
	
	public static void setupSingleDCSpikyQueue() {

		SimpleQueue qA = new SimpleQueue();
		qA.name = "qA";
		qA.sendRate = 1000;
		qA.receiveRate = 10000;
		
		queues.add(qA);
		
		configureQueueLogging(qA.name);
		
		Event e1 = new Event(qA, 20000, 0);
		addEvent(1000, e1);
		Event e2 = new Event(qA, -20000, 0);
		addEvent(1020, e2);

		Event e3 = new Event(qA, 20000, 0);
		addEvent(2000, e3);
		Event e4 = new Event(qA, -20000, 0);
		addEvent(2020, e4);

		Event e5 = new Event(qA, 20000, 0);
		addEvent(3000, e5);
		Event e6 = new Event(qA, -20000, 0);
		addEvent(3020, e6);
	}

	public static void setupSingleDCQueueAtCapacity() {

		SimpleQueue qA = new SimpleQueue();
		qA.name = "qA";
		qA.sendRate = 1000;
		qA.receiveRate = 1010;
		
		queues.add(qA);
		
		configureQueueLogging(qA.name);
	}

	public static void setupDualDCQueueAtCapacity() {
		
		SimpleQueue qA = new SimpleQueue();
		qA.name = "qA";
		qA.sendRate = 1000;
		qA.receiveRate = 1010;
		
		queues.add(qA);

		configureQueueLogging(qA.name);

		SimpleQueue qB = new SimpleQueue();
		qB.name = "qB";
		qB.sendRate = 1000;
		qB.receiveRate = 1010;

		queues.add(qB);
		
		configureQueueLogging(qB.name);
	}

	public static void setupDualDCQueueDiminishedCapacity() {
		
		SimpleQueue qA = new SimpleQueue();
		qA.name = "qA";
		qA.sendRate = 1000;
		qA.receiveRate = 1500;
		
		queues.add(qA);

		configureQueueLogging(qA.name);

		SimpleQueue qB = new SimpleQueue();
		qB.name = "qB";
		qB.sendRate = 1000;
		qB.receiveRate = 1500;

		queues.add(qB);
		
		configureQueueLogging(qB.name);
		
		Event e1 = new Event(qA, 0, -750);
		addEvent(1000, e1);
		Event e2 = new Event(qA, 0, 750);
		addEvent(2000, e2);
	}
	
	public static void setupDualDCQueueFullFailover() {
		
		SimpleQueue qA = new SimpleQueue();
		qA.name = "qA";
		qA.sendRate = 1000;
		qA.receiveRate = 1500;
		
		queues.add(qA);

		configureQueueLogging(qA.name);

		SimpleQueue qB = new SimpleQueue();
		qB.name = "qB";
		qB.sendRate = 1000;
		qB.receiveRate = 1500;

		queues.add(qB);
		
		configureQueueLogging(qB.name);
		
		Event e1 = new Event(qA, 0, -1500);
		addEvent(1000, e1);
	}
	
	public static void setupDualDCSpikyQueue() {

		SimpleQueue qA = new SimpleQueue();
		qA.name = "qA";
		qA.sendRate = 1000;
		qA.receiveRate = 10000;
		
		queues.add(qA);
		
		//configureQueueLogging(qA.name);
		
		Event e1 = new Event(qA, 20000, 0);
		addEvent(1000, e1);
		Event e2 = new Event(qA, -20000, 0);
		addEvent(1020, e2);

		Event e3 = new Event(qA, 20000, 0);
		addEvent(2000, e3);
		Event e4 = new Event(qA, -20000, 0);
		addEvent(2020, e4);

		Event e5 = new Event(qA, 20000, 0);
		addEvent(3000, e5);
		Event e6 = new Event(qA, -20000, 0);
		addEvent(3020, e6);

		SimpleQueue qB = new SimpleQueue();
		qB.name = "qB";
		qB.sendRate = 1000;
		qB.receiveRate = 10000;
		
		queues.add(qB);
		
		configureQueueLogging(qB.name);
		
		Event e7 = new Event(qB, 20000, 0);
		addEvent(1000, e7);
		Event e8 = new Event(qB, -20000, 0);
		addEvent(1020, e8);

		Event e9 = new Event(qB, 20000, 0);
		addEvent(2000, e9);
		Event e10 = new Event(qB, -20000, 0);
		addEvent(2020, e10);

		Event e11 = new Event(qB, 20000, 0);
		addEvent(3000, e11);
		Event e12 = new Event(qB, -20000, 0);
		addEvent(3020, e12);
	}

	
	public static void main(String argv[]) {
		
		try {
		
			Util.initLog4jTest();
			
			logger.info("event=simulator_started");
			transferStrategy = new DepthOnlyTransferStrategy();
			setupDualDCQueueAtCapacity();
			simulate();
			plotLineChart(logs, "/tmp/qsim_atcap_qdepth.png", "Queue Sim At Capacity Queue Depth Strategy", "ts", "count");
			logger.info("event=done all_out_a=" + allAOut + " all_out_b=" + allBOut);
			clear();
			
			logger.info("event=simulator_started");
			transferStrategy = new DepthOnlyTransferStrategy();
			setupDualDCQueueDiminishedCapacity();
			simulate();
			plotLineChart(logs, "/tmp/qsim_dimcap_qdepth.png", "Queue Sim Diminshed Capacity Queue Depth Strategy", "ts", "count");
			logger.info("event=done all_out_a=" + allAOut + " all_out_b=" + allBOut);
			clear();

			logger.info("event=simulator_started");
			transferStrategy = new DepthOnlyTransferStrategy();
			setupDualDCQueueFullFailover();
			simulate();
			plotLineChart(logs, "/tmp/qsim_fail_qdepth.png", "Queue Sim Failover Queue Depth Strategy", "ts", "count");
			logger.info("event=done all_out_a=" + allAOut + " all_out_b=" + allBOut);
			clear();

			logger.info("event=simulator_started");
			transferStrategy = new DepthAndRateStrategy();
			setupDualDCQueueAtCapacity();
			simulate();
			plotLineChart(logs, "/tmp/qsim_atcap_qdepthandrate.png", "Queue Sim At Capacity Queue Depth and Rate Strategy", "ts", "count");
			logger.info("event=done all_out_a=" + allAOut + " all_out_b=" + allBOut);
			clear();
			
			logger.info("event=simulator_started");
			transferStrategy = new DepthAndRateStrategy();
			setupDualDCQueueDiminishedCapacity();
			simulate();
			plotLineChart(logs, "/tmp/qsim_dimcap_qdepthandrate.png", "Queue Sim Diminshed Capacity Queue Depth and Rate Strategy", "ts", "count");
			logger.info("event=done all_out_a=" + allAOut + " all_out_b=" + allBOut);
			clear();

			logger.info("event=simulator_started");
			transferStrategy = new DepthAndRateStrategy();
			setupDualDCQueueFullFailover();
			simulate();
			plotLineChart(logs, "/tmp/qsim_fail_qdepthandrate.png", "Queue Sim Failover Queue Depth and Rate Strategy", "ts", "count");
			logger.info("event=done all_out_a=" + allAOut + " all_out_b=" + allBOut);
			clear();

		} catch (Exception ex) {
			logger.error("", ex);
		}
	}
}
