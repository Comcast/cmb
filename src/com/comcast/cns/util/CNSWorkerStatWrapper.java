package com.comcast.cns.util;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbRow;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.model.CNSWorkerStats;
import com.comcast.cns.tools.CNSWorkerMonitor;
import com.comcast.cns.tools.CNSWorkerMonitorMBean;

public class CNSWorkerStatWrapper {
	
	private static Logger logger = Logger.getLogger(CNSWorkerStatWrapper.class);
	private static AbstractDurablePersistence cassandraHandler = DurablePersistenceFactory.getInstance();
	private static final String CNS_WORKERS = "CNSWorkers";
	public static List<CNSWorkerStats> getCassandraWorkerStats() throws PersistenceException {

		List<CmbRow<String, String, String>> rows = cassandraHandler.readNextNRows(AbstractDurablePersistence.CNS_KEYSPACE, CNS_WORKERS, null, 1000, 10, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		List<CNSWorkerStats> statsList = new ArrayList<CNSWorkerStats>();

		if (rows != null) {

			for (CmbRow<String, String, String> row : rows) {

				CNSWorkerStats stats = new CNSWorkerStats();

				stats.setIpAddress(row.getKey());

				if (row.getColumnSlice().getColumnByName("producerTimestamp") != null) {
					stats.setProducerTimestamp(Long.parseLong(row.getColumnSlice().getColumnByName("producerTimestamp").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("consumerTimestamp") != null) {
					stats.setConsumerTimestamp(Long.parseLong(row.getColumnSlice().getColumnByName("consumerTimestamp").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("jmxport") != null) {
					stats.setJmxPort(Long.parseLong(row.getColumnSlice().getColumnByName("jmxport").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("mode") != null) {
					stats.setMode(row.getColumnSlice().getColumnByName("mode").getValue());
				}

				if (row.getColumnSlice().getColumnByName("dataCenter") != null) {
					stats.setDataCenter(row.getColumnSlice().getColumnByName("dataCenter").getValue());
				}
				statsList.add(stats);
			}
		}

		return statsList;
	}

	public static List<CNSWorkerStats> getCassandraWorkerStatsByDataCenter(String dataCenter) throws PersistenceException {
		List<CNSWorkerStats> cnsWorkerStatsList = getCassandraWorkerStats();
		List<CNSWorkerStats> cnsWorkerStatsByDataCenterList = new ArrayList<CNSWorkerStats>();
		for (CNSWorkerStats currentWorkerStats: cnsWorkerStatsList) {
			if (currentWorkerStats.getDataCenter().equals(dataCenter)) {
				cnsWorkerStatsByDataCenterList.add(currentWorkerStats);
			}
		}
		return cnsWorkerStatsByDataCenterList;
	}

	private static void callOperation(String operation, List<CNSWorkerStats> cnsWorkerStats) throws Exception{

		//register JMX Bean
		
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
		ObjectName name = new ObjectName("com.comcast.cns.tools:type=CNSWorkerMonitorMBean");

		if (!mbs.isRegistered(name)) {
			mbs.registerMBean(CNSWorkerMonitor.getInstance(), name);
		}

		if ((operation!=null)&&(operation.equals("startWorker")||operation.equals("stopWorker"))) {
			JMXConnector jmxConnector = null;
			String url = null;
			String host = null;
			long port = 0;
			for (CNSWorkerStats stats:cnsWorkerStats) {
				try {

					host = stats.getIpAddress(); 
					port = stats.getJmxPort();
					url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";

					JMXServiceURL serviceUrl = new JMXServiceURL(url);
					jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);

					MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
					ObjectName cnsWorkerMonitor = new ObjectName("com.comcast.cns.tools:type=CNSWorkerMonitorMBean");
					CNSWorkerMonitorMBean mbeanProxy = JMX.newMBeanProxy(mbeanConn, cnsWorkerMonitor, CNSWorkerMonitorMBean.class, false);
					if (operation.equals("startWorker")) {
						mbeanProxy.startCNSWorkers();
					} else {
						mbeanProxy.stopCNSWorkers();
					}
				} catch (Exception e) {
					logger.error("event=error_in_"+operation+" Hose:"+host+" port:"+port+"Exception: "+e);
					String operationString = null;
					if(operation.equals("startWorker")){
						operationString = "start";
					} else {
						operationString = "stop";
					}
					throw new CMBException(CMBErrorCodes.InternalError, "Cannot " + operationString + " CNS workers");
				} finally {

					if (jmxConnector != null) {
						jmxConnector.close();
					}
				}
			}
		}
	}

	public static void stopWorkers(List<CNSWorkerStats> cnsWorkersList) throws Exception{
		callOperation("stopWorker", cnsWorkersList);
	}

	public static void startWorkers(List<CNSWorkerStats> cnsWorkersList)throws Exception{
		callOperation("startWorker", cnsWorkersList);
	}

	public static void startWorkers(String dataCenter) throws Exception{
		List<CNSWorkerStats> cnsWorkerStarts = getCassandraWorkerStatsByDataCenter(dataCenter);
		callOperation("startWorker", cnsWorkerStarts);
	}

	public static void stopWorkers(String dataCenter) throws Exception{
		List<CNSWorkerStats> cnsWorkerStarts = getCassandraWorkerStatsByDataCenter(dataCenter);
		callOperation("stopWorker", cnsWorkerStarts);
	}
}
