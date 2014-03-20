package com.comcast.cqs.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.comcast.cmb.common.persistence.AbstractCassandraPersistence;
import com.comcast.cmb.common.persistence.AbstractCassandraPersistence.CmbRow;
import com.comcast.cmb.common.persistence.CassandraPersistenceFactory;
import com.comcast.cmb.common.persistence.AbstractCassandraPersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.model.CQSAPIStats;

public class CQSAPIStatWrapper {
	
	public static final String CNS_API_SERVERS = "CNSAPIServers";
	public static final String CQS_API_SERVERS = "CQSAPIServers";
	
	public static List<CQSAPIStats> getCNSAPIStats() throws PersistenceException{

		AbstractCassandraPersistence cassandraHandler = CassandraPersistenceFactory.getInstance(CMBProperties.getInstance().getCNSKeyspace());
		
		List<CmbRow<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows(CMBProperties.getInstance().getCNSKeyspace(), CNS_API_SERVERS, null, 1000, 10, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		List<CQSAPIStats> statsList = new ArrayList<CQSAPIStats>();
		
		if (rows != null) {
			
			for (CmbRow<String, String, String> row : rows) {
				
				CQSAPIStats stats = new CQSAPIStats();
				stats.setIpAddress(row.getKey());
				
				if (row.getColumnSlice().getColumnByName("timestamp") != null) {
					stats.setTimestamp(Long.parseLong(row.getColumnSlice().getColumnByName("timestamp").getValue()));
				}
				
				if (row.getColumnSlice().getColumnByName("jmxport") != null) {
					stats.setJmxPort(Long.parseLong(row.getColumnSlice().getColumnByName("jmxport").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("dataCenter") != null) {
					stats.setDataCenter(row.getColumnSlice().getColumnByName("dataCenter").getValue());
				}
				
				if (row.getColumnSlice().getColumnByName("serviceUrl") != null) {
					stats.setServiceUrl(row.getColumnSlice().getColumnByName("serviceUrl").getValue());
				}

				if (stats.getIpAddress().contains(":")) {
					statsList.add(stats);
				}
			}
		}
		return statsList;
	}
	
	//the first data center is the the local data center
	
	public static List<String> getCNSDataCenterNames() throws PersistenceException {
		
		List<CQSAPIStats> statsList = getCNSAPIStats();
		String localDataCenter = CMBProperties.getInstance().getCMBDataCenter();
		List <String> dataCenterList = new ArrayList<String>();
		dataCenterList.add(localDataCenter);
		Set <String> dataCenterNameSet = new HashSet<String>();
		
		for (CQSAPIStats currentCQSAPIStat : statsList) {
			if ((currentCQSAPIStat.getDataCenter()!=null)&&(!currentCQSAPIStat.getDataCenter().equals(localDataCenter))) {
				dataCenterNameSet.add(currentCQSAPIStat.getDataCenter());
			}
		}

		dataCenterList.addAll(dataCenterNameSet);
		return dataCenterList;
	}
	
	//the first data center is the the local data center
	public static List<String> getCQSDataCenterNames() throws PersistenceException {
		
		List<CQSAPIStats> statsList = getCQSAPIStats();
		String localDataCenter = CMBProperties.getInstance().getCMBDataCenter();
		List <String> dataCenterList = new ArrayList<String>();
		dataCenterList.add(localDataCenter);
		Set <String> dataCenterNameSet = new HashSet<String>();
		
		for (CQSAPIStats currentCQSAPIStat : statsList) {
			if ((currentCQSAPIStat.getDataCenter()!=null) && (!currentCQSAPIStat.getDataCenter().equals(localDataCenter))) {
				dataCenterNameSet.add(currentCQSAPIStat.getDataCenter());
			}
		}

		dataCenterList.addAll(dataCenterNameSet);
		return dataCenterList;
	}
	
	public static List<CQSAPIStats> getCNSAPIStatsByDataCenter(String dataCenter) throws PersistenceException {
		List<CQSAPIStats> cqsAPIStatsList = getCNSAPIStats();
		List<CQSAPIStats> cqsAPIStatsByDataCenterList = new ArrayList<CQSAPIStats>();
		
		for (CQSAPIStats currentCQSAPIStats: cqsAPIStatsList) {
			if (currentCQSAPIStats.getDataCenter().equals(dataCenter)) {
				cqsAPIStatsByDataCenterList.add(currentCQSAPIStats);
			}
		}
		
		return cqsAPIStatsByDataCenterList;
	}
	
	public static List<CQSAPIStats> getCQSAPIStatsByDataCenter(String dataCenter) throws PersistenceException {
		List<CQSAPIStats> cqsAPIStatsList = getCQSAPIStats();
		List<CQSAPIStats> cqsAPIStatsByDataCenterList = new ArrayList<CQSAPIStats>();
		for (CQSAPIStats currentCQSAPIStats: cqsAPIStatsList) {
			if (currentCQSAPIStats.getDataCenter().equals(dataCenter)) {
				cqsAPIStatsByDataCenterList.add(currentCQSAPIStats);
			}
		}
		return cqsAPIStatsByDataCenterList;
	}
	
	public static List<CQSAPIStats> getCQSAPIStats() throws PersistenceException {
		
		AbstractCassandraPersistence cassandraHandler = CassandraPersistenceFactory.getInstance(CMBProperties.getInstance().getCQSKeyspace());
		
		List<CmbRow<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows(CMBProperties.getInstance().getCQSKeyspace(), CQS_API_SERVERS, null, 1000, 10, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
		List<CQSAPIStats> statsList = new ArrayList<CQSAPIStats>();
		
		if (rows != null) {
			
			for (CmbRow<String, String, String> row : rows) {
				
				CQSAPIStats stats = new CQSAPIStats();
				stats.setIpAddress(row.getKey());
				
				if (row.getColumnSlice().getColumnByName("timestamp") != null) {
					stats.setTimestamp(Long.parseLong(row.getColumnSlice().getColumnByName("timestamp").getValue()));
				}
				
				if (row.getColumnSlice().getColumnByName("jmxport") != null) {
					stats.setJmxPort(Long.parseLong(row.getColumnSlice().getColumnByName("jmxport").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("port") != null) {
					stats.setLongPollPort(Long.parseLong(row.getColumnSlice().getColumnByName("port").getValue()));
				}

				if (row.getColumnSlice().getColumnByName("dataCenter") != null) {
					stats.setDataCenter(row.getColumnSlice().getColumnByName("dataCenter").getValue());
				}
				
				if (row.getColumnSlice().getColumnByName("serviceUrl") != null) {
					stats.setServiceUrl(row.getColumnSlice().getColumnByName("serviceUrl").getValue());
				}
				
				if (row.getColumnSlice().getColumnByName("redisServerList") != null) {
					stats.setRedisServerList(row.getColumnSlice().getColumnByName("redisServerList").getValue());
				}

				if (stats.getIpAddress().contains(":")) {
					statsList.add(stats);
				}
			}
		}
		return statsList;
	}
}
