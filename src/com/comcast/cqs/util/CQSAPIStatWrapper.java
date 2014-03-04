package com.comcast.cqs.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Row;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.model.CQSAPIStats;

public class CQSAPIStatWrapper {
	
	public static List<CQSAPIStats> getCNSAPIStats(){
		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCNSKeyspace());
		
		List<Row<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows("CNSAPIServers", null, 1000, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		List<CQSAPIStats> statsList = new ArrayList<CQSAPIStats>();
		
		if (rows != null) {
			
			for (Row<String, String, String> row : rows) {
				
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
	public static List<String> getCNSDataCenterNames(){
		List<CQSAPIStats> statsList = getCNSAPIStats();
		String localDataCenter = CMBProperties.getInstance().getCMBDataCenter();
		List <String> dataCenterList = new ArrayList<String>();
		dataCenterList.add(localDataCenter);
		Set <String> dataCenterNameSet = new HashSet<String>();
		for(CQSAPIStats currentCQSAPIStat : statsList){
			if((currentCQSAPIStat.getDataCenter()!=null)&&(!currentCQSAPIStat.getDataCenter().equals(localDataCenter))){
				dataCenterNameSet.add(currentCQSAPIStat.getDataCenter());
			}
		}

		dataCenterList.addAll(dataCenterNameSet);
		return dataCenterList;
	}
	
	//the first data center is the the local data center
	public static List<String> getCQSDataCenterNames(){
		List<CQSAPIStats> statsList = getCQSAPIStats();
		String localDataCenter = CMBProperties.getInstance().getCMBDataCenter();
		List <String> dataCenterList = new ArrayList<String>();
		dataCenterList.add(localDataCenter);
		Set <String> dataCenterNameSet = new HashSet<String>();
		for(CQSAPIStats currentCQSAPIStat : statsList){
			if((currentCQSAPIStat.getDataCenter()!=null)&&(!currentCQSAPIStat.getDataCenter().equals(localDataCenter))){
				dataCenterNameSet.add(currentCQSAPIStat.getDataCenter());
			}
		}

		dataCenterList.addAll(dataCenterNameSet);
		return dataCenterList;
	}
	
	
	
	public static List<CQSAPIStats> getCNSAPIStatsByDataCenter(String dataCenter){
		List<CQSAPIStats> cqsAPIStatsList = getCNSAPIStats();
		List<CQSAPIStats> cqsAPIStatsByDataCenterList = new ArrayList<CQSAPIStats>();
		for (CQSAPIStats currentCQSAPIStats: cqsAPIStatsList){
			if(currentCQSAPIStats.getDataCenter().equals(dataCenter)){
				cqsAPIStatsByDataCenterList.add(currentCQSAPIStats);
			}
		}
		return cqsAPIStatsByDataCenterList;
	}
	
	
	public static List<CQSAPIStats> getCQSAPIStatsByDataCenter(String dataCenter){
		List<CQSAPIStats> cqsAPIStatsList = getCQSAPIStats();
		List<CQSAPIStats> cqsAPIStatsByDataCenterList = new ArrayList<CQSAPIStats>();
		for (CQSAPIStats currentCQSAPIStats: cqsAPIStatsList){
			if(currentCQSAPIStats.getDataCenter().equals(dataCenter)){
				cqsAPIStatsByDataCenterList.add(currentCQSAPIStats);
			}
		}
		return cqsAPIStatsByDataCenterList;
	}
	public static List<CQSAPIStats> getCQSAPIStats(){
		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCQSKeyspace());
		
		List<Row<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows("CQSAPIServers", null, 1000, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		List<CQSAPIStats> statsList = new ArrayList<CQSAPIStats>();
		
		if (rows != null) {
			
			for (Row<String, String, String> row : rows) {
				
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
