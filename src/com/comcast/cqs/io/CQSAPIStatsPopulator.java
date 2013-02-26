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
package com.comcast.cqs.io;

import java.util.List;

import com.comcast.cqs.model.CQSAPIStats;

public class CQSAPIStatsPopulator {

	public static String getGetAPIStatsResponse(List<CQSAPIStats> statsList) {

		String out = "";
		out += "<GetAPIStatsResponse>\n";
		out +=  "\t<GetAPIStatsResult>\n";

		for (CQSAPIStats stats : statsList) {
			
			out += "\t\t<Stats>\n";
			
			out += "\t\t\t<IpAddress>"+stats.getIpAddress()+"</IpAddress>\n";
			out += "\t\t\t<JmxPort>"+stats.getJmxPort()+"</JmxPort>\n";
			out += "\t\t\t<LongPollPort>"+stats.getLongPollPort()+"</LongPollPort>\n";
			out += "\t\t\t<DataCenter>"+stats.getDataCenter()+"</DataCenter>\n";
			out += "\t\t\t<ServiceUrl>"+stats.getServiceUrl()+"</ServiceUrl>\n";
			out += "\t\t\t<Timestamp>"+stats.getTimestamp()+"</Timestamp>\n";
			out += "\t\t\t<NumberOfLongPollReceives>"+stats.getNumberOfLongPollReceives()+"</NumberOfLongPollReceives>\n";
			out += "\t\t\t<NumberOfRedisKeys>"+stats.getNumberOfRedisKeys()+"</NumberOfRedisKeys>\n";
			out += "\t\t\t<NumberOfRedisShards>"+stats.getNumberOfRedisShards()+"</NumberOfRedisShards>\n";
			
			if (stats.getRedisServerList() != null) {
				out += "\t\t\t<RedisServerList>"+stats.getRedisServerList()+"</RedisServerList>\n";
			}
			
			if (stats.getStatus() != null) {
				out += "\t\t\t<Status>"+stats.getStatus()+"</Status>\n";
			}
			
			if (stats.getCassandraClusterName() != null) {
				out += "\t\t\t<CassandraClusterName>"+stats.getCassandraClusterName()+"</CassandraClusterName>\n";
			}
			
			if (stats.getCassandraNodes() != null) { 
				out += "\t\t\t<CassandraNodes>"+stats.getCassandraNodes()+"</CassandraNodes>\n";
			}
			
			out += "\t\t\t<CallStats>\n";
			
			if (stats.getCallStats() != null) {
				for (String action : stats.getCallStats().keySet()) {
					out += "\t\t\t\t<"+action+">"+stats.getCallStats().get(action)+"</"+action+">\n";
				}
			}
			
			out += "\t\t\t</CallStats>\n";
			
			out += "\t\t\t<CallFailureStats>\n";
			
			if (stats.getCallFailureStats() != null) {
				for (String action : stats.getCallFailureStats().keySet()) {
					out += "\t\t\t\t<"+action+">"+stats.getCallFailureStats().get(action)+"</"+action+">\n";
				}
			}
			
			out += "\t\t\t</CallFailureStats>\n";
			
			out += "\t\t</Stats>\n";
		}

		out += "\t</GetAPIStatsResult>\n";
		out += "</GetAPIStatsResponse>";

		return out;
	}
}
