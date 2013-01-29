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

	public static String getGetAPIStatsResponse(List<CQSAPIStats> stats) {

		String res = "";
		res += "<GetAPIStatsResponse>\n";
		res +=  "\t<GetAPIStatsResult>\n";

		for (CQSAPIStats s : stats) {
			
			res += "\t\t<Stats>\n";
			
			res += "\t\t\t<IpAddress>"+s.getIpAddress()+"</IpAddress>\n";
			res += "\t\t\t<JmxPort>"+s.getJmxPort()+"</JmxPort>\n";
			res += "\t\t\t<LongPollPort>"+s.getLongPollPort()+"</LongPollPort>\n";
			res += "\t\t\t<DataCenter>"+s.getDataCenter()+"</DataCenter>\n";
			res += "\t\t\t<ServiceUrl>"+s.getServiceUrl()+"</ServiceUrl>\n";
			res += "\t\t\t<Timestamp>"+s.getTimestamp()+"</Timestamp>\n";
			res += "\t\t\t<NumberOfLongPollReceives>"+s.getNumberOfLongPollReceives()+"</NumberOfLongPollReceives>\n";
			res += "\t\t\t<NumberOfRedisKeys>"+s.getNumberOfRedisKeys()+"</NumberOfRedisKeys>\n";
			res += "\t\t\t<NumberOfRedisShards>"+s.getNumberOfRedisShards()+"</NumberOfRedisShards>\n";
			
			res += "\t\t\t<CallStats>\n";
			
			if (s.getCallStats() != null) {
				for (String action : s.getCallStats().keySet()) {
					res += "\t\t\t\t<"+action+">"+s.getCallStats().get(action)+"</"+action+">\n";
				}
			}
			
			res += "\t\t\t</CallStats>\n";
			
			res += "\t\t\t<CallFailureStats>\n";
			
			if (s.getCallFailureStats() != null) {
				for (String action : s.getCallFailureStats().keySet()) {
					res += "\t\t\t\t<"+action+">"+s.getCallFailureStats().get(action)+"</"+action+">\n";
				}
			}
			
			res += "\t\t\t</CallFailureStats>\n";
			
			res += "\t\t</Stats>\n";
		}

		res += "\t</GetAPIStatsResult>\n";
		res += "</GetAPIStatsResponse>";

		return res;
	}
}
