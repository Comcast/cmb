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
package com.comcast.cns.io;

import java.util.List;

import com.comcast.cns.model.CNSWorkerStats;

public class CNSWorkerStatsPopulator {

	public static String getGetWorkerStatsResponse(List<CNSWorkerStats> statsList) {

		String out = "";
		out += "<GetWorkerStatsResponse>\n";
		out +=  "\t<GetWorkerStatsResult>\n";

		for (CNSWorkerStats stats : statsList) {
			out += "\t\t<Stats>\n";
			out += "\t\t\t<IpAddress>"+stats.getIpAddress()+"</IpAddress>\n";
			out += "\t\t\t<JmxPort>"+stats.getJmxPort()+"</JmxPort>\n";
			out += "\t\t\t<Mode>"+stats.getMode()+"</Mode>\n";
			out += "\t\t\t<DataCenter>"+stats.getDataCenter()+"</DataCenter>\n";
			out += "\t\t\t<NumPublishedMessages>"+stats.getNumPublishedMessages()+"</NumPublishedMessages>\n";
			out += "\t\t\t<ProducerTimestamp>"+stats.getProducerTimestamp()+"</ProducerTimestamp>\n";
			out += "\t\t\t<ActiveProducer>"+stats.isProducerActive()+"</ActiveProducer>\n";
			out += "\t\t\t<ConsumerTimestamp>"+stats.getConsumerTimestamp()+"</ConsumerTimestamp>\n";
			out += "\t\t\t<ActiveConsumer>"+stats.isConsumerActive()+"</ActiveConsumer>\n";
			out += "\t\t\t<DeliveryQueueSize>"+stats.getDeliveryQueueSize()+"</DeliveryQueueSize>\n";
			out += "\t\t\t<RedeliveryQueueSize>"+stats.getRedeliveryQueueSize()+"</RedeliveryQueueSize>\n";
			out += "\t\t\t<ConsumerOverloaded>"+stats.isConsumerOverloaded()+"</ConsumerOverloaded>\n";
			out += "\t\t\t<CqsServiceAvailable>"+stats.isCqsServiceAvailable()+"</CqsServiceAvailable>\n";
			out += "\t\t\t<NumPooledHttpConnections>"+stats.getNumPooledHttpConnections()+"</NumPooledHttpConnections>\n";

			if (stats.getErrorCountForEndpoints() != null && stats.getErrorCountForEndpoints().size() > 0) {
				
				out += "\t\t\t<ErrorCountForEndpoints>";
				
				for (String endpoint : stats.getErrorCountForEndpoints().keySet()) {
					out += "\t\t\t\t<Error endpoint=\""+endpoint+"\" count=\""+stats.getErrorCountForEndpoints().get(endpoint)+"\"/>";
				}

				out += "\t\t\t</ErrorCountForEndpoints>";
			}
			
			out += "\t\t</Stats>\n";
		}

		out += "\t</GetWorkerStatsResult>\n";
		out += "</GetWorkerStatsResponse>";

		return out;
	}

	public static String getGetManageWorkerResponse() {

		String res = "";
		
		res += "<GetManageWorkerResponse>\n";
		res += "\t" + CNSPopulator.getResponseMetadata() + "\n";
		res += "</GetManageWorkerResponse>";

		return res;
	}
}
