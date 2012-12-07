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

	public static String getGetWorkerStatsResponse(List<CNSWorkerStats> stats) {

		String res = "";
		res += "<GetWorkerStatsResponse>\n";
		res +=  "\t<GetWorkerStatsResult>\n";

		for (CNSWorkerStats s : stats) {
			res += "\t\t<Stats>\n";
			res += "\t\t\t<IpAddress>"+s.getIpAddress()+"</IpAddress>\n";
			res += "\t\t\t<Timestamp>"+s.getTimestamp()+"</Timestamp>\n";
			res += "\t\t\t<Active>"+s.isActive()+"</Active>\n";
			res += "\t\t\t<DeliveryQueueSize>"+s.getDeliveryQueueSize()+"</DeliveryQueueSize>\n";
			res += "\t\t\t<RedeliveryQueueSize>"+s.getRedeliveryQueueSize()+"</RedeliveryQueueSize>\n";
			res += "\t\t\t<ConsumerOverloaded>"+s.isConsumerOverloaded()+"</ConsumerOverloaded>\n";
			res += "\t\t</Stats>\n";
		}

		res += "\t</GetWorkerStatsResult>\n";
		res += "</GetWorkerStatsResponse>";

		return res;
	}
}
