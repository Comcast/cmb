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

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.HConsistencyLevel;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.util.Util;

public class CQSTestUtils {

	/**D
	 * @param args
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 */
	public static void main(String[] args) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		// TODO Auto-generated method stub
		if (args == null) {
			System.out.println("Usage: <action> <args>.  Valid actions are GetQueueCount with the queue url");
		}
		if (args[0].equals("GetQueueCount")) {
			if (args.length < 2) {
				System.out.println("Missing argument queueurl");
				return;
			}
			getQueueCount(args[1]);			
		}

	}

	private static void getQueueCount(String queueUrl) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		int numberOfPartitions = CMBProperties.getInstance().getCQSNumberOfQueuePartitions();
		CassandraPersistence persistence = new CassandraPersistence(CMBProperties.getInstance().getCQSKeyspace());
		String queueHash = Util.hashQueueUrl(queueUrl);
		long messageCount = 0;
		for (int i=0; i<numberOfPartitions; i++) {
			String queueKey = queueHash + "_" + i;
			long partitionCount = persistence.getCount("CQSPartitionedQueueMessages", queueKey, StringSerializer.get(), new CompositeSerializer(), HConsistencyLevel.QUORUM);
			messageCount += partitionCount;
			System.out.println("# of messages in " + queueKey + " =" + partitionCount);
		}
		
		System.out.println("There are " + messageCount + " messages in queue " + queueUrl);
	}

}
