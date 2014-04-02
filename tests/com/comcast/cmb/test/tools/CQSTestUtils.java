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

import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.util.Util;

public class CQSTestUtils {

	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 * @throws PersistenceException 
	 */
	public static void main(String[] args) throws NoSuchAlgorithmException, UnsupportedEncodingException, PersistenceException {
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

	private static void getQueueCount(String queueUrl) throws NoSuchAlgorithmException, UnsupportedEncodingException, PersistenceException {
		int numberOfPartitions = CMBProperties.getInstance().getCQSNumberOfQueuePartitions();
		String queueHash = Util.hashQueueUrl(queueUrl);
		long messageCount = 0;
		for (int i=0; i<numberOfPartitions; i++) {
			String queueKey = queueHash + "_" + i;
			long partitionCount = DurablePersistenceFactory.getInstance().getCount(CMBProperties.getInstance().getCQSKeyspace(), "CQSPartitionedQueueMessages", queueKey, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.COMPOSITE_SERIALIZER);
			messageCount += partitionCount;
			System.out.println("# of messages in " + queueKey + " =" + partitionCount);
		}
		
		System.out.println("There are " + messageCount + " messages in queue " + queueUrl);
	}
}
