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
package com.comcast.cqs.test.stress;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.util.CMBProperties;

/**
 * Test class
 * @author bwolf
 *
 */
public class CQSStressTestProperties {

    private final int testDurationSeconds;
    private final int messagesPerQueuePerSecond;
    private final int numberOfQueues;
    private final int numberOfSendersPerQueue;
    private final int numberOfReceiversPerQueue;
    private final int sendMessageBatchSize;
    private final int receiveMessageBatchSize;
    private final int delayBetweenSendsMS;
    private final int delayBetweenReceivesMS;
    private final int delayBetweenReceiveAndDeleteMS;
    private final int revisiblePercentage;
    private final String[] queueNames;

	private static final CQSStressTestProperties instance = new CQSStressTestProperties();

    private static final Logger logger = Logger.getLogger(CMBProperties.class);

	private CQSStressTestProperties()  {

		Properties props = new Properties();
		File file = null;

		if (System.getProperty("cqs.stresstest.propertyFile") != null) {
			file = new File(System.getProperty("cqs.stresstest.propertyFile"));
        } else if (new File("config/cqs.stresstest.properties").exists()) {
			file = new File("config/cqs.stresstest.properties");
        } else if (new File("cqs.stresstest.properties").exists()) {
			file = new File("cqs.stresstest.properties");
        } else {
            throw new IllegalArgumentException("Missing VM parameter cqs.stresstest.propertyFile");
        }

		try {

			FileInputStream fileStream = new FileInputStream(file);
			props.load(fileStream);

            testDurationSeconds = Integer.parseInt(props.getProperty("cmb.cqs.stress.testDurationSeconds", "100"));
            messagesPerQueuePerSecond = Integer.parseInt(props.getProperty("cmb.cqs.stress.messagesPerQueuePerSecond", "100"));
            numberOfQueues = Integer.parseInt(props.getProperty("cmb.cqs.stress.numberOfQueues", "5"));
            numberOfSendersPerQueue = Integer.parseInt(props.getProperty("cmb.cqs.stress.numberOfSendersPerQueue", "4"));
            numberOfReceiversPerQueue = Integer.parseInt(props.getProperty("cmb.cqs.stress.numberOfReceiversPerQueue", "15"));
            sendMessageBatchSize = Integer.parseInt(props.getProperty("cmb.cqs.stress.sendMessageBatchSize", "1"));
            receiveMessageBatchSize = Integer.parseInt(props.getProperty("cmb.cqs.stress.receiveMessageBatchSize", "1"));
            delayBetweenSendsMS = Integer.parseInt(props.getProperty("cmb.cqs.stress.delayBetweenSendsMS", "0"));
            delayBetweenReceivesMS = Integer.parseInt(props.getProperty("cmb.cqs.stress.delayBetweenReceivesMS", "5"));
            delayBetweenReceiveAndDeleteMS = Integer.parseInt(props.getProperty("cmb.cqs.stress.delayBetweenReceiveAndDeleteMS", "40"));
            revisiblePercentage = Integer.parseInt(props.getProperty("cmb.cqs.stress.revisiblePercentage", "0"));

            String queueNamesString = props.getProperty("cmb.cqs.stress.queueNames");

            if (queueNamesString != null) {
            	queueNames = queueNamesString.split(",");
            } else {
            	queueNames = null;
            }

            fileStream.close();

		} catch (Exception e) {
            logger.error("event=load_cmb_properties status=failed file="+System.getProperty("CMB.propertyFile"), e);
            throw new RuntimeException("Unable to load CMB.propertyFile");
		}
	}

	public static CQSStressTestProperties getInstance() {
		return instance;
	}

	public int getTestDurationSeconds() {
		return testDurationSeconds;
	}

	public int getMessagesPerQueuePerSecond() {
		return messagesPerQueuePerSecond;
	}

	public int getNumberOfQueues() {
		return numberOfQueues;
	}

	public int getNumberOfSendersPerQueue() {
		return numberOfSendersPerQueue;
	}

	public int getNumberOfReceiversPerQueue() {
		return numberOfReceiversPerQueue;
	}

	public int getSendMessageBatchSize() {
		return sendMessageBatchSize;
	}

	public int getDelayBetweenSendsMS() {
		return delayBetweenSendsMS;
	}

	public int getReceiveMessageBatchSize() {
		return receiveMessageBatchSize;
	}

	public int getDelayBetweenReceivesMS() {
		return delayBetweenReceivesMS;
	}

	public int getDelayBetweenReceiveAndDeleteMS() {
		return delayBetweenReceiveAndDeleteMS;
	}

	public int getRevisiblePercentage() {
		return revisiblePercentage;
	}

	public String[] getQueueNames() {
		return queueNames;
	}
}
