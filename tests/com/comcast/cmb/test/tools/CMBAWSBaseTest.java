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
 */package com.comcast.cmb.test.tools;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;

public class CMBAWSBaseTest {
	
	protected static Logger logger = Logger.getLogger(CMBAWSBaseTest.class);
	
	public enum USR { USER1, USER2, USER3 }
	
	public static final String userName1 = "cmb_unit_test_1";
	
	public static String accessKey1 = null;
	public static String accessSecret1 = null;

	public static final String userName2 = "cmb_unit_test_2";
	public static String accessKey2 = null;
	public static String accessSecret2 = null;
	
	public static final String userName3 = "cmb_unit_test_3";
	public static String accessKey3 = null;
	public static String accessSecret3 = null;

	public static final String PREFIX = "T";
	
	public static final String cnsServiceUrl = CMBProperties.getInstance().getCNSServiceUrl();
	public static final String cqsServiceUrl = CMBProperties.getInstance().getCQSServiceUrl();

	//public static final String cnsServiceUrl = "http://cmbstage.sv.comcast.com:10162/";
	//public static final String cqsServiceUrl = "http://cmbstage.sv.comcast.com:10159/";
	
	//public static final String cnsServiceUrl = "http://172.20.5.151:6000/";
	//public static final String cqsServiceUrl = "http://172.20.5.151:6000/";

	public static final String cqsServiceUrlAlt = null;
	public static final String cnsServiceUrlAlt = null;
	
	protected static User user1, user2, user3;
	
	protected static AmazonSNSClient cns1 = null;
	protected static AmazonSQSClient cqs1 = null;

	protected static AmazonSNSClient cns2 = null;
	protected static AmazonSQSClient cqs2 = null;
	
	protected static AmazonSNSClient cns3 = null;
	protected static AmazonSQSClient cqs3 = null;

	protected static AmazonSNSClient cnsAlt = null;
	protected static AmazonSQSClient cqsAlt = null;

	protected static Random rand = new Random();
	
	private Map<String, String> queues = new HashMap<String, String>();
	private Map<String, String> topics = new HashMap<String, String>();
	
	@Before
	public void setup() throws Exception {
		
		Util.initLog4jTest();
		CMBControllerServlet.valueAccumulator.initializeAllCounters();
		PersistenceFactory.reset();
		
		IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
		
		AWSCredentials credentials1 = null; 
		AWSCredentials credentials2 = null;
		AWSCredentials credentials3 = null;
		
		if (accessKey1 != null && accessSecret1 != null) {
			credentials1 = new BasicAWSCredentials(accessKey1, accessSecret1);
		} else {
			user1 = userHandler.getUserByName(userName1);
			if (user1 == null) {	          
				user1 =  userHandler.createUser(userName1, userName1);
			}
			credentials1 = new BasicAWSCredentials(user1.getAccessKey(), user1.getAccessSecret());
			accessKey1 = user1.getAccessKey();
			accessSecret1 = user1.getAccessSecret();
		}
		
		if (accessKey2 != null && accessSecret2 != null) {
			credentials2 = new BasicAWSCredentials(accessKey2, accessSecret2);
		} else {
			user2 = userHandler.getUserByName(userName2);
			if (user2 == null) {           	
				user2 =  userHandler.createUser(userName2, userName2);
			}
			credentials2 = new BasicAWSCredentials(user2.getAccessKey(), user2.getAccessSecret());
			accessKey2 = user2.getAccessKey();
			accessSecret2 = user2.getAccessSecret();
		}		
		
		if (accessKey3 != null && accessSecret3 != null) {
			credentials3 = new BasicAWSCredentials(accessKey3, accessSecret3);
		} else {
			user3 = userHandler.getUserByName(userName3);
			if (user3 == null) {           	
				user3 =  userHandler.createUser(userName3, userName3);
			}
			credentials3 = new BasicAWSCredentials(user3.getAccessKey(), user3.getAccessSecret());
			accessKey3 = user3.getAccessKey();
			accessSecret3 = user3.getAccessSecret();
		}		

		if (cnsServiceUrl != null) {
			cns1 = new AmazonSNSClient(credentials1);
	    	cns1.setEndpoint(cnsServiceUrl);
			cns2 = new AmazonSNSClient(credentials2);
			cns2.setEndpoint(cnsServiceUrl);
			cns3 = new AmazonSNSClient(credentials3);
			cns3.setEndpoint(cnsServiceUrl);
			deleteAllTopics(cns1);
			deleteAllTopics(cns2);
			deleteAllTopics(cns3);
		}

		if (cqsServiceUrl != null) {
		    cqs1 = new AmazonSQSClient(credentials1);
			cqs1.setEndpoint(cqsServiceUrl);
			cqs2 = new AmazonSQSClient(credentials2);
			cqs2.setEndpoint(cqsServiceUrl);
			cqs3 = new AmazonSQSClient(credentials3);
			cqs3.setEndpoint(cqsServiceUrl);
			deleteAllQueues(cqs1);
			deleteAllQueues(cqs2);
			deleteAllQueues(cqs3);
		}
	}
	
	@After
	public void tearDown() {
		for (String key : queues.keySet()) {
			logger.info("deleting " + queues.get(key));
			if (key.endsWith("_USER1")) {
				cqs1.deleteQueue(new DeleteQueueRequest(queues.get(key)));
			} else if (key.endsWith("_USER2")) {
				cqs2.deleteQueue(new DeleteQueueRequest(queues.get(key)));
			} else {
				cqs3.deleteQueue(new DeleteQueueRequest(queues.get(key)));
			}
		}
		for (String key : topics.keySet()) {
			logger.info("deleting " + topics.get(key));
			if (key.endsWith("_USER1")) {
				cns1.deleteTopic(new DeleteTopicRequest(topics.get(key)));
			} else if (key.endsWith("_USER2")) {
				cns2.deleteTopic(new DeleteTopicRequest(topics.get(key)));
			} else {
				cns3.deleteTopic(new DeleteTopicRequest(topics.get(key)));
			}
		}
		CMBControllerServlet.valueAccumulator.deleteAllCounters();
	}
	
	public AmazonSQSClient getCqs(USR usr) {
		if (usr.equals(USR.USER1)) {
			return cqs1;
		} else if (usr.equals(USR.USER2)) {
			return cqs2;
		} else {
			return cqs3;
		}
	}

	public AmazonSNSClient getCns(USR usr) {
		if (usr.equals(USR.USER1)) {
			return cns1;
		} else if (usr.equals(USR.USER2)) {
			return cns2;
		} else {
			return cns3;
		}
	}

	public String getQueueUrl(int idx, USR usr) {
		if (!queues.containsKey(idx)) {
			queues.put(idx + "_" + usr, getCqs(usr).createQueue(new CreateQueueRequest(PREFIX + rand.nextInt())).getQueueUrl());
			logger.info("created queue " + queues.get(idx + "_" + usr));
		}
		return queues.get(idx + "_" + usr);
	}
	
	public String getQueueArn(int idx, USR usr) {
		return com.comcast.cqs.util.Util.getArnForAbsoluteQueueUrl(queues.get(idx + "_" + usr));
	}

	public String getTopic(int idx, USR usr) {
		if (!topics.containsKey(idx)) {
			topics.put(idx + "_" + usr, getCns(usr).createTopic(new CreateTopicRequest(PREFIX + rand.nextInt())).getTopicArn());
			logger.info("created topic " + topics.get(idx + "_" + usr));
		}
		return topics.get(idx + "_" + usr);
	}
	
	private void deleteAllTopics(AmazonSNSClient sns) {
		ListTopicsResult listTopicsResult = sns.listTopics();
		for (Topic topic : listTopicsResult.getTopics()) {
			sns.deleteTopic(new DeleteTopicRequest(topic.getTopicArn()));
		}
	}
	
	private void deleteAllQueues(AmazonSQSClient sqs) {
		ListQueuesResult listQueueaResult = sqs.listQueues();
		for (String queueUrl : listQueueaResult.getQueueUrls()) {
			sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
		}
	}
}
