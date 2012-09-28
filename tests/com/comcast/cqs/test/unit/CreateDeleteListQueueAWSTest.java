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
package com.comcast.cqs.test.unit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.fail;

/**
 * Created by IntelliJ IDEA.
 * User: michael
 * Date: 2/3/12
 * Time: 10:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class CreateDeleteListQueueAWSTest {

    protected static Logger logger = Logger.getLogger(CreateDeleteListQueueAWSTest.class);

    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        PersistenceFactory.reset();
    }

    @Test
    public void testCreateDeleteQueue() {

        try {

        	Random rand = new Random();

            IUserPersistence dao = new UserCassandraPersistence();
            User user = dao.getUserByName("cqs_unit_test");

            if (user == null) {
                user =  dao.createUser("cqs_unit_test", "cqs_unit_test");
            }

            AWSCredentials awsCredentials = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());

            ClientConfiguration clientConfiguration = new ClientConfiguration();

            AmazonSQS sqs = new AmazonSQSClient(awsCredentials, clientConfiguration);

            sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());

            CreateQueueRequest qRequest = new CreateQueueRequest("TestQueue"+rand.nextInt());

            CreateQueueResult result = sqs.createQueue(qRequest);

            String qUrl = result.getQueueUrl();

            DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(qUrl);

            sqs.deleteQueue(deleteQueueRequest);

        } catch (Exception ex) {
            logger.error("test failed", ex);
            fail("Test failed: " + ex.toString());
        }
    }
    
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
