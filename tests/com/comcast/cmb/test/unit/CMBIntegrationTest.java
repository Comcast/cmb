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
package com.comcast.cmb.test.unit;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: michael
 * Date: 3/5/12
 * Time: 4:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class CMBIntegrationTest {

    AmazonSNSClient sns;

    @Before
    public void setup() throws Exception{
    	
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        
        IUserPersistence userHandler = new UserCassandraPersistence();

        User user = userHandler.getUserByName("cqs_unit_test");

        if (user == null) {
            user = userHandler.createUser("cqs_unit_test", "cqs_unit_test");
        }

        BasicAWSCredentials bas = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
        sns = new AmazonSNSClient(bas);
        sns.setEndpoint(CMBProperties.getInstance().getCNSServerUrl());
    }

    @Test
    public void testAuth() {
        sns.createTopic(new CreateTopicRequest("TopicBlah"));
    }

    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
