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
package com.comcast.cmb.test.common.model.unit;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.CMBStatement;
import com.comcast.cmb.common.util.Util;

public class CMBPolicyTest {
	
    @Before
    public void setup() throws Exception{
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
    }    
    
    @Test
    public void testCMBStatement() {
    	
        try {
	        CMBPolicy policy = new CMBPolicy();
	
	        policy.addStatement(CMBPolicy.SERVICE.CQS, "unittest1", "Allow", Arrays.asList("1234567", "345678", "6789"), Arrays.asList("SendMessage", "GetQueueUrl"), "arn:cmb:cqs:ccp:331770435817:MyQueue123456789", null);
	        policy.addStatement(CMBPolicy.SERVICE.CQS, "unittest2", "Allow", Arrays.asList("1234567"), Arrays.asList("DeleteMessage", "GetQueueUrl"), "arn:cmb:cqs:ccp:331770435817:MyQueue123456789", null);
	        policy.addStatement(CMBPolicy.SERVICE.CQS, "unittest3", "Allow", Arrays.asList("1234567"), Arrays.asList("GetQueueUrl"), "arn:cmb:cqs:ccp:331770435817:MyQueue123456789", null);
	        policy.addStatement(CMBPolicy.SERVICE.CQS, "unittest4", "Allow", Arrays.asList("1234567", "345678", "6789"), Arrays.asList("ReceiveMessage"), "arn:cmb:cqs:ccp:331770435817:MyQueue123456789", null);
	        
	        String policyStr = policy.toString();
	        
	        CMBPolicy policy2;
		
        	policy2 = new CMBPolicy(policyStr);
			List<CMBStatement> stmtList = policy2.getStatements();

	        assertTrue("Is there 4 permissions: ", stmtList.size() == 4);
	        int i = 1;

	        for (CMBStatement stmt : stmtList) {
	            assertTrue("Is the Id correct: ", stmt.getSid().equals("unittest" + i++));
	        }

        } catch (Exception e) {
			fail();
		}
    }
    
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }
}
