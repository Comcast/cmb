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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.comcast.cmb.common.controller.AdminServlet;
import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.Util;
import com.comcast.cmb.test.tools.SimpleHttpServletRequest;
import com.comcast.cmb.test.tools.SimpleHttpServletResponse;

import org.apache.log4j.Logger;
import org.junit.* ;


public class AdminServletTest {

    private static Logger log = Logger.getLogger(AdminServletTest.class);
    
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
    }
	
	@Test
	public void testAdminCreateDeleteListUserServlet() {
		try {
			AdminServlet adm = new AdminServlet();
            Resp res1 = addUser(adm, "JunitTestName", "JunitTestPasswd");
            assertTrue(res1.httpCode == 200 || res1.httpCode == 0);
            
            Resp res2 = deleteUser(adm, "JunitTestName");
            assertTrue(res2.httpCode == 200 || res2.httpCode == 0);

		} catch (Exception ex) {
			log.error("Exception="+ex, ex);
            assertFalse(true);
        }
	}
	
	private Resp addUser(AdminServlet adm, String userName, String passwd) throws Exception
	{
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
	    addParam(params, "user", userName);
	    addParam(params, "password", passwd);
	    addParam(params, "Create", "Create");
			
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();	
		response.setOutputStream(out);
		adm.doGet(request, response);
		response.getWriter().flush();
		//System.out.println("Out:" + out.toString());
		
		return new Resp(response.getStatus(), out.toString());
	}
	
	private Resp deleteUser(AdminServlet adm, String userName) throws Exception
	{
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "user", userName);
		addParam(params, "Delete", "Delete");
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		adm.doGet(request, response);
		response.getWriter().flush();
		//System.out.println("Out:" + out.toString());
		return new Resp(response.getStatus(), out.toString());
	}
	
		
	private void addParam(Map<String, String[]> params, String name, String val) {        
        String[] paramVals = new String[1];
        paramVals[0] = val;
        params.put(name, paramVals);        
    }
	
	class Resp {
        Resp(int code, String res) {
            this.httpCode = code;
            this.resp = res;
        }
        
        int httpCode;
        String resp;
    };
    @After    
    public void tearDown() {
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }

}
