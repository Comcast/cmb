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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.comcast.cmb.test.tools.SimpleHttpServletRequest;
import com.comcast.cmb.test.tools.SimpleHttpServletResponse;
import com.comcast.plaxo.cmb.common.util.Util;
import org.apache.log4j.Logger;
import org.junit.*;

import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.controller.UserLoginPageServlet;
import com.comcast.plaxo.cmb.common.util.ValueAccumulator.AccumulatorName;

import static org.junit.Assert.assertTrue;

public class UserLoginPageTest {

    private static Logger log = Logger.getLogger(UserLoginPageTest.class);
    
    @Before
    public void setup() throws Exception {
        Util.initLog4jTest();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();

    }
	
	@Test
	public void testLoginUserPage() {
		try {
			UserLoginPageServlet loginServlet = new UserLoginPageServlet();
			Resp res1 = loginUser(loginServlet, "tina", "");
			assertTrue(res1.httpCode == 200 || res1.httpCode == 0);
		} catch (Exception ex) {
            log.error("exception="+ex, ex);
		}
	}
	
	private Resp loginUser(UserLoginPageServlet loginServlet, String userName, String passwd) throws Exception {
		SimpleHttpServletRequest request = new SimpleHttpServletRequest();
		Map<String, String[]> params = new HashMap<String, String[]>();
		addParam(params, "user", userName);
		addParam(params, "passwd", passwd);
		addParam(params, "Login", "Login");
		
		request.setParameterMap(params);
		SimpleHttpServletResponse response = new SimpleHttpServletResponse();
		OutputStream out = new ByteArrayOutputStream();
		response.setOutputStream(out);
		loginServlet.doGet(request, response);
		response.getWriter().flush();
		
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
