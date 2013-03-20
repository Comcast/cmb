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
package com.comcast.cmb.common.controller;

import java.net.URL;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.tools.CNSPublisher;

/**
 * Bootstrap class for cns, cqs service endpoint servlets and cns workers. Servlets
 * will be launched in embedded Jetty instances. 
 * @author bwolf
 *
 */
public class CMB {
	
	private static Logger logger = Logger.getLogger(CMB.class);
	 
    public static void main(String argv[]) throws Exception {
    	
        /*ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(CQSControllerServlet.class, "/");
        server.setHandler(handler);*/
    	
    	Util.initLog4j();
    	
    	// launch cqs service endpoint if enabled
    	
    	if (CMBProperties.getInstance().getCQSServiceEnabled()) {
        	
        	URL cqsUrl = new URL(CMBProperties.getInstance().getCQSServerUrl());
        	Server cqsServer = new Server(cqsUrl.getPort());

        	WebAppContext cqsWebContext = new WebAppContext();
	
	        cqsWebContext.setDescriptor("config/WEB-INF-CQS/web.xml");
	        cqsWebContext.setResourceBase("WebContent");      
	        cqsWebContext.setServer(cqsServer);
	        cqsWebContext.setParentLoaderPriority(true);
	        cqsWebContext.setContextPath("/");
	        
	        cqsServer.setHandler(cqsWebContext);
	        cqsServer.start();
	        
	        logger.info("event=laucnhed_cqs_service_endpoint port=" + cqsUrl.getPort());
    	}
    	
    	// launch cns service endpoint if enabled

    	if (CMBProperties.getInstance().getCNSServiceEnabled()) {

        	URL cnsUrl = new URL(CMBProperties.getInstance().getCNSServerUrl());
        	Server cnsServer = new Server(cnsUrl.getPort());
	    	
	        WebAppContext cnsWebContext = new WebAppContext();
	
	        cnsWebContext.setDescriptor("config/WEB-INF-CNS/web.xml");
	        cnsWebContext.setResourceBase("WebContent");      
	        cnsWebContext.setServer(cnsServer);
	        cnsWebContext.setParentLoaderPriority(true);
	        cnsWebContext.setContextPath("/");
	        
	        cnsServer.setHandler(cnsWebContext);
	        cnsServer.start();

	        logger.info("event=laucnhed_cns_service_endpoint port=" + cnsUrl.getPort());
    	}
    	
    	// launch cns publish worker if enabled
    	
    	if (CMBProperties.getInstance().isCNSPublisherEnabled()) {

    		CNSPublisher.start(CMBProperties.getInstance().getCNSPublisherMode());
	        
    		logger.info("event=laucnhed_cns_publisher mode=" + CMBProperties.getInstance().getCNSPublisherMode());
    	}
    }
}
