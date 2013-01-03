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
package com.comcast.cns.tools;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.controller.CNSMonitor;

/**
 * The main class for the tool that sends out notifications or creates endpointPublich jobs
 *
 * @author aseem, ppang, bwolf
 */
public class CNSPublisher {
    
    private static Logger logger = Logger.getLogger(CNSPublisher.class);
    
    public enum Mode {
        Producer,
        Consumer
    }
    
    static volatile EnumSet<Mode> modes;
    
    static volatile CNSPublisherJobThread[] jobProducers = null;
    static volatile CNSPublisherJobThread[] consumers = null;
    
    // used for ping
    
    public static volatile AtomicLong lastProducerMinute = new AtomicLong(System.currentTimeMillis()/(1000*60)); 
    public static volatile AtomicLong lastConsumerMinute = new AtomicLong(System.currentTimeMillis()/(1000*60)); 
    
	public static volatile CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCMBCNSKeyspace());
    
    private static void printUsage() {
        System.out.println("java <opts> com.comcast.cns.tools.CNSPublisher -role=<comma separated list of roles>");
        System.out.println("where possible roles are {Producer, Consumer}");
    }
    
    private static EnumSet<Mode> parseMode(String param) {
        String []arr = param.split("=");
        if (arr.length != 2) {
            throw new IllegalArgumentException("Bad format for parameter. Expected:-role=<comma seperated list of roles> got:" + param);
        }
        String []roles = arr[1].split(",");
        if (roles.length == 0) {
            throw new IllegalArgumentException("Expected a comma separated list of roles. Got:" + arr[1]);
        }
        EnumSet<Mode> ms = EnumSet.of(Mode.valueOf(roles[0]));
        for (int i = 1; i < roles.length; i++) {
            ms.add(Mode.valueOf(roles[i]));
        }
        return ms;
    }
    
    public static String getModeString() {
    	if (modes.contains(Mode.Producer) && modes.contains(Mode.Consumer)) {
    		return "Producer,Consumer";
    	} else if (modes.contains(Mode.Producer)) {
    		return "Producer";
    	} else if (modes.contains(Mode.Consumer)) {
    		return "Consumer";
    	} else {
    		return "";
    	}
    }
    
    public static void clearQueues() throws PersistenceException {
    	
    	CNSEndpointPublisherJobProducer.shutdown();
    	CNSEndpointPublisherJobConsumer.shutdown();

    	CNSEndpointPublisherJobConsumer.initialize();
    	CNSEndpointPublisherJobProducer.initialize();
    }
    
    /**
     * Usage is java <opts> com.comcast.cns.tools.CNSPublisher -role=<comma seperated list of roles>
     * @param argv
     * @throws Exception
     */
    public static void main(String argv[]) throws Exception {

    	if (argv.length < 1) {
            System.out.println("Bad usage");
            printUsage();
            System.exit(1);
        }
    	
        Util.initLog4jTest();

    	logger.info("event=startup version=" + CMBControllerServlet.VERSION + " ip=" + InetAddress.getLocalHost().getHostAddress() + " io_mode=" + CMBProperties.getInstance().getCnsIOMode());
        
    	modes = parseMode(argv[0]);
        logger.info("modes=" + modes);        
        
        if (modes.contains(Mode.Producer)) {
        	
        	CNSEndpointPublisherJobProducer.initialize(); 
        	jobProducers = new CNSPublisherJobThread[CMBProperties.getInstance().getNumEPPubJobProducers()]; 
        	
            for (int i = 0; i < jobProducers.length; i++) {
                jobProducers[i] = new CNSPublisherJobThread("CNSEPJobProducer-" + i, new CNSEndpointPublisherJobProducer(), CMBProperties.getInstance().getNumPublishJobQs(), CMBProperties.getInstance().getProducerProcessingMaxDelay());
                jobProducers[i].start();
            }
        } 
        
        if (modes.contains(Mode.Consumer)) {
        	
            CNSEndpointPublisherJobConsumer.initialize();
            consumers = new CNSPublisherJobThread[CMBProperties.getInstance().getNumEPPubJobConsumers()];
            
            for (int i = 0; i < consumers.length; i++) {
                consumers[i] = new CNSPublisherJobThread("CNSEPJobConsumer-" + i, new CNSEndpointPublisherJobConsumer(), CMBProperties.getInstance().getNumEPPublishJobQs(), CMBProperties.getInstance().getConsumerProcessingMaxDelay());
                consumers[i].start();
            }

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
            ObjectName name = new ObjectName("com.comcast.cns.controller:type=CNSMonitorMBean");
            
            if (!mbs.isRegistered(name)) {
                mbs.registerMBean(CNSMonitor.getInstance(), name);
            }
        }        
    }    
}
