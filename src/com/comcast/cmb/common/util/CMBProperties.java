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
package com.comcast.cmb.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import me.prettyprint.hector.api.HConsistencyLevel;

import org.apache.log4j.Logger;

/**
 * Class represents all properties for CMB. Most of them are read from a properties file.
 * @author jorge, vvenkatraman, michael, bwolf, aseem
 * Class is thread-safe
 */
public class CMBProperties {
	
	public enum IO_MODE {
	    SYNC, ASYNC 
	}
	
	private final String cmbUnsubscribeUrl;
	
	private final HConsistencyLevel consistencyLevel;
	private final HConsistencyLevel readConsistencyLevel;
	private final HConsistencyLevel writeConsistencyLevel;
	
	private final int cmbWorkerPoolSize;
	
	private final String cnsHttpProxy;
	
	private final boolean cmbEnableStats;
	
	private final String cmbDeploymentName;
	
	private final boolean cnsBypassPublishJobQueueForSmallTopics;
	
	private final int cnsServerPort;
	private final int cqsServerPort;
	
	private final boolean cnsPublisherEnabled;
	private final String cnsPublisherMode;
	
	private final int requestParameterValueMaxLength;
	
	private final String cmbDataCenter;
	
	private final IO_MODE cnsIOMode;

	private final int cqsLongPollPort;
	private final boolean cqsLongPollEnabled;
	
	private final int cnsMessageExpirationSeconds;
	
	private final int httpTimeoutSeconds;
	private final List<String> acceptableHttpStatusCodes;

	private final int endpointFailureCountToSuspensionThreshold;
	
    private final String cnsServiceUrl;
	private final String cqsServiceUrl;
	private final String cnsAdminUrl;
	private final String cqsAdminUrl;
	
	private final String cnsUserName;
	private final String cnsUserPassword;
	private final String cnsUserAccessKey;

	private final String cnsUserAccessSecret;
	
	private final String awsAccessKey;
	private final String awsAccessSecret;
	
	private final String cnsPublishQueueNamePrefix;
	private final String cnsEndpointPublishQueueNamePrefix;

	private final String clusterName;
	private final String clusterUrl;
	
    private final int hectorPoolSize;
    private final String hectorBalancingPolicy;

	private final boolean hectorAutoDiscovery;
    private final int hectorAutoDiscoveryDelaySeconds;
    private final String hectorAutoDiscoveryDataCenter;
    private final Map<String, String> hectorCredentials;
	
	private final String region;
    
	private final String smtpHostName;
    private final String smtpUserName;
    private final String smtpPassword;
    private final String smtpReplyAddress;
    private final boolean smtpEnabled;
    private final int publisherThreadPoolSize;
    public volatile int publishJobQueueSizeLimit; //this made public volatile so unit-tests can tweak it
    private final int pingDelayMS;
    private final int workStealerDelayMS;
    
    private final int cnsMaxMsgSize;

    private final int visibilityTO;
    private final int maxMsgCountBatch;
    private final int cqsMaxMsgSize;
    private final int cqsMaxMsgSizeBatch;
    private final int msgRetentionPeriod;
    private final int maxMsgRetentionPeriod;
    private final int minMsgRetentionPeriod;
    private final int delaySeconds;    
    private final int maxQueueNameLength;
    private final int maxMessageSuppliedIdLength;
    private final int maxBatchEntries;
    private final int maxReceiveMessageCount;
    private final int maxVisibilityTO;
    private final int maxDelaySeconds;
    
    private final int userCacheExpiring;
    private final int userCacheSizeLimit;
    
    private final String cmbCommonKeyspace;
    private final String cmbCQSKeyspace;
    private final String cmbCNSKeyspace;
    
    private final int cnsCacheExpiring;
    private final int cnsCacheSizeLimit;

    private final int httpPublisherEndpointConnectionPoolSize;
    private final int httpPublisherEndpointConnectionsPerRouteSize;
    
    private final int cqsCacheExpiring;
    private final int cqsCacheSizeLimit;

    private final int rollingWindowTimeSec;    
    
    private final int redisConnectionsMaxActive;
    private final String redisServerList;
    private final int redisFillerThreads;
    private final int redisRevisibleThreads;
    private volatile int redisRevisibleFrequencySec;
    private final int redisRevisibleSetFrequencySec;
    private final int redisExpireTTLSec;
    
    private final int cassandraThriftSocketTimeOutMS;
    
    private final int cqsNumberOfQueuePartitions;
        
    private final boolean enableSignatureAuth;
    private final boolean requireSubscriptionConfirmation;
    private final boolean cnsServiceEnabled;
    private final boolean cqsServiceEnabled;
    
    // these are configs for the CNSPublisher tool
    
    private final int numEPPubJobProducers;
    private final int numEPPubJobConsumers;
    private final int numPublishJobQs;
    private final int numEPPublishJobQs;
    private final int numDeliveryHandlers;
    private final int numReDeliveryHandlers;
    private final int deliveryHandlerJobQueueLimit;
    private final int reDeliveryHandlerJobQueueLimit;
    private final int publishJobVTO;
    private final int EPPublishJobVTO;
    private final int maxSubscriptionsPerEPPublishJob;
    private final int producerProcessingMaxDelay;
    private final int consumerProcessingMaxDelay;
    private volatile boolean useSubInfoCache;
        
    private static final Logger log = Logger.getLogger(CMBProperties.class);
	private static final CMBProperties instance = new CMBProperties();
	
	private CMBProperties()  {
		
		Properties props = new Properties();
		File file = null;
		
		if (System.getProperty("cmb.propertyFile") != null) {
			file = new File(System.getProperty("cmb.propertyFile"));
        } else if (System.getProperty("CMB.propertyFile") != null) {
			file = new File(System.getProperty("CMB.propertyFile"));
        } else if (new File("config/cmb.properties").exists()) {
			file = new File("config/cmb.properties");
        } else if (new File("cmb.properties").exists()) {
			file = new File("cmb.properties");
        } else {
            throw new IllegalArgumentException("Could not locate cmb.properties.");
        }
		
		log.info("event=loading_cmb_properties file=" + file.getAbsolutePath());

		try {
			
			FileInputStream fileStream = new FileInputStream(file);
			props.load(fileStream);
			
			consistencyLevel = HConsistencyLevel.valueOf(props.getProperty("cmb.cassandra.consistencyLevel","QUORUM"));
			readConsistencyLevel = HConsistencyLevel.valueOf(props.getProperty("cmb.cassandra.readConsistencyLevel",consistencyLevel.name()));
			writeConsistencyLevel = HConsistencyLevel.valueOf(props.getProperty("cmb.cassandra.writeConsistencyLevel",consistencyLevel.name()));
			
			cmbWorkerPoolSize = Integer.parseInt(props.getProperty("cmb.workerpool.size","256"));
			
			cnsHttpProxy = props.getProperty("cmb.cns.http.proxy", null);
			
			cmbDeploymentName = props.getProperty("cmb.deployment.name", "");
			
			cnsBypassPublishJobQueueForSmallTopics = Boolean.parseBoolean(props.getProperty("cmb.cns.bypassPublishJobQueueForSmallTopics", "true"));
			cmbEnableStats = Boolean.parseBoolean(props.getProperty("cmb.stats.enable", "false"));
			
			cqsServerPort = Integer.parseInt(props.getProperty("cmb.cqs.server.port", "6059"));
			cnsServerPort = Integer.parseInt(props.getProperty("cmb.cns.server.port", "6061"));
			
			cnsPublisherEnabled = Boolean.parseBoolean(props.getProperty("cmb.cns.publisherEnabled", "true"));
			cnsPublisherMode = props.getProperty("cmb.cns.publisherMode", "Consumer,Producer");
			
			hectorAutoDiscoveryDataCenter = props.getProperty("cmb.hector.autoDiscoveryDataCenter");
			
			cmbDataCenter = props.getProperty("cmb.dc.name", "default");
			
			requestParameterValueMaxLength = Integer.parseInt(props.getProperty("cmb.log.requestParameterValueMaxLength", "128"));
			
			cqsLongPollPort = Integer.parseInt(props.getProperty("cmb.cqs.longpoll.port", "5555"));
			cqsLongPollEnabled = Boolean.parseBoolean(props.getProperty("cmb.cqs.longpoll.enable", "true"));
			
			cnsServiceUrl = props.getProperty("cmb.cns.service.url", "http://localhost:6061/");
			cqsServiceUrl = props.getProperty("cmb.cqs.service.url", "http://localhost:6059/");
			cnsAdminUrl = props.getProperty("cmb.cns.admin.url", cnsServiceUrl);
			cqsAdminUrl = props.getProperty("cmb.cqs.admin.url", cqsServiceUrl);
			cmbUnsubscribeUrl = props.getProperty("cmb.cns.unsubscribe.url", cnsServiceUrl);

			cnsUserName = props.getProperty("cmb.cns.user.name", "cns_internal");
			cnsUserPassword = props.getProperty("cmb.cns.user.password", "cns_internal");
			cnsUserAccessKey = props.getProperty("cmb.cns.user.access.key", null);
			cnsUserAccessSecret = props.getProperty("cmb.cns.user.access.secret", null);
			
			awsAccessKey = props.getProperty("aws.access.key");
			awsAccessSecret = props.getProperty("aws.access.secret");

			cnsPublishQueueNamePrefix = props.getProperty("cmb.cns.publish.queueNamePrefix", "PublishJobQ_");
			cnsEndpointPublishQueueNamePrefix = props.getProperty("cmb.cns.publish.endpointQueueNamePrefix", "EndpointPublishQ_");

			clusterName = props.getProperty("cmb.cassandra.clusterName");
			clusterUrl = props.getProperty("cmb.cassandra.clusterUrl");
			
            hectorPoolSize = Integer.parseInt(props.getProperty("cmb.hector.pool.size", "75"));
            hectorAutoDiscoveryDelaySeconds = Integer.parseInt(props.getProperty("cmb.hector.autoDiscoveryDelaySeconds", "60"));
            hectorAutoDiscovery = Boolean.parseBoolean(props.getProperty("cmb.hector.autoDiscovery", "true"));
            hectorBalancingPolicy = props.getProperty("cmb.hector.balancingPolicy", "RoundRobinBalancingPolicy");
            if (props.getProperty("cmb.hector.username") != null) {
                hectorCredentials = new HashMap<String, String>(2);
                hectorCredentials.put("username", props.getProperty("cmb.hector.username", ""));
                hectorCredentials.put("password", props.getProperty("cmb.hector.password", ""));
            } else {
                hectorCredentials = null;
            }

            
			smtpHostName = props.getProperty("cmb.cns.smtp.hostname");
			smtpUserName = props.getProperty("cmb.cns.smtp.username");
			smtpPassword = props.getProperty("cmb.cns.smtp.password");
			smtpReplyAddress = props.getProperty("cmb.cns.smtp.replyAddress");
			smtpEnabled = Boolean.parseBoolean(props.getProperty("cmb.cns.smtp.enabled", "false"));
			
            region = props.getProperty("cmb.region");

            publisherThreadPoolSize = Integer.parseInt(props.getProperty("cmb.cns.publisher.threadPoolSize", "20"));
            publishJobQueueSizeLimit = Integer.parseInt(props.getProperty("cmb.cns.publisher.publishJobQueueSizeLimit", "100000"));
            pingDelayMS = Integer.parseInt(props.getProperty("cmb.cns.publisher.pingDelayMS", "60000"));
            workStealerDelayMS = Integer.parseInt(props.getProperty("cmb.cns.publisher.workStealerDelayMS", "60000"));

            enableSignatureAuth = Boolean.parseBoolean(props.getProperty("cmb.enableSignatureAuth", "false"));
            requireSubscriptionConfirmation = Boolean.parseBoolean(props.getProperty("cmb.cns.requireSubscriptionConfirmation", "true"));
            cnsServiceEnabled = Boolean.parseBoolean(props.getProperty("cmb.cns.serviceEnabled", "true"));
            cqsServiceEnabled = Boolean.parseBoolean(props.getProperty("cmb.cqs.serviceEnabled", "true"));
            
            cnsMaxMsgSize = Integer.parseInt(props.getProperty("cmb.cns.maxMsgSize", "65536"));
            
            visibilityTO = Integer.parseInt(props.getProperty("cmb.cqs.visibilityTO", "30"));
            maxMsgCountBatch = Integer.parseInt(props.getProperty("cmb.cqs.maxMsgCountBatch", "10"));
            cqsMaxMsgSize = Integer.parseInt(props.getProperty("cmb.cqs.maxMsgSize", "65536"));
            cqsMaxMsgSizeBatch = Integer.parseInt(props.getProperty("cmb.cqs.maxMsgSizeBatch", "65536"));
            msgRetentionPeriod = Integer.parseInt(props.getProperty("cmb.cqs.msgRetentionPeriod", "345600"));
            maxMsgRetentionPeriod = Integer.parseInt(props.getProperty("cmb.cqs.maxMsgRetentionPeriod", "1209600"));
            minMsgRetentionPeriod = Integer.parseInt(props.getProperty("cmb.cqs.minMsgRetentionPeriod", "60"));
            delaySeconds = Integer.parseInt(props.getProperty("cmb.cqs.delaySeconds", "0"));            
            maxQueueNameLength = Integer.parseInt(props.getProperty("cmb.cqs.maxQueueNameLength", "80"));
            maxMessageSuppliedIdLength = Integer.parseInt(props.getProperty("cmb.cqs.maxMessageSuppliedIdLength", "80"));
            maxBatchEntries = Integer.parseInt(props.getProperty("cmb.cqs.maxBatchEntries", "10"));
            maxReceiveMessageCount = Integer.parseInt(props.getProperty("cmb.cqs.maxReceiveMessageCount", "10"));
            maxVisibilityTO = Integer.parseInt(props.getProperty("cmb.cqs.maxVisibilityTO", "43200"));
            maxDelaySeconds=Integer.parseInt(props.getProperty("cmb.cqs.maxDelaySeconds", "900"));
            
            cmbCommonKeyspace = props.getProperty("cmb.common.keyspace", "CMB");
            cmbCQSKeyspace = props.getProperty("cmb.cqs.keyspace", "CQS");
            cmbCNSKeyspace = props.getProperty("cmb.cns.keyspace", "CNS");

            userCacheExpiring = Integer.parseInt(props.getProperty("cmb.user.cacheExpiringInSeconds", "60"));
            userCacheSizeLimit = Integer.parseInt(props.getProperty("cmb.user.cacheSizeLimit", "1000"));
            
            httpPublisherEndpointConnectionPoolSize = Integer.parseInt(props.getProperty("cmb.cns.publisher.http.connectionPoolSize", "250"));
            httpPublisherEndpointConnectionsPerRouteSize = Integer.parseInt(props.getProperty("cmb.cns.publisher.http.connectionsPerRouteSize", "50"));
            httpTimeoutSeconds = Integer.parseInt(props.getProperty("cmb.cns.publisher.http.httpTimeoutSeconds", "5"));
            acceptableHttpStatusCodes = Arrays.asList(props.getProperty("cmb.cns.publisher.http.acceptableStatusCodes", "").split(","));
            cnsIOMode = IO_MODE.valueOf(props.getProperty("cmb.cns.publisher.http.iomode", "sync").toUpperCase());
			
            cnsCacheExpiring = Integer.parseInt(props.getProperty("cmb.cns.cacheExpiringInSeconds", "60"));
            cnsCacheSizeLimit = Integer.parseInt(props.getProperty("cmb.cns.cacheSizeLimit", "1000"));
            
            cqsCacheExpiring = Integer.parseInt(props.getProperty("cmb.cqs.cacheExpiringInSeconds", "60"));
            cqsCacheSizeLimit = Integer.parseInt(props.getProperty("cmb.cqs.cacheSizeLimit", "1000"));
            
            cqsNumberOfQueuePartitions = Integer.parseInt(props.getProperty("cmb.cqs.numberOfQueuePartitions", "100"));
            
            rollingWindowTimeSec = Integer.parseInt(props.getProperty("cmb.rollingWindowSizeSec", "600"));
            
            redisConnectionsMaxActive = Integer.parseInt(props.getProperty("cmb.redis.connectionsMaxActive", "100"));
            redisServerList = props.getProperty("cmb.redis.serverList");
            redisFillerThreads = Integer.parseInt(props.getProperty("cmb.redis.fillerThreads", "5"));
            redisRevisibleThreads = Integer.parseInt(props.getProperty("cmb.redis.revisibleThreads", "3"));
            redisExpireTTLSec = Integer.parseInt(props.getProperty("cmb.redis.expireTTLSec", "1209600"));
            redisRevisibleFrequencySec = Integer.parseInt(props.getProperty("cmb.redis.revisibleFrequencySec", "10"));
            redisRevisibleSetFrequencySec = Integer.parseInt(props.getProperty("cmb.redis.revisibleSetFrequencySec", "1"));
            
            cassandraThriftSocketTimeOutMS = Integer.parseInt(props.getProperty("cmb.cassandra.thriftSocketTimeOutMS", "2000"));
            
            numEPPubJobProducers = Integer.parseInt(props.getProperty("cmb.cns.publisher.numProducers", "8"));
            numEPPubJobConsumers = Integer.parseInt(props.getProperty("cmb.cns.publisher.numConsumers", "4"));
            numPublishJobQs = Integer.parseInt(props.getProperty("cmb.cns.publisher.numPublishJobQs", "2"));
            numEPPublishJobQs = Integer.parseInt(props.getProperty("cmb.cns.publisher.numEPPublishJobQs", "4"));
            numDeliveryHandlers = Integer.parseInt(props.getProperty("cmb.cns.publisher.numDeliveryHandlers", "128"));
            numReDeliveryHandlers = Integer.parseInt(props.getProperty("cmb.cns.publisher.numReDeliveryHandlers", "128"));
            deliveryHandlerJobQueueLimit = Integer.parseInt(props.getProperty("cmb.cns.publisher.deliveryHandlerJobQueueLimit", "1000"));
            reDeliveryHandlerJobQueueLimit = Integer.parseInt(props.getProperty("cmb.cns.publisher.reDeliveryHandlerJobQueueLimit", "5000"));
            publishJobVTO = Integer.parseInt(props.getProperty("cmb.cns.publisher.publichJobVTO", "10"));
            EPPublishJobVTO = Integer.parseInt(props.getProperty("cmb.cns.publisher.EPPublishJobVTO", "10"));
            maxSubscriptionsPerEPPublishJob = Integer.parseInt(props.getProperty("cmb.cns.publisher.maxSubscriptionsPerEPPublishJob", "100"));
            producerProcessingMaxDelay = Integer.parseInt(props.getProperty("cmb.cns.publisher.producerProcessingMaxDelay", "1000"));
            consumerProcessingMaxDelay = Integer.parseInt(props.getProperty("cmb.cns.publisher.consumerProcessingMaxDelay", "1000"));
            
            endpointFailureCountToSuspensionThreshold = Integer.parseInt(props.getProperty("cmb.cns.publisher.endpointFailureCountToSuspensionThreshold", "10"));
            cnsMessageExpirationSeconds = Integer.parseInt(props.getProperty("cmb.cns.publisher.messageExpirationSeconds", "0"));
            
            useSubInfoCache = Boolean.parseBoolean(props.getProperty("cmb.cns.useSubInfoCache", "true"));
            fileStream.close();

		} catch (Exception ex) {
            log.error("event=load_cmb_properties error_code=unable_to_load_file file=" + file.getAbsolutePath(), ex);
            throw new RuntimeException("Could not load properties from " + file.getAbsolutePath());
		}
	}
	
	public String getCNSHttpProxy() {
		return cnsHttpProxy;
	}

	public boolean isCMBStatsEnabled() {
		return cmbEnableStats;
	}

	public String getHectorAutoDiscoveryDataCenter() {
		return hectorAutoDiscoveryDataCenter;
	}

	public int getEndpointFailureCountToSuspensionThreshold() {
		return endpointFailureCountToSuspensionThreshold;
	}

	public static CMBProperties getInstance() {			
		return instance;
	}
	
	public String getCNSServiceUrl() {
		return cnsServiceUrl;
	}
	
	public String getCNSAdminUrl() {
		return cnsAdminUrl;
	}

	public String getClusterName() {
		return clusterName;
	}
			
	public String getClusterUrl() {
		return clusterUrl;
	}
	
	public String getCQSServiceUrl() {
		return cqsServiceUrl;
	}
	
	public String getCQSAdminUrl() {
		return cqsAdminUrl;
	}

	public String getRegion() {
        return region;
    }
    
    public boolean getSmtpEnabled() {
		return smtpEnabled;
	}

    public String getSmtpHostName() {
		return smtpHostName;
	}

	public String getSmtpUserName() {
		return smtpUserName;
	}

	public String getSmtpPassword() {
		return smtpPassword;
	}
	
	public int getCNSPublisherThreadPoolSize() {
	    return publisherThreadPoolSize;
	}
	
	public int getCNSPublishJobQueueSizeLimit() {
	    return publishJobQueueSizeLimit;
	}
	
	public int getPingDelayMS() {
	    return pingDelayMS;
	}

    public int getWorkStealerDelayMS() {
        return workStealerDelayMS;
    }

    public boolean getEnableSignatureAuth() {
        return enableSignatureAuth;
    }

    public boolean getCNSRequireSubscriptionConfirmation() {
        return requireSubscriptionConfirmation;
    }

    public boolean getCNSServiceEnabled() {
        return cnsServiceEnabled;
    }

    public boolean getCQSServiceEnabled() {
        return cqsServiceEnabled;
    }

    public int getCQSVisibilityTimeOut() {
		return visibilityTO;
	}

	public int getCQSMaxMessageSize() {
		return cqsMaxMsgSize;
	}
	
	public int getCQSMaxMessageCountBatch() {
		return maxMsgCountBatch;
	}

	public int getCQSMaxMessageSizeBatch() {
		return cqsMaxMsgSizeBatch;
	}

    public int getCQSMessageRetentionPeriod() {
        return msgRetentionPeriod;
    }

    public int getCQSMaxMessageRetentionPeriod() {
        return maxMsgRetentionPeriod;
    }

    public int getCQSMinMessageRetentionPeriod() {
        return minMsgRetentionPeriod;
    }

    public int getCQSMessageDelaySeconds() {
        return delaySeconds;
    }

    public int getCQSMaxMessageDelaySeconds() {
        return maxDelaySeconds;
    }

	public int getCQSMaxNameLength() {
		return maxQueueNameLength;
	}

	public int getCQSMaxMessageSuppliedIdLength() {
		return maxMessageSuppliedIdLength;
	}

	public int getCQSMaxBatchEntries() {
		return maxBatchEntries;
	}

	public int getCQSMaxReceiveMessageCount() {
		return maxReceiveMessageCount;
	}

	public int getCQSMaxVisibilityTimeOut() {
		return maxVisibilityTO;
	}
	
	public int getUserCacheExpiring() {
	    return userCacheExpiring;
	}

	public String getCMBKeyspace() {
		return cmbCommonKeyspace;
	}

	public String getCQSKeyspace() {
		return cmbCQSKeyspace;
	}

	public String getCNSKeyspace() {
		return cmbCNSKeyspace;
	}

	public int getUserCacheSizeLimit() {
        return userCacheSizeLimit;
    }

	public String getSmtpReplyAddress() {
		return smtpReplyAddress;
	}

	public int getCNSCacheExpiring() {
	    return cnsCacheExpiring;
	}

	public int getCNSCacheSizeLimit() {
        return cnsCacheSizeLimit;
    }

	public int getHectorPoolSize() {
		return hectorPoolSize;
	}

	public int getCQSCacheExpiring() {
		return cqsCacheExpiring;
	}

	public int getCQSCacheSizeLimit() {
		return cqsCacheSizeLimit;
	}
	
	public String getHectorBalancingPolicy(){
		return hectorBalancingPolicy;
	}

	public int getCQSNumberOfQueuePartitions() {
		return cqsNumberOfQueuePartitions;
	}
	
    public int getRedisFillerThreads() {
        return redisFillerThreads;
    }

    public int getCassandraThriftSocketTimeOutMS() {
        return cassandraThriftSocketTimeOutMS;
    }

    public int getCNSNumPublishJobQueues() {
        return numPublishJobQs;
    }

    public int getCNSNumEndpointPublishJobQueues() {
        return numEPPublishJobQs;
    }

    public int getCNSDeliveryHandlerJobQueueLimit() {
        return deliveryHandlerJobQueueLimit;
    }

    public String getCNSUserName() {
        return cnsUserName;
    }

    public String getCNSUserPassword() {
        return cnsUserPassword;
    }
    
    public String getCNSPublishQueueNamePrefix() {
    	return cnsPublishQueueNamePrefix;
    }

    public String getCNSEndpointPublishQueueNamePrefix() {
    	return cnsEndpointPublishQueueNamePrefix;
    }
    
	public String getAwsAccessSecret() {
		return awsAccessSecret;
	}

	public String getAwsAccessKey() {
		return awsAccessKey;
	}
	
    public void setCNSUseSubInfoCache(boolean useSubInfoCache) {
        this.useSubInfoCache = useSubInfoCache;
    }

    public boolean isCNSUseSubInfoCache() {
        return useSubInfoCache;
    }

    public int getCNSConsumerProcessingMaxDelay() {
        return consumerProcessingMaxDelay;
    }

    public int getCNSProducerProcessingMaxDelay() {
        return producerProcessingMaxDelay;
    }

    public int getCNSPublishJobVisibilityTimeout() {
		return publishJobVTO;
	}

	public int getCNSEndpointPublishJobVisibilityTimeout() {
        return EPPublishJobVTO;
    }

	public int getCNSMaxSubscriptionsPerEndpointPublishJob() {
		return maxSubscriptionsPerEPPublishJob;
	}

	public int getCNSReDeliveryHandlerJobQueueLimit() {
        return reDeliveryHandlerJobQueueLimit;
    }

    public void setCNSPublishJobQueueSizeLimit(int publishJobQueueSizeLimit) {
        this.publishJobQueueSizeLimit = publishJobQueueSizeLimit;
    }

    public int getCNSNumPublisherDeliveryHandlers() {
        return numDeliveryHandlers;
    }

    public int getCNSNumPublisherReDeliveryHandlers() {
        return numReDeliveryHandlers;
    }

    public int getCNSNumEndpointPublisherJobProducers() {
        return numEPPubJobProducers;
    }

    public int getCNSNumEndpointPublisherJobConsumers() {
        return numEPPubJobConsumers;
    }

    public void setRedisRevisibleFrequencySec(int redisRevisibleFrequencySec) {
        this.redisRevisibleFrequencySec = redisRevisibleFrequencySec;
    }

    public int getRedisRevisibleSetFrequencySec() {
        return redisRevisibleSetFrequencySec;
    }

    public int getRedisRevisibleFrequencySec() {
        return redisRevisibleFrequencySec;
    }

    public int getRedisExpireTTLSec() {
        return redisExpireTTLSec;
    }

    public int getRedisRevisibleThreads() {
        return redisRevisibleThreads;
    }

    public String getRedisServerList() {
        return redisServerList;
    }

    public int getRedisConnectionsMaxActive() {
        return redisConnectionsMaxActive;
    }

    public int getRollingWindowTimeSec() {
        return rollingWindowTimeSec;
    }

    public int getCNSPublisherHttpEndpointConnectionPoolSize() {
        return httpPublisherEndpointConnectionPoolSize;
    }

    public int getCNSPublisherHttpEndpointConnectionsPerRouteSize() {
        return httpPublisherEndpointConnectionsPerRouteSize;
    }
    
	public int getCNSPublisherHttpTimeoutSeconds() {
		return httpTimeoutSeconds;
	}
	
	public List<String> getCNSPublisherAcceptableHttpStatusCodes() {
		return acceptableHttpStatusCodes;
	}
	
	public int getCNSMessageExpirationSeconds() {
		return cnsMessageExpirationSeconds;
	}

	public IO_MODE getCNSIOMode() {
		return cnsIOMode;
	}

	public int getCQSLongPollPort() {
		return cqsLongPollPort;
	}

	public boolean isCQSLongPollEnabled() {
		return cqsLongPollEnabled;
	}

	public String getCMBDataCenter() {
		return cmbDataCenter;
	}
	
	public int getCNSMaxMessageSize() {
		return cnsMaxMsgSize;
	}
	
	public int getCMBRequestParameterValueMaxLength() {
		return requestParameterValueMaxLength;
	}
	
    public boolean isHectorAutoDiscovery() {
		return hectorAutoDiscovery;
	}

	public int getHectorAutoDiscoveryDelaySeconds() {
		return hectorAutoDiscoveryDelaySeconds;
	}
	
	public Map<String, String> getHectorCredentials() {
	    return hectorCredentials;
	}

	public boolean isCNSPublisherEnabled() {
		return this.cnsPublisherEnabled;
	}
	
	public String getCNSPublisherMode() {
		return this.cnsPublisherMode;
	}

	public int getCNSServerPort() {
		return cnsServerPort;
	}

	public int getCQSServerPort() {
		return cqsServerPort;
	}

	public boolean isCNSBypassPublishJobQueueForSmallTopics() {
		return cnsBypassPublishJobQueueForSmallTopics;
	}

	public String getCMBDeploymentName() {
		return cmbDeploymentName;
	}
	
	public String getCNSUserAccessKey() {
		return cnsUserAccessKey;
	}

	public String getCNSUserAccessSecret() {
		return cnsUserAccessSecret;
	}
	
	public int getCMBWorkerPoolSize() {
		return cmbWorkerPoolSize;
	}
	
	/*public HConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}*/
	
	public HConsistencyLevel getReadConsistencyLevel() {
		return readConsistencyLevel;
	}
	
	public HConsistencyLevel getWriteConsistencyLevel() {
		return writeConsistencyLevel;
	}

	public String getCmbUnsubscribeUrl() {
		return cmbUnsubscribeUrl;
	}
}
