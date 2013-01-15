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

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Class represents all properties for CMB. Most of them are read from a properties file.
 * @author jorge, vvenkatraman, michael, bwolf, aseem
 * Class is thread-safe
 */
public class CMBProperties {
	
	public enum IO_MODE {
	    SYNC, ASYNC 
	}
	
	private final String cmbDataCenter;
	
	private final IO_MODE cnsIOMode;

	private final int cqsLongPollPort;
	private final boolean cqsLongPollEnabled;
	
	private final int cnsMessageExpirationSeconds;
	
	private final int httpTimeoutSeconds;
	private final List<String> acceptableHttpStatusCodes;

	private final int endpointFailureCountToSuspensionThreshold;
	
    private final String cnsServerUrl;
	private final String cqsServerUrl;
	private final String cnsAdminUrl;
	private final String cqsAdminUrl;
	
	private final String cnsUserName;
	private final String cnsUserPassword;
	
	private final String awsAccessKey;
	private final String awsAccessSecret;
	
	private final String cnsPublishQueueNamePrefix;
	private final String cnsEndpointPublishQueueNamePrefix;

	private final String clusterName;
	private final String clusterUrl;
	
    private final int hectorPoolSize;
    private final String hectorBalancingPolicy;
	
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
    
    private final int visibilityTO;
    private final int maxMsgCountBatch;
    private final int maxMsgSize;
    private final int maxMsgSizeBatch;
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
    private final int redisPayloadCacheSizePerQueue;
    private final int redisPayloadCacheTTLSec;
    private volatile boolean redisPayloadCacheEnabled;
    
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
        
	private static final CMBProperties instance = new CMBProperties();

    private static final Logger log = Logger.getLogger(CMBProperties.class);
	
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
            throw new IllegalArgumentException("Missing VM parameter cmb.propertyFile");
        }

		try {
			
			FileInputStream fileStream = new FileInputStream(file);
			props.load(fileStream);
			
			cmbDataCenter = props.getProperty("cmb.dc.name", "default");
			
			cqsLongPollPort = Integer.parseInt(props.getProperty("cmb.cqs.longpoll.port", "5555"));
			cqsLongPollEnabled = Boolean.parseBoolean(props.getProperty("cmb.cqs.longpoll.enable", "true"));
			
			cnsServerUrl = props.getProperty("cmb.cns.server.url");
			cqsServerUrl = props.getProperty("cmb.cqs.server.url");
			cnsAdminUrl = props.getProperty("cmb.cns.admin.url", cnsServerUrl);
			cqsAdminUrl = props.getProperty("cmb.cqs.admin.url", cqsServerUrl);

			cnsUserName = props.getProperty("cmb.cns.user.name", "cns_internal");
			cnsUserPassword = props.getProperty("cmb.cns.user.password", "cns_internal");
			
			awsAccessKey = props.getProperty("aws.access.key");
			awsAccessSecret = props.getProperty("aws.access.secret");

			cnsPublishQueueNamePrefix = props.getProperty("cmb.cns.publish.queueNamePrefix", "PublishJobQ_");
			cnsEndpointPublishQueueNamePrefix = props.getProperty("cmb.cns.publish.endpointQueueNamePrefix", "EndpointPublishQ_");

			clusterName = props.getProperty("cmb.cassandra.clusterName");
			clusterUrl = props.getProperty("cmb.cassandra.clusterUrl");
			
            hectorPoolSize = Integer.parseInt(props.getProperty("cmb.hector.pool.size", "50"));
            hectorBalancingPolicy = props.getProperty("cmb.hector.balancingPolicy", "RoundRobinBalancingPolicy");
            
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
            
            visibilityTO = Integer.parseInt(props.getProperty("cmb.cqs.visibilityTO", "30"));
            maxMsgCountBatch = Integer.parseInt(props.getProperty("cmb.cqs.maxMsgCountBatch", "10"));
            maxMsgSize = Integer.parseInt(props.getProperty("cmb.cqs.maxMsgSize", "65536"));
            maxMsgSizeBatch = Integer.parseInt(props.getProperty("cmb.cqs.maxMsgSizeBatch", "65536"));
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
            
            redisConnectionsMaxActive = Integer.parseInt(props.getProperty("cmb.redis.connectionsMaxActive", "20"));
            redisServerList = props.getProperty("cmb.redis.serverList");
            redisFillerThreads = Integer.parseInt(props.getProperty("cmb.redis.fillerThreads", "5"));
            redisRevisibleThreads = Integer.parseInt(props.getProperty("cmb.redis.revisibleThreads", "3"));
            redisExpireTTLSec = Integer.parseInt(props.getProperty("cmb.redis.expireTTLSec", "1209600"));
            redisPayloadCacheSizePerQueue = Integer.parseInt(props.getProperty("cmb.redis.payloadCacheSizePerQueue", "10000"));
            redisPayloadCacheTTLSec = Integer.parseInt(props.getProperty("cmb.redis.payloadCacheTTLSec", "3600"));
            redisPayloadCacheEnabled = Boolean.parseBoolean(props.getProperty("cmb.redis.PayloadCacheEnabled", "true"));
            redisRevisibleFrequencySec = Integer.parseInt(props.getProperty("cmb.redis.revisibleFrequencySec", "10"));
            redisRevisibleSetFrequencySec = Integer.parseInt(props.getProperty("cmb.redis.revisibleSetFrequencySec", "1"));
            
            cassandraThriftSocketTimeOutMS = Integer.parseInt(props.getProperty("cmb.cassandra.thriftSocketTimeOutMS", "2000"));
            
            numEPPubJobProducers = Integer.parseInt(props.getProperty("cmb.cns.publisher.numProducers", "8"));
            numEPPubJobConsumers = Integer.parseInt(props.getProperty("cmb.cns.publisher.numConsumers", "4"));
            numPublishJobQs = Integer.parseInt(props.getProperty("cmb.cns.publisher.numPublishJobQs", "2"));
            numEPPublishJobQs = Integer.parseInt(props.getProperty("cmb.cns.publisher.numEPPublishJobQs", "4"));
            numDeliveryHandlers = Integer.parseInt(props.getProperty("cmb.cns.publisher.numDeliveryHandlers", "256"));
            numReDeliveryHandlers = Integer.parseInt(props.getProperty("cmb.cns.publisher.numReDeliveryHandlers", "256"));
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

		} catch (Exception e) {
            log.error("event=load_cmb_properties status=failed file="+System.getProperty("CMB.propertyFile"), e);
            throw new RuntimeException("Unable to load CMB.propertyFile");
		}
	}
	
	public int getEndpointFailureCountToSuspensionThreshold() {
		return endpointFailureCountToSuspensionThreshold;
	}

	public static CMBProperties getInstance() {			
		return instance;
	}
	
	public String getCNSServerUrl() {
		return cnsServerUrl;
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
	
	public String getCQSServerUrl() {
		return cqsServerUrl;
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
	
	public int getPublisherThreadPoolSize() {
	    return publisherThreadPoolSize;
	}
	
	public int getPublishJobQueueSizeLimit() {
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

    public boolean getRequireSubscriptionConfirmation() {
        return requireSubscriptionConfirmation;
    }

    public boolean getCNSServiceEnabled() {
        return cnsServiceEnabled;
    }

    public boolean getCQSServiceEnabled() {
        return cqsServiceEnabled;
    }

    public int getVisibilityTO() {
		return visibilityTO;
	}

	public int getMaxMsgSize() {
		return maxMsgSize;
	}
	
	public int getMaxMsgCountBatch() {
		return maxMsgCountBatch;
	}

	public int getMaxMsgSizeBatch() {
		return maxMsgSizeBatch;
	}

    public int getMsgRetentionPeriod() {
        return msgRetentionPeriod;
    }

    public int getMaxMsgRetentionPeriod() {
        return maxMsgRetentionPeriod;
    }

    public int getMinMsgRetentionPeriod() {
        return minMsgRetentionPeriod;
    }

    public int getDelaySeconds() {
        return delaySeconds;
    }

    public int getMaxDelaySeconds() {
        return maxDelaySeconds;
    }

	public int getMaxQueueNameLength() {
		return maxQueueNameLength;
	}

	public int getMaxMessageSuppliedIdLength() {
		return maxMessageSuppliedIdLength;
	}

	public int getMaxBatchEntries() {
		return maxBatchEntries;
	}

	public int getMaxReceiveMessageCount() {
		return maxReceiveMessageCount;
	}

	public int getMaxVisibilityTO() {
		return maxVisibilityTO;
	}
	
	public int getUserCacheExpiring() {
	    return userCacheExpiring;
	}

	public String getCMBCommonKeyspace() {
		return cmbCommonKeyspace;
	}

	public String getCMBCQSKeyspace() {
		return cmbCQSKeyspace;
	}

	public String getCMBCNSKeyspace() {
		return cmbCNSKeyspace;
	}

	public int getUserCacheSizeLimit() {
        return userCacheSizeLimit;
    }

	public String getSmtpReplyAddress() {
		return smtpReplyAddress;
	}

	public int getCnsCacheExpiring() {
	    return cnsCacheExpiring;
	}

	public int getCnsCacheSizeLimit() {
        return cnsCacheSizeLimit;
    }

	public int getHectorPoolSize() {
		return hectorPoolSize;
	}

	public int getCqsCacheExpiring() {
		return cqsCacheExpiring;
	}

	public int getCqsCacheSizeLimit() {
		return cqsCacheSizeLimit;
	}
	
	public String getHectorBalancingPolicy(){
		return hectorBalancingPolicy;
	}

	public int getCqsNumberOfQueuePartitions() {
		return cqsNumberOfQueuePartitions;
	}
	
    public int getRedisFillerThreads() {
        return redisFillerThreads;
    }

    public int getCassandraThriftSocketTimeOutMS() {
        return cassandraThriftSocketTimeOutMS;
    }

    public int getNumPublishJobQs() {
        return numPublishJobQs;
    }

    public int getNumEPPublishJobQs() {
        return numEPPublishJobQs;
    }

    public int getDeliveryHandlerJobQueueLimit() {
        return deliveryHandlerJobQueueLimit;
    }

    public String getCnsUserName() {
        return cnsUserName;
    }

    public String getCnsUserPassword() {
        return cnsUserPassword;
    }
    
    public String getCnsPublishQueueNamePrefix() {
    	return cnsPublishQueueNamePrefix;
    }

    public String getCnsEndpointPublishQueueNamePrefix() {
    	return cnsEndpointPublishQueueNamePrefix;
    }
    
	public String getAwsAccessSecret() {
		return awsAccessSecret;
	}

	public String getAwsAccessKey() {
		return awsAccessKey;
	}
	
    public void setUseSubInfoCache(boolean useSubInfoCache) {
        this.useSubInfoCache = useSubInfoCache;
    }

    public boolean isUseSubInfoCache() {
        return useSubInfoCache;
    }

    public int getConsumerProcessingMaxDelay() {
        return consumerProcessingMaxDelay;
    }

    public int getProducerProcessingMaxDelay() {
        return producerProcessingMaxDelay;
    }

    public int getPublishJobVTO() {
		return publishJobVTO;
	}

	public int getEPPublishJobVTO() {
        return EPPublishJobVTO;
    }

	public int getMaxSubscriptionsPerEPPublishJob() {
		return maxSubscriptionsPerEPPublishJob;
	}

	public int getReDeliveryHandlerJobQueueLimit() {
        return reDeliveryHandlerJobQueueLimit;
    }

    public void setPublishJobQueueSizeLimit(int publishJobQueueSizeLimit) {
        this.publishJobQueueSizeLimit = publishJobQueueSizeLimit;
    }

    public int getNumDeliveryHandlers() {
        return numDeliveryHandlers;
    }

    public int getNumReDeliveryHandlers() {
        return numReDeliveryHandlers;
    }

    public int getNumEPPubJobProducers() {
        return numEPPubJobProducers;
    }

    public int getNumEPPubJobConsumers() {
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

    public boolean isRedisPayloadCacheEnabled() {
        return redisPayloadCacheEnabled;
    }

    public void setRedisPayloadCacheEnabled(boolean redisPayloadCacheEnabled) {
        this.redisPayloadCacheEnabled = redisPayloadCacheEnabled;
    }

    public int getRedisPayloadCacheTTLSec() {
        return redisPayloadCacheTTLSec;
    }

    public int getRedisPayloadCacheSizePerQueue() {
        return redisPayloadCacheSizePerQueue;
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

    public int getHttpPublisherEndpointConnectionPoolSize() {
        return httpPublisherEndpointConnectionPoolSize;
    }

    public int getHttpPublisherEndpointConnectionsPerRouteSize() {
        return httpPublisherEndpointConnectionsPerRouteSize;
    }
    
	public int getHttpTimeoutSeconds() {
		return httpTimeoutSeconds;
	}
	
	public List<String> getAcceptableHttpStatusCodes() {
		return acceptableHttpStatusCodes;
	}
	
	public int getCnsMessageExpirationSeconds() {
		return cnsMessageExpirationSeconds;
	}

	public IO_MODE getCnsIOMode() {
		return cnsIOMode;
	}

	public int getCqsLongPollPort() {
		return cqsLongPollPort;
	}

	public boolean isCqsLongPollEnabled() {
		return cqsLongPollEnabled;
	}

	public String getCmbDataCenter() {
		return cmbDataCenter;
	}
}
