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
package com.comcast.plaxo.cns.tools;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.comcast.plaxo.cmb.common.controller.CMBControllerServlet;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.persistence.UserCassandraPersistence;
import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cmb.common.util.PersistenceException;
import com.comcast.plaxo.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.plaxo.cns.controller.CNSMonitor;
import com.comcast.plaxo.cns.io.EndpointPublisherFactory;
import com.comcast.plaxo.cns.io.IEndpointPublisher;
import com.comcast.plaxo.cns.model.CNSEndpointPublishJob;
import com.comcast.plaxo.cns.model.CNSEndpointPublishJob.SubInfo;
import com.comcast.plaxo.cns.model.CNSMessage;
import com.comcast.plaxo.cns.model.CNSRetryPolicy;
import com.comcast.plaxo.cns.model.CNSSubscriptionAttributes;
import com.comcast.plaxo.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.plaxo.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.plaxo.cns.persistence.CachedCNSEndpointPublishJob;
import com.comcast.plaxo.cns.persistence.ICNSAttributesPersistence;
import com.comcast.plaxo.cns.persistence.SubscriberNotFoundException;
import com.comcast.plaxo.cns.persistence.TopicNotFoundException;
import com.comcast.plaxo.cns.util.Util;

/**
 * 
 * This is the Endpoint Job Consumer. Its role is to dequeue CNSEndpointPublishJobs from
 * CQS, send the notifications and retry if needed
 * This class uses two internal executors to send notifications: one for normal notifications and one for retrys
 * @author aseem, bwolf
 * 
 * Class is thread-safe
 */

public class CNSEndpointPublisherJobConsumer implements RunnableForPartition {
	
    private static Logger logger = Logger.getLogger(CNSEndpointPublisherJobConsumer.class);
    
    public static final String CNS_ENDPOINT_PUBLISH_QUEUE_NAME_PREFIX = CMBProperties.getInstance().getCnsEndpointPublishQueueNamePrefix();

    static volatile ScheduledThreadPoolExecutor deliveryHandlers = null;
    static volatile ScheduledThreadPoolExecutor reDeliveryHandlers = null;
    static volatile boolean initialized = false; 
    static volatile AmazonSQS sqs;
    static volatile Integer testQueueLimit = null;
            
    public static class MonitoringInterface {
        public static int getDeliveryHandlersQueueSize() {
            return deliveryHandlers.getQueue().size();
        }
        public static int getReDeliveryHandlersQueueSize() {
            return reDeliveryHandlers.getQueue().size();
        }
    }
    public static class TestInterface {
    	
        public static boolean isInitialized() {
            return initialized;
        }
        
        public static AmazonSQS getSQS() {
            return sqs;
        }
        
        public static void setAmazonSQS(AmazonSQS inst) {
            sqs = inst;
        }
        
        public static void setTestQueueLimit(Integer limit) {
            testQueueLimit = limit;
        }
        public static void clearDeliveryHandlerQueue() {
            deliveryHandlers.getQueue().clear();
        }
        public static void clearReDeliveryHandlerQueue() {
            reDeliveryHandlers.getQueue().clear();
        }
    }
    /**
     * Make the appr executors
     * Read the deliveryNumHandlers and the retryDeliveryNumHandlers properties at startup
     * Read the EndpointPublishQ_<m> property and ensuring they exist (create if not) 
     * @throws PersistenceException 
     */
    public static void initialize() throws PersistenceException {
    	
        deliveryHandlers = new ScheduledThreadPoolExecutor(CMBProperties.getInstance().getNumDeliveryHandlers());
        reDeliveryHandlers = new ScheduledThreadPoolExecutor(CMBProperties.getInstance().getNumReDeliveryHandlers());
       
        IUserPersistence userPersistence = new UserCassandraPersistence();        
        User user = userPersistence.getUserByName(CMBProperties.getInstance().getCnsUserName());

        if (user == null) {
            user = userPersistence.createUser(CMBProperties.getInstance().getCnsUserName(), CMBProperties.getInstance().getCnsUserPassword());
        }

        BasicAWSCredentials credentialsUser = new BasicAWSCredentials(user.getAccessKey(), user.getAccessSecret());
        sqs = new AmazonSQSClient(credentialsUser);
        sqs.setEndpoint(CMBProperties.getInstance().getCQSServerUrl());
        
        for (int i = 0; i < CMBProperties.getInstance().getNumEPPublishJobQs(); i++) {
        	
            GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(CNS_ENDPOINT_PUBLISH_QUEUE_NAME_PREFIX + i);
            
            try {
                sqs.getQueueUrl(getQueueUrlRequest);
            } catch (AmazonServiceException e) {
            	
                if (e.getStatusCode() == 400) {
                	
                    logger.info("event=creating_missing_queue name=" + CNS_ENDPOINT_PUBLISH_QUEUE_NAME_PREFIX + i);

                    CreateQueueRequest createQueueRequest = new CreateQueueRequest(CNS_ENDPOINT_PUBLISH_QUEUE_NAME_PREFIX + i);
                    CreateQueueResult createQueueResponse = sqs.createQueue(createQueueRequest);
                    
                    if (createQueueResponse.getQueueUrl() == null) {
                        throw new IllegalStateException("Could not create queue with name " + CNS_ENDPOINT_PUBLISH_QUEUE_NAME_PREFIX + i);
                    }
                    
                    logger.info("event=created_missing_queue name=" + CNS_ENDPOINT_PUBLISH_QUEUE_NAME_PREFIX + i + " url=" + createQueueResponse.getQueueUrl());

                } else {
                    throw e;
                }
            }
        }
        
        logger.info("event=initialize status=success");
        initialized = true;
    }
    /**
     * shutsdown all internal thread-pools. Cannot use object again unless initialize() called again.
     */
    public static void shutdown() {
        deliveryHandlers.shutdownNow();
        reDeliveryHandlers.shutdownNow();
        initialized = false;
        logger.info("event=shutdown status=success");
    }
    
    private static void deleteMessage(String queueUrl, String receiptHandle) {
        long ts3 = System.currentTimeMillis();
        DeleteMessageRequest delMsgReq = new DeleteMessageRequest(queueUrl, receiptHandle);
        sqs.deleteMessage(delMsgReq);
        long ts4 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts4 - ts3);

    }
    /**
     * Helper class representing individual (to one endpoint) publish job
     *
     *
     * Class is thread-safe
     */
   
    public static class PublishJob implements Runnable {
        
        final CNSMessage message;
        final User user;
        final CnsSubscriptionProtocol protocol;
        final String endpoint;
        final String subArn;
        final String queueUrl;
        final String receiptHandle;
        final AtomicInteger endpointPublishJobCount;

        public static volatile IEndpointPublisher testPublisher = null; //set and used for unit-testing

        enum RetryPhase {
            None, ImmediateRetry, PreBackoff, Backoff, PostBackoff;
        }
        
        public volatile int numRetries = 0;
        volatile int maxDelayRetries = 0;
        
        /**
         * Change message visibility for the ep-job whose receipt handle we have by delay amount 
         */
        private void changeMessageVisibilityEpJob(int delaySec) {
            long ts3 = System.currentTimeMillis();
            ChangeMessageVisibilityRequest req = new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, delaySec);
            sqs.changeMessageVisibility(req);
            long ts4 = System.currentTimeMillis();
            CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts4 - ts3);
            logger.debug("event=send_notification status=change_message_visibility message_id=" + message.getMessageId() + " delay=" + delaySec);
        }
        
        /**
         * Call this method only in retry mode for a single sub
         */
        public void doRetry(IEndpointPublisher pub, CnsSubscriptionProtocol protocol, String endpoint, String subArn) {
            
            try {
                
                ICNSAttributesPersistence attributePers = PersistenceFactory.getCNSAttributePersistence();
                CNSSubscriptionAttributes subAttr = attributePers.getSubscriptionAttributes(subArn);
                
                if (subAttr == null) {
                    throw new CMBException(CMBErrorCodes.InternalError, "Could not get subscription delivery policy for subscripiton " + subArn);
                }
                
                CNSSubscriptionDeliveryPolicy deliveryPolicy = subAttr.getEffectiveDeliveryPolicy();
                CNSRetryPolicy retryPolicy = deliveryPolicy.getHealthyRetryPolicy();
                logger.debug("retry_policy=" + retryPolicy);
                
                while (numRetries < retryPolicy.getNumNoDelayRetries()) {
                    
                    logger.info("event=immediate_retry num_retries=" + numRetries);
                    
                    //handle immediate retry phase
                    
                    try {
                        numRetries++;
                        runCommon(pub, protocol, endpoint, subArn);
                        return; //suceeded.
                    } catch(Exception e) {
                        logger.info("event=retry_failed phase=" + RetryPhase.ImmediateRetry.name() + " attempt=" + numRetries);
                    }
                }                    
                
                //handle pre-backoff phase
                
                if (numRetries < retryPolicy.getNumMinDelayRetries() + retryPolicy.getNumNoDelayRetries()) {
                    
                    logger.info("event=pre_backoff num_retries=" + numRetries + " mind_delay_target_secs=" + retryPolicy.getMinDelayTarget());
                    numRetries++;
                    
                    reDeliveryHandlers.schedule(this, retryPolicy.getMinDelayTarget(), TimeUnit.SECONDS);

                    changeMessageVisibilityEpJob(retryPolicy.getMinDelayTarget() + 1); //add 1 second buffer to avoid race condition                    
                    return;
                }
                
                //if reached here, in the backoff phase
                
                if (numRetries < retryPolicy.getNumRetries() - (retryPolicy.getNumMinDelayRetries() + retryPolicy.getNumNoDelayRetries() + retryPolicy.getNumMaxDelayRetries())) {
                    
                    numRetries++;
                    
                    int delay = Util.getNextRetryDelay(numRetries - retryPolicy.getNumMinDelayRetries() - retryPolicy.getNumNoDelayRetries(), 
                            retryPolicy.getNumRetries() - retryPolicy.getNumMinDelayRetries() - retryPolicy.getNumNoDelayRetries(),
                            retryPolicy.getMinDelayTarget(), retryPolicy.getMaxDelayTarget(), retryPolicy.getBackOffFunction());
                    
                    logger.info("event=retry_notification phase=" + RetryPhase.Backoff.name() + " delay=" + delay + " attempt=" + numRetries + " backoff_function=" + retryPolicy.getBackOffFunction().name());
                    
                    reDeliveryHandlers.schedule(this, delay, TimeUnit.SECONDS);
                    
                    changeMessageVisibilityEpJob(delay + 1);//add 1 second buffer to avoid race condition
                    return;
                }                    
                
                if (numRetries < retryPolicy.getNumRetries()) { //remainder must be post-backoff
                    
                    logger.info("event=post_backoff max_delay_retries=" + maxDelayRetries + " max_delay_target=" + retryPolicy.getMaxDelayTarget());
                    maxDelayRetries++;
                    
                    reDeliveryHandlers.schedule(this, retryPolicy.getMaxDelayTarget(), TimeUnit.SECONDS);
                    
                    changeMessageVisibilityEpJob(retryPolicy.getMaxDelayTarget() + 1); //add 1 second buffer to avoid race condition
                    return;
                } 
                
                logger.info("event=retries_exhausted action=skip_message endpoint=" + endpoint + " message=" + message);
                
                //let message die 
                if (endpointPublishJobCount.decrementAndGet() == 0) {
                    deleteMessage(queueUrl, receiptHandle);
                    logger.debug("event=send_notification status=deleting_publish_message message_id=" + message.getMessageId());
                }

                
            } catch (SubscriberNotFoundException e) {
                logger.error("event=retry_error status=subscriber_not_found subArn=" + subArn);                
            } catch (Exception e) {
                logger.error("event=retry_error endpoint=" + endpoint + " protocol=" + protocol + " message_length=" + message.getMessage().length() + (user == null ?"":" " + user), e);
            }            
        }        
        /**
         * Send the notification and let any exceptions bubble up
         * @param pub
         * @param protocol
         * @param endpoint
         * @param subArn
         * @throws Exception
         */
        public void runCommon(IEndpointPublisher pub, CnsSubscriptionProtocol protocol, String endpoint, String subArn) throws Exception {
            long ts1 = System.currentTimeMillis();
            logger.debug("event=run_common protocol=" + protocol + " endpoint=" + endpoint + " sub_arn=" + subArn + " pub=" + pub);
            pub.setEndpoint(endpoint);
            pub.setMessage(message.getProtocolSpecificProcessedMessage(protocol));
            pub.setSubject(message.getSubject());            
            pub.setUser(user);
            pub.send();
            long ts2 = System.currentTimeMillis();
            CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSPublishSendTime, ts2 - ts1);
            logger.debug("event=send_notification status=success endpoint=" + endpoint + " protocol=" + protocol.name() + " message_length=" + message.getMessage().length() + (user == null ?"":" " + user));
            
            //decrement the total number of sub-tasks and if counter is 0 delete publishjob
            
            if (endpointPublishJobCount.decrementAndGet() == 0) {
                deleteMessage(queueUrl, receiptHandle);
                logger.debug("event=send_notification status=deleting_publish_message message_id=" + message.getMessageId());
            }

            CNSMonitor.getInstance().registerSendsRemaining(message.getMessageId(), -1);
            CNSMonitor.getInstance().registerBadEndpoint(endpoint, 0, 1, message.getTopicArn());
        }
        
        public void runCommonAndRetry(IEndpointPublisher pub, CnsSubscriptionProtocol protocol, String endpoint, String subArn) {
            
            try {
            
                logger.debug("event=run_common_and_retry protocol=" + protocol + " endpoint=" + endpoint + " sub_arn=" + subArn);
                runCommon(pub, protocol, endpoint, subArn);
            
            } catch(Exception e) {
            
                logger.error("event=error_notifying_subscriber endpoint=" + endpoint + " protocol=" + protocol + " message_length=" + message.getMessage().length() + (user == null ?"":" " + user), e);
                CNSMonitor.getInstance().registerBadEndpoint(endpoint, 1, 1, message.getTopicArn());
                
                if (protocol == CnsSubscriptionProtocol.http || protocol == CnsSubscriptionProtocol.https) {
                    doRetry(pub, protocol, endpoint, subArn);
                }
            }            
        }        
        
        public PublishJob(CNSMessage message, User user, CnsSubscriptionProtocol protocol, String endpoint, String subArn, String queueUrl, String receiptHandle, AtomicInteger endpointPublishJobCount) {
        	this.message = message; 
            this.user = user;
            this.protocol = protocol;
            this.endpoint = endpoint;
            this.subArn = subArn;
            this.queueUrl = queueUrl;
            this.receiptHandle = receiptHandle;
            this.endpointPublishJobCount = endpointPublishJobCount;
        }

        @Override
        public void run() {
            
            long ts1 = System.currentTimeMillis();
            CMBControllerServlet.valueAccumulator.initializeAllCounters();
                        
            if (testPublisher != null) {
                logger.debug("event=test_publisher_not_null publisher=" + testPublisher);
            } else {
                logger.debug("event=test_publisher_null protocol=" + protocol.name());
            }
            
            IEndpointPublisher pub = (testPublisher == null ? EndpointPublisherFactory.getPublisherInstance(protocol) : testPublisher);
            runCommonAndRetry(pub, protocol, endpoint, subArn);
            
            long ts2 = System.currentTimeMillis();            
            logger.info("event=notifying_subscriber endpoint=" + endpoint + " protocol=" + protocol.name() + " message_length=" + message.getMessage().length() + (user == null ?"":" " + user.getUserName()) + " responseTimeMS=" + (ts2 - ts1) + " CassandraTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime) + " publishTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSPublishSendTime) + " CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime));
            CMBControllerServlet.valueAccumulator.deleteAllCounters();
        }        
    }
    
    /**
     * Server is overloaded if 
     *  - the size of queue for deliveryHandlers is larger than deliveryHandlerQLimit OR
     *  - The size of the reDeliveryHandlers is larger than reDeliveryHandlerQLimit
     * @return true if overloaded
     */
    public static boolean isOverloaded() {

    	if (testQueueLimit == null) {
        
    		if (deliveryHandlers.getQueue().size() >= CMBProperties.getInstance().getDeliveryHandlerJobQueueLimit() ||
                    reDeliveryHandlers.getQueue().size() >= CMBProperties.getInstance().getReDeliveryHandlerJobQueueLimit()) {
                return true;
            }
            
    		return false;
        
    	} else {
            logger.debug("event=is_overloaded queue_size=" + deliveryHandlers.getQueue().size());
        
            if (deliveryHandlers.getQueue().size() >= testQueueLimit || reDeliveryHandlers.getQueue().size() >= testQueueLimit) {
                return true;
            }
            
            return false;
        }
    }
            
    /**
     * a. Check if server is overloaded, if it is, go to sleep and retry
     * c. Start polling the EndpointPublishQ_<m> queues in a round-robin manner, getting the list of endpoints and handing them to the delivery executor framework. 
     * d. Create another executor called the reDeliveryExecutor that gets the failed endpoints from ( c). 
     * e. Keep in-memory structure that maps all individual executor jobs to the main EndpointPublish job. Once all individual jobs have completed, delete the EndpointPublish Job.
     * f. Set HTTP and TCP handshake timeouts
     * 
     * Note: the method backs off exponentially by putting the thread to sleep.
     * @return true if messages were found in current partition, false otherwise
     */
    @Override
    public boolean run(int partition) {
        boolean messageFound = false;
        long ts0 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.initializeAllCounters();
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        
        try {
            if (isOverloaded()) {
                logger.info("event=run status=server_overloaded");

                try {
                    Thread.sleep(100);                
                } catch (InterruptedException e) {
                    logger.error("event=run status=error", e);
                }

                CMBControllerServlet.valueAccumulator.deleteAllCounters();
                return messageFound;
            }
            
            long ts1 = System.currentTimeMillis();
            GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(CNS_ENDPOINT_PUBLISH_QUEUE_NAME_PREFIX + partition);
            GetQueueUrlResult getQRes = sqs.getQueueUrl(getQueueUrlRequest);
            String qUrl = getQRes.getQueueUrl();
            long ts2 = System.currentTimeMillis();
            CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts2 - ts1);

            long ts3 = System.currentTimeMillis();
            ReceiveMessageRequest recvReq = new ReceiveMessageRequest(qUrl);
            recvReq.setMaxNumberOfMessages(1);
            recvReq.setVisibilityTimeout(CMBProperties.getInstance().getEPPublishJobVTO());
            ReceiveMessageResult recvRes = sqs.receiveMessage(recvReq);
            List<Message> msgs = recvRes.getMessages();
            logger.debug("event=run num_msg_received=" + msgs.size());
            long ts4 = System.currentTimeMillis();
            CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSCQSTime, ts4 - ts3);

            if (msgs.size() > 0) {       
                messageFound = true;
                try {
                    String msg = msgs.get(0).getBody();
                    CNSEndpointPublishJob job = (CMBProperties.getInstance().isUseSubInfoCache()) ? CachedCNSEndpointPublishJob.parseInstance(msg) : CNSEndpointPublishJob.parseInstance(msg);
                    logger.debug("endpoint_publish_job=" + job.toString());
                    User pubUser = PersistenceFactory.getUserPersistence().getUserById(job.getMessage().getUserId());
                    List<? extends SubInfo> subs = job.getSubInfos();
                    
                    CNSMonitor.getInstance().registerSendsRemaining(job.getMessage().getMessageId(), subs.size());
                    
                    AtomicInteger endpointPublishJobCount = new AtomicInteger(subs.size());                
                    
                    for (SubInfo sub : subs) {             
                        PublishJob pubjob = new PublishJob(job.getMessage(), pubUser, sub.protocol, sub.endpoint, sub.subArn, qUrl, msgs.get(0).getReceiptHandle(), endpointPublishJobCount);
                        deliveryHandlers.submit(pubjob);
                    }
                } catch (TopicNotFoundException e) {
                    logger.error("event=run_exception exception=TopicNotFound action=skip_job");
                    deleteMessage(qUrl, msgs.get(0).getReceiptHandle());
                } catch (Exception e) {
                    logger.error("event=run_exception action=wait_for_revisibility", e);
                }
                long tsFinal = System.currentTimeMillis();
                logger.info("event=run_pass_done CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime) + " responseTimeMS=" + (tsFinal - ts0));
            } else {
                logger.debug("event=run_pass_done");
            }
        } catch(Exception e) {
            logger.error("event=run status=exception", e);
        }
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
        return messageFound;
    }    
}
