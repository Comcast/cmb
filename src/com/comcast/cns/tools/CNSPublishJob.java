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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cns.controller.CNSMonitor;
import com.comcast.cns.io.EndpointPublisherFactory;
import com.comcast.cns.io.IEndpointPublisher;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSRetryPolicy;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.persistence.ICNSAttributesPersistence;
import com.comcast.cns.persistence.SubscriberNotFoundException;
import com.comcast.cns.util.Util;

/**
 * Helper class representing individual (to one endpoint) publish job
 *
 *
 * Class is thread-safe
 */
public class CNSPublishJob implements Runnable {
	
    private static Logger logger = Logger.getLogger(CNSPublishJob.class);
    
    enum RetryPhase {
        None, ImmediateRetry, PreBackoff, Backoff, PostBackoff;
    }

    private final CNSMessage message;
    private final User user;
    private final CnsSubscriptionProtocol protocol;
    private final String endpoint;
    private final String subArn;
    private final String queueUrl;
    private final String receiptHandle;
    private final AtomicInteger endpointPublishJobCount;

    public static volatile IEndpointPublisher testPublisher = null; //set and used for unit-testing
    
    public volatile int numRetries = 0;
    private volatile int maxDelayRetries = 0;
    
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
                
                CNSEndpointPublisherJobConsumer.submitForReDeliver(this, retryPolicy.getMinDelayTarget(), TimeUnit.SECONDS);

                CQSHandler.changeMessageVisibility(queueUrl, receiptHandle, retryPolicy.getMinDelayTarget() + 1); //add 1 second buffer to avoid race condition                    
                return;
            }
            
            //if reached here, in the backoff phase
            
            if (numRetries < retryPolicy.getNumRetries() - (retryPolicy.getNumMinDelayRetries() + retryPolicy.getNumNoDelayRetries() + retryPolicy.getNumMaxDelayRetries())) {
                
                numRetries++;
                
                int delay = Util.getNextRetryDelay(numRetries - retryPolicy.getNumMinDelayRetries() - retryPolicy.getNumNoDelayRetries(), 
                        retryPolicy.getNumRetries() - retryPolicy.getNumMinDelayRetries() - retryPolicy.getNumNoDelayRetries(),
                        retryPolicy.getMinDelayTarget(), retryPolicy.getMaxDelayTarget(), retryPolicy.getBackOffFunction());
                
                logger.info("event=retry_notification phase=" + RetryPhase.Backoff.name() + " delay=" + delay + " attempt=" + numRetries + " backoff_function=" + retryPolicy.getBackOffFunction().name());
                
                CNSEndpointPublisherJobConsumer.submitForReDeliver(this, delay, TimeUnit.SECONDS);
                
                CQSHandler.changeMessageVisibility(queueUrl, receiptHandle, delay + 1);//add 1 second buffer to avoid race condition
                return;
            }                    
            
            if (numRetries < retryPolicy.getNumRetries()) { //remainder must be post-backoff
                
                logger.info("event=post_backoff max_delay_retries=" + maxDelayRetries + " max_delay_target=" + retryPolicy.getMaxDelayTarget());
                maxDelayRetries++;
                
                CNSEndpointPublisherJobConsumer.submitForReDeliver(this, retryPolicy.getMaxDelayTarget(), TimeUnit.SECONDS);
                
                CQSHandler.changeMessageVisibility(queueUrl, receiptHandle, retryPolicy.getMaxDelayTarget() + 1); //add 1 second buffer to avoid race condition
                return;
            } 
            
            logger.info("event=retries_exhausted action=skip_message endpoint=" + endpoint + " message=" + message);
            
            //let message die 

            if (endpointPublishJobCount.decrementAndGet() == 0) {
                CQSHandler.deleteMessage(queueUrl, receiptHandle);
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
     * @param publisher
     * @param protocol
     * @param endpoint
     * @param subArn
     * @throws Exception
     */
    private void runCommon(IEndpointPublisher publisher, CnsSubscriptionProtocol protocol, String endpoint, String subArn) throws Exception {
    	
        long ts1 = System.currentTimeMillis();
        logger.debug("event=run_common protocol=" + protocol + " endpoint=" + endpoint + " sub_arn=" + subArn + " pub=" + publisher);
        publisher.setEndpoint(endpoint);
        publisher.setMessage(message.getProtocolSpecificProcessedMessage(protocol));
        publisher.setSubject(message.getSubject());            
        publisher.setUser(user);
        publisher.send();
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSPublishSendTime, ts2 - ts1);
        logger.debug("event=send_notification status=success endpoint=" + endpoint + " protocol=" + protocol.name() + " message_length=" + message.getMessage().length() + (user == null ?"":" " + user));
        
        // decrement the total number of sub-tasks and if counter is 0 delete publish job
        
        if (endpointPublishJobCount.decrementAndGet() == 0) {
            CQSHandler.deleteMessage(queueUrl, receiptHandle);
            logger.debug("event=send_notification status=deleting_publish_message message_id=" + message.getMessageId());
        }

        CNSMonitor.getInstance().registerSendsRemaining(message.getMessageId(), -1);
        CNSMonitor.getInstance().registerBadEndpoint(endpoint, 0, 1, message.getTopicArn());
        CNSMonitor.getInstance().registerPublishMessage();
    }
    
    private void runCommonAndRetry(IEndpointPublisher pub, CnsSubscriptionProtocol protocol, String endpoint, String subArn) {
        
        try {
        
            logger.debug("event=run_common_and_retry protocol=" + protocol + " endpoint=" + endpoint + " sub_arn=" + subArn);
            runCommon(pub, protocol, endpoint, subArn);
        
        } catch (Exception e) {
        
            logger.error("event=error_notifying_subscriber endpoint=" + endpoint + " protocol=" + protocol + " message_length=" + message.getMessage().length() + (user == null ?"":" " + user), e);
            CNSMonitor.getInstance().registerBadEndpoint(endpoint, 1, 1, message.getTopicArn());
            
            if (protocol == CnsSubscriptionProtocol.http || protocol == CnsSubscriptionProtocol.https) {
                doRetry(pub, protocol, endpoint, subArn);
            } else {

            	logger.info("event=failed_to_deliver_message action=skip_message endpoint=" + endpoint + " message=" + message);
                
                // let message die 

                if (endpointPublishJobCount.decrementAndGet() == 0) {
                    CQSHandler.deleteMessage(queueUrl, receiptHandle);
                    logger.debug("event=send_notification status=deleting_publish_message message_id=" + message.getMessageId());
                }
            }
        }            
    }        
    
    public CNSPublishJob(CNSMessage message, User user, CnsSubscriptionProtocol protocol, String endpoint, String subArn, String queueUrl, String receiptHandle, AtomicInteger endpointPublishJobCount) {
    	
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