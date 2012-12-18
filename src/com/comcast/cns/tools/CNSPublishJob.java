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
import com.comcast.cmb.common.util.CMBProperties;
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
    
    public String getSubscriptionArn() {
    	return subArn;
    }
    
    private void letMessageDieForEndpoint() {
    	
        // decrement the total number of sub-tasks and if counter is 0 delete publish job
        
    	if (endpointPublishJobCount.decrementAndGet() == 0) {
            CQSHandler.deleteMessage(queueUrl, receiptHandle);
            logger.info("event=deleting_publish_job_from_cqs message_id=" + message.getMessageId() + " queue_url=" + queueUrl + " receipt_handle=" + receiptHandle);
        }
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

            logger.debug("retry_policy=" + retryPolicy + "sub_arn=" + subArn);
            
            while (numRetries < retryPolicy.getNumNoDelayRetries()) {
                
                logger.info("event=immediate_retry num_retries=" + numRetries);
                
                // handle immediate retry phase
                
                try {
                    numRetries++;
                    runCommon(pub, protocol, endpoint, subArn);
                    return; //suceeded.
                } catch (Exception e) {
                    logger.info("event=retry_failed phase=" + RetryPhase.ImmediateRetry.name() + " attempt=" + numRetries);
                }
            }                    
            
            // handle pre-backoff phase
            
            if (numRetries < retryPolicy.getNumMinDelayRetries() + retryPolicy.getNumNoDelayRetries()) {
                
                logger.info("event=pre_backoff num_retries=" + numRetries + " mind_delay_target_secs=" + retryPolicy.getMinDelayTarget());
                numRetries++;
                
                CNSEndpointPublisherJobConsumer.submitForReDelivery(this, retryPolicy.getMinDelayTarget(), TimeUnit.SECONDS);
                
                // add 6 second buffer to avoid race condition (assuming we are enforcing a 5 sec http timeout)
                
                CQSHandler.changeMessageVisibility(queueUrl, receiptHandle, retryPolicy.getMinDelayTarget() + 6);                    
                
                return;
            }
            
            // if reached here, in the backoff phase
            
            if (numRetries < retryPolicy.getNumRetries() - (retryPolicy.getNumMinDelayRetries() + retryPolicy.getNumNoDelayRetries() + retryPolicy.getNumMaxDelayRetries())) {
                
                numRetries++;
                
                int delay = Util.getNextRetryDelay(numRetries - retryPolicy.getNumMinDelayRetries() - retryPolicy.getNumNoDelayRetries(), 
                        retryPolicy.getNumRetries() - retryPolicy.getNumMinDelayRetries() - retryPolicy.getNumNoDelayRetries(),
                        retryPolicy.getMinDelayTarget(), retryPolicy.getMaxDelayTarget(), retryPolicy.getBackOffFunction());
                
                logger.info("event=retry_notification phase=" + RetryPhase.Backoff.name() + " delay=" + delay + " attempt=" + numRetries + " backoff_function=" + retryPolicy.getBackOffFunction().name());
                
                CNSEndpointPublisherJobConsumer.submitForReDelivery(this, delay, TimeUnit.SECONDS);
                
                // add 6 second buffer to avoid race condition (assuming we are enforcing a 6 sec http timeout)

                CQSHandler.changeMessageVisibility(queueUrl, receiptHandle, delay + 6);
                
                return;
            }                    
            
            if (numRetries < retryPolicy.getNumRetries()) { //remainder must be post-backoff
                
                logger.info("event=post_backoff max_delay_retries=" + maxDelayRetries + " max_delay_target=" + retryPolicy.getMaxDelayTarget());
                maxDelayRetries++;
                
                CNSEndpointPublisherJobConsumer.submitForReDelivery(this, retryPolicy.getMaxDelayTarget(), TimeUnit.SECONDS);
                
                // add 6 second buffer to avoid race condition (assuming we are enforcing a 5 sec http timeout)

                CQSHandler.changeMessageVisibility(queueUrl, receiptHandle, retryPolicy.getMaxDelayTarget() + 6); 

                return;
            } 
            
            logger.info("event=retries_exhausted action=skip_message endpoint=" + endpoint + " message=" + message);
            
            letMessageDieForEndpoint();
            
        } catch (SubscriberNotFoundException e) {
            logger.error("event=retry_error status=subscriber_not_found sub_arn=" + subArn);                
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
        
        publisher.setEndpoint(endpoint);
        publisher.setMessage(message.getProtocolSpecificProcessedMessage(protocol));
        publisher.setSubject(message.getSubject());            
        publisher.setUser(user);
        publisher.send();
        
        logger.info("event=successful_delivery protocol=" + protocol + " endpoint=" + endpoint + " sub_arn=" + subArn + " attempt=" + numRetries);

        long ts2 = System.currentTimeMillis();
        
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CNSPublishSendTime, ts2 - ts1);
        
        letMessageDieForEndpoint();

        CNSMonitor.getInstance().registerSendsRemaining(message.getMessageId(), -1);
        CNSMonitor.getInstance().registerBadEndpoint(endpoint, 0, 1, message.getTopicArn());
        CNSMonitor.getInstance().registerPublishMessage();
    }
    
    private void runCommonAndRetry(IEndpointPublisher pub, CnsSubscriptionProtocol protocol, String endpoint, String subArn) {
        
        try {
        
            int slowResponseCounter = CNSEndpointPublisherJobConsumer.getNumSlowResponses(endpoint);

            int failureSuspensionThreshold = CMBProperties.getInstance().getEndpointFailureCountToSuspensionThreshold(); 
            
            // only throw away new messages for bad endpoints (not those that are already in redlivery) - in effect we are temporarily suspending 
            // endpoints with more than three failures in the last minute

            if (failureSuspensionThreshold!=0 && slowResponseCounter>failureSuspensionThreshold && numRetries==0) {
            	
            	logger.warn("event=throwing_message_away reason=endpoint_has_too_many_slow_responses_in_last_minute endpoint=" + endpoint + " slow_response_counter=" + slowResponseCounter + " attempt=" + numRetries);

            	letMessageDieForEndpoint();
            	
            } else {
            	runCommon(pub, protocol, endpoint, subArn);
            }
        
        } catch (Exception ex) {
        	
    		CNSEndpointPublisherJobConsumer.addSlowResponseEvent(endpoint);
        
            if (protocol == CnsSubscriptionProtocol.http || protocol == CnsSubscriptionProtocol.https) {
            	
            	int errorCode = 0;
            	
            	if (ex instanceof CMBException) {
            		errorCode = ((CMBException)ex).getHttpCode();
            	}
            
                if (CNSEndpointPublisherJobConsumer.acceptableHttpResponseCodes.contains(errorCode+"")) {
                    
                	// if posting fails with an acceptable http error code let it die
                	
                	logger.warn("event=failed_to_deliver_message action=skip_message status_code=" + errorCode + " endpoint=" + endpoint);

                	letMessageDieForEndpoint();

                } else {  
                	
                	// if posting fails with an unacceptable http error code register bad endpoint in rolling window of 60 sec and start retry process

                	logger.warn("event=failed_to_deliver_message action=retry status_code=" + errorCode + " endpoint=" + endpoint);

                	CNSMonitor.getInstance().registerBadEndpoint(endpoint, 1, 1, message.getTopicArn());
	            	doRetry(pub, protocol, endpoint, subArn);
                }
            
            } else {

            	// if sending fails for a non-http endpoint let it die immediately without retry
            	
            	logger.warn("event=failed_to_deliver_message action=skip_message endpoint=" + endpoint);
                
            	letMessageDieForEndpoint();
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
        logger.debug("event=metrics endpoint=" + endpoint + " protocol=" + protocol.name() + " message_length=" + message.getMessage().length() + (user == null ?"":" " + user.getUserName()) + " responseTimeMS=" + (ts2 - ts1) + " CassandraTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime) + " publishTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSPublishSendTime) + " CNSCQSTimeMS=" + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime));
        CMBControllerServlet.valueAccumulator.deleteAllCounters();
    }        
}