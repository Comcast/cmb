package com.comcast.cns.test.unit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.junit.Test;

import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;
import com.comcast.cmb.test.tools.CNSTestingUtils;

public class CNSJustCreateSubscriptionsTest extends CMBAWSBaseTest {
	
    private static List<String> endpointIPs = new ArrayList<String>(Arrays.asList(
    		CMBProperties.getInstance().getCNSServiceUrl()
    		/*"10.1.36.102:10159",
    		"10.1.36.105:10159",
    		"10.1.36.100:10159",
    		"10.1.1.91:10159",
    		"10.1.36.107:10159",
    		"10.1.36.104:10159",
    		"10.1.36.101:10159",
    		"10.1.36.106:10159",
    		"10.1.36.109:10159",
    		"10.1.1.90:10159",
    		"10.1.36.108:10159",
    		"10.1.36.103:10159"*/));
    
    private static Random rand = new Random();
    private static String topicArn = null;
    
    private class SubscriptionGenerator extends Thread {
    	
    	private int numSubscriptions;
    	
    	public SubscriptionGenerator(int numSubscriptions) {
    		this.numSubscriptions = numSubscriptions;
    	}
    	
    	@Override
    	public void run() {
			int count = 0;
			for (int i=0; i<numSubscriptions; i++) {
				String endpointUrl = null;
				try {
					endpointUrl = endpointIPs.get(rand.nextInt(endpointIPs.size())) + "Endpoint/recv/" + rand.nextInt();
					SubscribeRequest subscribeRequest = new SubscribeRequest();
					subscribeRequest.setEndpoint(endpointUrl);
					subscribeRequest.setProtocol("http");
					subscribeRequest.setTopicArn(topicArn);
					String subscriptionArn = cns1.subscribe(subscribeRequest).getSubscriptionArn();
					String lastMessageUrl = endpointUrl.replace("recv", "info") + "?showLast=true";
					if (subscriptionArn.equals("pending confirmation")) {
						String resp = CNSTestingUtils.sendHttpMessage(lastMessageUrl, "");
		    			JSONObject o = new JSONObject(resp);
		    			if (!o.has("SubscribeURL")) {
		    				logger.error("event=no_confirmation_request_found message=" + resp);
		    				continue;
		    			}
		    			String subscriptionUrl = o.getString("SubscribeURL");
						resp = CNSTestingUtils.sendHttpMessage(subscriptionUrl, "");
						logger.info("event=subscribed_endpoint url=" + endpointUrl);
						count++;
					}
				} catch (Exception ex) {
					logger.error("event=failed_to_subscribe url=" + endpointUrl + " topic_arn=" + topicArn, ex);
				}
			}
			logger.info("event=created_subscriptions count=" + count);
    	}
    }
    
    @Test
    public void justCreateSubscriptionsFast() {
		long start = System.currentTimeMillis();
		String topicName = "BigTopic100";
    	int numThreads = 10;
    	int numSubscriptions = 100;
    	topicArn = cns1.createTopic(new CreateTopicRequest(topicName)).getTopicArn();
		ScheduledThreadPoolExecutor ep = new ScheduledThreadPoolExecutor(numThreads + 2);
		for (int i=0; i<numThreads; i++) {
			ep.submit(new SubscriptionGenerator(numSubscriptions/numThreads));
		}
		logger.info("event=started");
		try {
			ep.shutdown();
			ep.awaitTermination(60, TimeUnit.MINUTES);
		} catch (InterruptedException ex) {
			logger.error("event=failure", ex);
		}
		long end = System.currentTimeMillis();
		logger.info("event=done duration=" + (end-start));
    }
}
