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
package com.comcast.cqs.test.unit;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cmb.test.tools.CMBAWSBaseTest;
import com.comcast.cqs.controller.CQSControllerServlet;
import com.comcast.cqs.persistence.RedisSortedSetPersistence;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.Util;

public class CQSSendDeleteReceiveMessageTest extends CMBAWSBaseTest {
	
	private static final String text = "2\n"
		+"4|arn:cmb:cqs:ccp:381515276957:a|arn:cmb:cns:ccp:381515276957:test:7334d7cc-ea66-4260-8b66-f28a18978e11|true\n"
		+"0|http://localhost:6059/Endpoint/recv/a|arn:cmb:cns:ccp:381515276957:test:bc4d8987-12fa-45e4-9569-595fd35cff02|true\n"
		+"1\n"
		+"\n"
		+"*\n"
		+"arn:cmb:cns:ccp:381515276957:test\n"
		+"1381614162483\n"
		+"381515276957\n"
		+"20db7499-8f6e-4116-8f77-91a69582c40b\n"
		+"Notification\n"
		+"{ \n"
		+"  default: <enter your message here>,\n"
		+"  email: <enter your message here>,\n"
		+"  cqs: <enter your message here>,\n"
		+"  http: <enter your message here>,\n"
		+"  https: <enter your message here> \n"
		+"}";


    @Test
    public void testSendDeleteReceiveMessage() {
    	
    	String queueUrl = getQueueUrl(1, USR.USER1);
    	//String message = "hello world!!!";
    	
    	String message = text;
			    	
    	cqs1.sendMessage(new SendMessageRequest(queueUrl, message));
    	
    	ReceiveMessageResult result = cqs1.receiveMessage(new ReceiveMessageRequest(queueUrl));
    	
    	if (result != null && result.getMessages().size() == 1) {
    		assertTrue("wrong message content: " + result.getMessages().get(0).getBody(), message.equals(result.getMessages().get(0).getBody()));
    		logger.info("event=message_found queue=" + queueUrl + " message=" + message);
    	} else {
    		fail("no message found in " + queueUrl);
    	}
    }
    
    @Test
    public void testSendDeleteReceiveMessageForCacheFiller() {
    	
    	String queueUrl = getQueueUrl(1, USR.USER1);
    	String ralativeUrl = Util.getRelativeForAbsoluteQueueUrl(queueUrl);
    	//String message = "hello world";
    	int num = 10;
    	String message = "test";
    	for (int i = 0 ; i < num; i++) {
    		cqs1.sendMessage(new SendMessageRequest(queueUrl, message + i));
/*    		try {
    		Thread.sleep(10);
    		} catch (Exception e) {
    			fail("exception while sleep " + e);
    		}*/
    	}

    	//delete redis queue
    	RedisSortedSetPersistence redisP = RedisSortedSetPersistence.getInstance();
    	try{

            
    		boolean brokenJedis = false;
            ShardedJedis jedis = null;
            int shard = 0;
            
            try {
                long ts1 = System.currentTimeMillis();
                jedis = redisP.getResource();
                Long clearNum = jedis.del(ralativeUrl + "-" + shard + "-" + CQSConstants.REDIS_STATE);
                logger.debug("num removed=" + clearNum);
                clearNum = jedis.del(ralativeUrl + "-" + shard + "-Q");
                logger.debug("num removed=" + clearNum);
                clearNum = jedis.del(ralativeUrl + "-" + shard + "-F");
                logger.debug("num removed=" + clearNum);
                long ts2 = System.currentTimeMillis();
                CQSControllerServlet.valueAccumulator.addToCounter(AccumulatorName.RedisTime, (ts2 - ts1));
                logger.debug("event=cleared_queue queue_url=" + queueUrl + " shard=" + shard);
            } catch (JedisConnectionException e) {
                logger.warn("event=clear_queue error_code=redis_unavailable num_connections=");
                brokenJedis = true;
            } finally {
                if (jedis != null) {
                	redisP.returnResource(jedis, brokenJedis);
                }
            }
        //end of clear cache
        logger.info("message_count after delete"+redisP.getCacheQueueMessageCount(ralativeUrl));
    	//test redis queue count is 0
    	assertTrue("wrong message count in Redis: " + redisP.getCacheQueueMessageCount(ralativeUrl), redisP.getCacheQueueMessageCount(ralativeUrl)==0);
    	//test if retrieve can get the same number of message and same body of message after cache filler
    	Map <String, String> resultMap = new HashMap<String, String>();
    	for (int i = 0; i < num; i ++) {
	    	ReceiveMessageResult result = cqs1.receiveMessage(new ReceiveMessageRequest(queueUrl));
	    	if (i == 0) {
	    		try {
	    		Thread.sleep(num);
	    		} catch (Exception e) {
	    			fail("exception while sleep " + e);
	    		}
	    	}
	    	
	    	if (result != null && result.getMessages().size() == 1) {
	    		String previousValue = resultMap.put(result.getMessages().get(0).getBody(), "OK");
	    		assertNull("duplicate message: " + result.getMessages().get(0).getBody() + "i is:" + i,previousValue);
	    		logger.info("message received: " + result.getMessages().get(0).getBody() + " i: " +i);
				DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, result.getMessages().get(0).getReceiptHandle());
				cqs1.deleteMessage(deleteMessageRequest);
	    	} 
	    	else {
	    		fail("no message found in " + queueUrl);
	    	}    	
    	}
    	assertTrue("message received: " + resultMap.size(), resultMap.size() == num);
    	} catch (Exception ex){
    		fail("fails in queue " + queueUrl);
    	}
    	
    }
    @Test
    public void testSendDeleteReceiveMessageZipped() throws IOException {
    	
    	String queueUrl = getQueueUrl(1, USR.USER1);
    	
    	Map<String, String> attributes = new HashMap<String, String>();
    	attributes.put("IsCompressed", "true");
    	
    	cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl,attributes));
    	
    	StringBuffer sb = new StringBuffer("");
    	String filename = "data/message.xml";
    	String message = "";
    	
    	if (new File(filename).exists()) {
    	    BufferedReader br = new BufferedReader(new FileReader(filename));
        	String line;
        	while ((line = br.readLine()) != null) {
        	   sb.append(line);
        	}
        	br.close();
        	message = sb.toString();
    	} else {
    		logger.info("using small test message");
    		message = text;
    	}
    	
    	cqs1.sendMessage(new SendMessageRequest(queueUrl, message.toString()));
    	ReceiveMessageResult result = cqs1.receiveMessage(new ReceiveMessageRequest(queueUrl));
    	
    	attributes.put("IsCompressed", "false");
    	cqs1.setQueueAttributes(new SetQueueAttributesRequest(queueUrl,attributes));
    	
    	if (result != null && result.getMessages().size() == 1) {
    		assertTrue("wrong message content: " + result.getMessages().get(0).getBody(), message.equals(result.getMessages().get(0).getBody()));
    		logger.info("event=message_found queue=" + queueUrl + " message=" + message);
    	} else {
    		fail("no message found in " + queueUrl);
    	}
    }
}
