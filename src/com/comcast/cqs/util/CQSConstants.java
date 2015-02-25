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
package com.comcast.cqs.util;

/**
 * declare all constants
 * @author baosen, vvenkatraman, aseem, bwolf
 *
 */
public class CQSConstants {
	
    public static final String ACTION_NAME = "ActionName";
    public static final String AWS_ACCOUNT_ID = "AWSAccountId";
    public static final String APPROXIMATE_FIRST_RECEIVE_TIMESTAMP = "ApproximateFirstReceiveTimestamp";	
	public static final String APPROXIMATE_RECEIVE_COUNT = "ApproximateReceiveCount";
	public static final String APPROXIMATE_NUMBER_OF_MESSAGES = "ApproximateNumberOfMessages";
	public static final String APPROXIMATE_NUMBER_OF_MESSAGES_NOTVISIBLE = "ApproximateNumberOfMessagesNotVisible";
	public static final String APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED = "ApproximateNumberOfMessagesDelayed";
    public static final String ATTRIBUTE_NAME = "AttributeName";
    public static final String MESSAGE_ATTRIBUTE_NAME = "MessageAttributeName";
    public static final String DELAY_SECONDS = "DelaySeconds";
    public static final String LABEL = "Label";
    public static final String MAXIMUM_MESSAGE_SIZE = "MaximumMessageSize";
    public static final String MAX_NUMBER_OF_MESSAGES = "MaxNumberOfMessages";
    public static final String MESSAGE_BODY = "MessageBody";
    public static final String MESSAGE_RETENTION_PERIOD = "MessageRetentionPeriod";
    public static final String POLICY = "Policy";
    public static final String QUEUE_ARN = "QueueArn";
    public static final String QUEUE_URL = "QueueUrl";
	public static final String RECEIPT_HANDLE = "ReceiptHandle";
    public static final String REQUEST_ENTRY = "RequestEntry.";
    public static final String SENDER_ID = "SenderId";
    public static final String SENT_TIMESTAMP = "SentTimestamp";
    public static final String VISIBILITY_TIMEOUT = "VisibilityTimeout";    
	public static final String LATEST_TIMESTAMP = "latestTimestamp";
	public static final String WAIT_TIME_SECONDS = "WaitTimeSeconds";
	public static final String RECEIVE_MESSAGE_WAIT_TIME_SECONDS = "ReceiveMessageWaitTimeSeconds";
	public static final String NUMBER_OF_PARTITIONS = "NumberOfPartitions";
	public static final String NUMBER_OF_SHARDS = "NumberOfShards";
	public static final String IS_COMPRESSED = "IsCompressed";
	public static final String MESSAGE_ATTRIBUTE = "MessageAttribute";
	public static final String MESSAGE_ATTRIBUTES = "MessageAttributes";

    public static final String REDIS_APPROXIMATE_FIRST_RECEIVE_TIMESTAMP = "AFRTS";	
	public static final String REDIS_APPROXIMATE_RECEIVE_COUNT = "ARC";
	public static final String REDIS_APPROXIMATE_NUMBER_OF_MESSAGES = "ANM";
	public static final String REDIS_STATE = "S";

	public static final String COL_ARN = "arn";
	public static final String COL_NAME = "name";
	public static final String COL_OWNER_USER_ID = "ownerUserId";
	public static final String COL_REGION = "region";
	public static final String COL_VISIBILITY_TO = "visibilityTO";
	public static final String COL_MAX_MSG_SIZE = "maxMsgSize";
	public static final String COL_MSG_RETENTION_PERIOD = "msgRetentionPeriod";
	public static final String COL_DELAY_SECONDS = "delaySeconds";
	public static final String COL_POLICY = "policy";
	public static final String COL_CREATED_TIME = "createdTime";
	public static final String COL_HOST_NAME = "hostName";
	public static final String COL_WAIT_TIME_SECONDS = "waitTimeSeconds";
	public static final String COL_NUMBER_PARTITIONS = "numPartitions";
	public static final String COL_NUMBER_SHARDS = "numShards";
	public static final String COL_COMPRESSED = "compressed";
}
