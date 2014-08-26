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

import com.comcast.cmb.common.util.CMBErrorCodes;

/**
 * declare all error codes for cqs
 * @author baosen, bwolf
 *
 */
public final class CQSErrorCodes extends CMBErrorCodes {
    
    public static final CQSErrorCodes NonExistentQueue = new CQSErrorCodes(400, "AWS.SimpleQueueService.NonExistentQueue"); //Queue does not exist.
        
    public static final CQSErrorCodes QueueDeletedRecently = new CQSErrorCodes(400, "AWS.SimpleQueueService.QueueDeletedRecently"); //You must wait 60 seconds after deleting a queue before you can create another with the same name.
        
    public static final CQSErrorCodes QueueNameExists = new CQSErrorCodes(400, "AWS.SimpleQueueService.QueueNameExists"); //Queue already exists. CQS returns this error only if the request includes an attribute name or value that differs from the name or value for the existing attribute.
    
    public static final CQSErrorCodes BatchEntryIdsNotDistinct = new CQSErrorCodes(400, "AWS.SimpleQueueService.BatchEntryIdsNotDistinct"); // The supplied id must be distinct

    public static final CQSErrorCodes BatchRequestTooLong = new CQSErrorCodes(400, "AWS.SimpleQueueService.BatchRequestTooLong"); // Batch requests cannot be longer than 65536 bytes

    public static final CQSErrorCodes TooManyEntriesInBatchRequest = new CQSErrorCodes(400, "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"); // Batch requests cannot have more than 10 entries
    
    public static final CQSErrorCodes InvalidBatchEntryId = new CQSErrorCodes(400, "AWS.SimpleQueueService.InvalidBatchEntryId"); // A batch entry id can only contain alphanumeric characters, hyphens and underscores.
    
    public static final CQSErrorCodes InvalidMessageContents = new CQSErrorCodes(400, "AWS.SimpleQueueService.InvalidMessageContents"); // The message contains characters outside the allowed set.

    public static final CQSErrorCodes BatchResultErrorEntry = new CQSErrorCodes(400, "AWS.SimpleQueueService.BatchResultErrorEntry"); //A message was not added to the queue.

    public static final CQSErrorCodes ReceiptHandleInvalid = new CQSErrorCodes(400, "AWS.SimpleQueueService.ReceiptHandleInvalid"); //A message was not added to the queue.

    protected CQSErrorCodes(int httpCode, String awsCode) {
        super(httpCode, awsCode);
    }
}
