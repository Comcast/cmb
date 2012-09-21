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
package com.comcast.plaxo.cns.util;

import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;

/**
 * Declare all the error codes we use
 * @author aseem, bwolf, jorge
 *
 */
public class CNSErrorCodes extends CMBErrorCodes {
    
    public static final CNSErrorCodes CNS_InvalidParameter = new CNSErrorCodes(400, "InvalidParameter");
    
    public static final CNSErrorCodes CNS_TopicLimitExceeded = new CNSErrorCodes(403, "TopicLimitExceeded"); // Indicates that the customer already owns the maximum allowed number of topics
    
    public static final CNSErrorCodes CNS_NotFound = new CNSErrorCodes(404, "NotFound"); // Indicates that the requested resource does not exist.

    public static final CNSErrorCodes CNS_SubscriptionLimitExceeded = new CNSErrorCodes(403, "SubscriptionLimitExceeded");
    
    public static final CNSErrorCodes CNS_TokenExpired = new CNSErrorCodes(403, "TokenExpired");
    
    public static final CNSErrorCodes CNS_ValidationError = new CNSErrorCodes(400, "ValidationError");

    protected CNSErrorCodes(int httpCode, String cmbCode) {
        super(httpCode, cmbCode);
    }
}
