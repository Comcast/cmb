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
package com.comcast.plaxo.cns.persistence;

import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBException;

/**
 * Exception representing subscriber not found
 * @author aseem
 *
 */
public class SubscriberNotFoundException extends CMBException {
    private static final long serialVersionUID = 1L;

    public SubscriberNotFoundException(String awsCode, String message) {
        super(CMBErrorCodes.NotFound.getHttpCode(), awsCode, message);
    }
    
    public SubscriberNotFoundException(String message) {
        super(CMBErrorCodes.NotFound, message);
    }

}
