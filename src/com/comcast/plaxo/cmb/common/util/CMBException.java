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
package com.comcast.plaxo.cmb.common.util;

/**
 * Class represents generic CMBException
 * @author bwolf, baosen, aseem, michael
 * 
 * Class is immutable
 */
public class CMBException extends Exception {
    
    private static final long serialVersionUID = 1L;
    protected final int httpCode;
    protected final String cmbCode;

    /**
     * THe superclass exception for all ComcastMessageBus
     * @param httpCode the corresponding http error code
     * @param awsCode the corresponding AWS string error code
     * @param message A more descriptive message about teh error
     */
    public CMBException(int httpCode, String cmbCode, String message) {
        super(message);

        this.httpCode = httpCode;
        this.cmbCode = cmbCode;
    }

    public CMBException(CMBErrorCodes error, String message) {
        super(message);

        this.httpCode = error.getHttpCode();
        this.cmbCode = error.getCMBCode();
    }
    
    public int getHttpCode() {
        return httpCode;
    }
    
    public String getCMBCode() {
        return cmbCode;
    }
}
