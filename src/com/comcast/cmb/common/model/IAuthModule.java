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
package com.comcast.cmb.common.model;

import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.util.CMBException;

import javax.servlet.http.HttpServletRequest;

/**
 * @author michael, bwolf, baosen
 * Interface representing functionality to authenticate users
 */
public interface IAuthModule {

    public void setUserPersistence(IUserPersistence userPersistence);

    /**
     * Authenticate the request by checking the signature provided in the parameters. 
     * Assumed that the request has the following parameters:
     *  - AWSAccessKeyId 
     *  - Signature 
     *  - Timestamp
     *  - Expires
     *  - SignatureVersion
     *  - SignatureMethod
     * @param request
     * @return On succesful authentication returns an instance of the User making the request
     * @throws CMBException
     */
    public User authenticateByRequest(HttpServletRequest request) throws CMBException;

    /**
     * Authenticates a user given their username and hashedPassword
     * @param username
     * @param password the MD5 hashed password
     * @return
     * @throws CMBException
     */
    public User authenticateByPassword(String username, String password) throws CMBException;
}
