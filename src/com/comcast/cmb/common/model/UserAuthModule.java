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
import com.comcast.cmb.common.util.AuthUtil;
import com.comcast.cmb.common.util.AuthenticationException;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ExpiringCache;
import com.comcast.cmb.common.util.ExpiringCache.CacheFullException;

import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Provide user authentication capability
 * @author bwolf, aseem, michael, vvenkatraman
 *
 */
public class UserAuthModule implements IAuthModule {
    IUserPersistence userPersistence;
    
    //Cache user-ids to user instances
    static ExpiringCache<String, User> userCache = new ExpiringCache<String, User>(CMBProperties.getInstance().getUserCacheSizeLimit());

    private static final Logger logger = Logger.getLogger(UserAuthModule.class);
    
    public class UserCallable implements Callable<User> {
        String accessKey = null;
        public UserCallable(String k) {
            this.accessKey = k;
        }
        @Override
        public User call() throws Exception {
            User u = userPersistence.getUserByAccessKey(accessKey);
            return u;
        }
    }
    
    @Override
    public void setUserPersistence(IUserPersistence up) {
        userPersistence = up;
    }
    
    private Map<String, String> getAllParameters(HttpServletRequest requestUrl) {
        Enumeration<String> enumeration = requestUrl.getParameterNames();

        Map<String, String> parameters = new HashMap<String, String>();
        while(enumeration.hasMoreElements()) {
            String name = enumeration.nextElement();
            
            parameters.put(name, requestUrl.getParameter(name));
        }
        return parameters;
    }

    @Override
    public User authenticateByRequest(HttpServletRequest requestUrl) throws CMBException {
        
        Map<String, String> parameters = getAllParameters(requestUrl);
        
        String accessKey = parameters.get("AWSAccessKeyId");
        
        if (accessKey == null) {
            logger.error("event=authentication status=failed request="+requestUrl+" message=missing_access_key");
            throw new AuthenticationException(CMBErrorCodes.InvalidAccessKeyId, "No Access Key provided");   
        }
        
        User user = null;
        
        try {

        	try {
                user = userCache.getAndSetIfNotPresent(accessKey, new UserCallable(accessKey), CMBProperties.getInstance().getUserCacheExpiring() * 1000);
            } catch (CacheFullException e) {
                user = new UserCallable(accessKey).call();
            }
            
            if (user == null) {
                logger.error("event=authentication status=failed request="+requestUrl+" access_key="+accessKey+" message=invalid_accesskey");
                throw new AuthenticationException(CMBErrorCodes.InvalidAccessKeyId, "AccessKey="+accessKey+" is not valid");
            }
            
        } catch (Exception ex) {
            logger.error("event=authentication status=failed request="+requestUrl+" message=exception", ex);
            throw new AuthenticationException(CMBErrorCodes.InvalidAccessKeyId, "AccessKey="+accessKey+" is not valid");
        }

        if (!CMBProperties.getInstance().getEnableSignatureAuth()) {
            logger.debug("event=authentication status=success EnableSignatureAuth=false");
            return user;
        }
        
        String signatureToCheck = parameters.get("Signature");
        
        if (signatureToCheck == null) {
            logger.error("event=authentication status=failed request="+requestUrl+" message=cannot_find_signature");
            throw new AuthenticationException(CMBErrorCodes.MissingParameter, "Signature not found");
        }

        String timeStamp = parameters.get("Timestamp");
        String expiration = parameters.get("Expires");
        
        if (timeStamp != null) {
            AuthUtil.checkTimeStamp(timeStamp);
        } else if (expiration != null) {
            AuthUtil.checkExpiration(expiration);
        } else {
            logger.error("event=authentication status=failed request="+requestUrl+" message=no_time_stamp_or_expiration");
            throw new AuthenticationException(CMBErrorCodes.MissingParameter, "Request must provide either Timestamp or Expires parameter");
        }

        String version = parameters.get("SignatureVersion");
        
        if (!version.equals("1") && !version.equals("2")) {
        	logger.error("event=authentication status=failed request="+requestUrl+" signature_version="+version+" message=invalid_signature_version");
            throw new AuthenticationException(CMBErrorCodes.NoSuchVersion, "SignatureVersion="+version+" is not valid");
        }

        String signatureMethod = parameters.get("SignatureMethod");
        
        if (!signatureMethod.equals("HmacSHA256") && !signatureMethod.equals("HmacSHA1")) {	
            logger.error("event=authentication status=failed request="+requestUrl+" signature_method="+signatureMethod+" message=invalid_signature_method");
            throw new AuthenticationException(CMBErrorCodes.InvalidParameterValue, "SignatureMethod="+signatureMethod+" is not valid");
        }
       
        URL url = null;
        String signature = null;
        
        try {
            url = new URL(requestUrl.getRequestURL().toString());
            parameters.remove("Signature");
        	signature = AuthUtil.generateSignature(url, parameters, version, signatureMethod, user.getAccessSecret());
        } catch (Exception ex) {
            logger.error("event=authentication status=failed request="+requestUrl+" url="+url+" message=invalid_url");
            throw new AuthenticationException(CMBErrorCodes.InternalError, "Invalid Url="+url);
        }

        if (signature == null || !signature.equals(signatureToCheck)) {
            logger.error("event=authentication status=failed request="+requestUrl+" signature="+signature+" signature_given="+signatureToCheck+" message=signature_mismatch");
            throw new AuthenticationException(CMBErrorCodes.InvalidSignature, "Invalid signature");
        }

        logger.debug("event=authentication status=success");

        return user;
    }

    @Override
    public User authenticateByPassword(String username, String password) throws CMBException {
        
    	try {
	        User user = userPersistence.getUserByName(username);
            
	        if (user == null) {
                logger.error("event=authentication status=failed username="+username+" message=user_not_found");
	            throw new AuthenticationException(CMBErrorCodes.AuthFailure, "User="+username+" not found");
	        }
	        
	        if (!AuthUtil.verifyPassword(password, user.getHashPassword())) {
                logger.error("event=authentication status=failed username="+username+" password="+password+" message=invalid_password");
	            throw new AuthenticationException(CMBErrorCodes.AuthFailure, "User="+username+" invalid password");
	        }
	        
	        return user;
	        
        } catch (Exception ex) {
            logger.error("event=authentication status=failed username="+username+" message=exception", ex);
            throw new AuthenticationException(CMBErrorCodes.AuthFailure, "User="+username+" not found");
        }
    }
}
