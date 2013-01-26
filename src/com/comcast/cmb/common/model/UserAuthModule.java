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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Provide user authentication capability
 * @author bwolf, aseem, michael, vvenkatraman
 *
 */
public class UserAuthModule implements IAuthModule {

	private IUserPersistence userPersistence;
    private static ExpiringCache<String, User> userCache = new ExpiringCache<String, User>(CMBProperties.getInstance().getUserCacheSizeLimit());

    private static final Logger logger = Logger.getLogger(UserAuthModule.class);
    
    private static final List<String> ADMIN_ACTIONS = Arrays.asList(new String[] { "HealthCheck", "ClearCache", "GetAPIStats", "GetWorkerStats", "ManageWorker" });
    
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

        while (enumeration.hasMoreElements()) {
            String name = enumeration.nextElement();
            parameters.put(name, requestUrl.getParameter(name));
        }

        return parameters;
    }

    private Map<String, String> getAllHeaders(HttpServletRequest requestUrl) {
        
    	Enumeration<String> enumeration = requestUrl.getHeaderNames();
        Map<String, String> headers = new HashMap<String, String>();

        while (enumeration.hasMoreElements()) {
            String name = enumeration.nextElement();
            headers.put(name, requestUrl.getHeader(name));
        }

        return headers;
    }
    
    @Override
    public User authenticateByRequest(HttpServletRequest request) throws CMBException {
        
        Map<String, String> parameters = getAllParameters(request);
        Map<String, String> headers = getAllHeaders(request);
        
        // sample header
        
        //content-type=application/x-www-form-urlencoded; 
        //charset=utf-8 
        //x-amz-date=20130109T230435Z 
        //connection=Keep-Alive
        //host=localhost:7070
        //content-length=36
        //user-agent=aws-sdk-java/1.3.27 Mac_OS_X/10.7 Java_HotSpot(TM)_64-Bit_Server_VM/20.12-b01-434
        //authorization=AWS4-HMAC-SHA256 
        //Credential=50AL8M5BLRQB4LG5MU1C/20130109/us-east-1/us-east-1/aws4_request
        //SignedHeaders=host;user-agent;x-amz-content-sha256;x-amz-date
        //Signature=f4afd88c15fc41aacae2dc8b7d014673a0a51b4bfc7f2932993329be65c3f2fd
        //x-amz-content-sha256=48a38266faf90970d6c7fea9b15e6ba366e5f6397c2970fc893f8a7b5e207bd0
        
        String accessKey = parameters.get("AWSAccessKeyId");
        
        if (accessKey == null && headers.containsKey("authorization")) {
        	
        	String authorization = headers.get("authorization");
        	
        	if (authorization.contains("Credential=") && authorization.contains("/")) {
        		
        		accessKey = authorization.substring(authorization.indexOf("Credential=") + "Credential=".length(), authorization.indexOf("/"));
        		
        		if (CMBProperties.getInstance().getEnableSignatureAuth()) {
                    throw new AuthenticationException(CMBErrorCodes.InvalidAccessKeyId, "AWS4 signatures currently not supported.");
        		}
        	}
        }
        
        if (accessKey == null) {
            logger.error("event=authentication status=failed request="+request+" message=missing_access_key");
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
                logger.error("event=authentication status=failed request="+request+" access_key="+accessKey+" message=invalid_accesskey");
                throw new AuthenticationException(CMBErrorCodes.InvalidAccessKeyId, "AccessKey="+accessKey+" is not valid");
            }
            
        } catch (Exception ex) {
            logger.error("event=authentication status=failed request="+request+" message=exception", ex);
            throw new AuthenticationException(CMBErrorCodes.InvalidAccessKeyId, "AccessKey="+accessKey+" is not valid");
        }
        
        // admin actions do not require signatures but can only be performed by admin user
        
        if (ADMIN_ACTIONS.contains(parameters.get("Action"))) {
        	if (CMBProperties.getInstance().getCnsUserName().equals(user.getUserName())) {
                logger.debug("event=authentication status=success action=admin_action");
        		return user;
        	} else {
                throw new AuthenticationException(CMBErrorCodes.InvalidAccessKeyId, "User not authorized to perform admin actions.");
        	}
        }

        if (!CMBProperties.getInstance().getEnableSignatureAuth()) {
            logger.debug("event=authentication status=success EnableSignatureAuth=false");
            return user;
        }
        
        String signatureToCheck = parameters.get("Signature");
        
        if (signatureToCheck == null) {
            logger.error("event=authentication status=failed request="+request+" message=cannot_find_signature");
            throw new AuthenticationException(CMBErrorCodes.MissingParameter, "Signature not found");
        }

        String timeStamp = parameters.get("Timestamp");
        String expiration = parameters.get("Expires");
        
        if (timeStamp != null) {
            AuthUtil.checkTimeStamp(timeStamp);
        } else if (expiration != null) {
            AuthUtil.checkExpiration(expiration);
        } else {
            logger.error("event=authentication status=failed request="+request+" message=no_time_stamp_or_expiration");
            throw new AuthenticationException(CMBErrorCodes.MissingParameter, "Request must provide either Timestamp or Expires parameter");
        }

        String version = parameters.get("SignatureVersion");
        
        if (!version.equals("1") && !version.equals("2")) {
        	logger.error("event=authentication status=failed request="+request+" signature_version="+version+" message=invalid_signature_version");
            throw new AuthenticationException(CMBErrorCodes.NoSuchVersion, "SignatureVersion="+version+" is not valid");
        }

        String signatureMethod = parameters.get("SignatureMethod");
        
        if (!signatureMethod.equals("HmacSHA256") && !signatureMethod.equals("HmacSHA1")) {	
            logger.error("event=authentication status=failed request="+request+" signature_method="+signatureMethod+" message=invalid_signature_method");
            throw new AuthenticationException(CMBErrorCodes.InvalidParameterValue, "SignatureMethod="+signatureMethod+" is not valid");
        }
       
        URL url = null;
        String signature = null;
        
        try {
            url = new URL(request.getRequestURL().toString());
            parameters.remove("Signature");
        	signature = AuthUtil.generateSignature(url, parameters, version, signatureMethod, user.getAccessSecret());
        } catch (Exception ex) {
            logger.error("event=authentication status=failed request="+request+" url="+url+" message=invalid_url");
            throw new AuthenticationException(CMBErrorCodes.InternalError, "Invalid Url="+url);
        }

        if (signature == null || !signature.equals(signatureToCheck)) {
            logger.error("event=authentication status=failed request="+request+" signature="+signature+" signature_given="+signatureToCheck+" message=signature_mismatch");
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
