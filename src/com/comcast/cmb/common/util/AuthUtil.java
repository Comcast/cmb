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

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Utility functions for authentication
 * @author michael, bwolf
 * 
 */
public class AuthUtil {

    private static final Logger logger = Logger.getLogger(AuthUtil.class);
    private static final int REQUEST_VALIDITY_PERIOD_MS = 900000; //15 mins
    private static final Random rand = new SecureRandom();
    
    public static String hashPassword(String password) throws Exception {

    	MessageDigest digest = MessageDigest.getInstance("MD5");

        String salt = getRandomString(4, SECRET_CHARS);
        String toBeHashed = salt + password;
        
        byte [] hashed = digest.digest(toBeHashed.getBytes("UTF-8"));
        
        StringBuilder sb = new StringBuilder(hashed.length * 2 + 8);

        byte [] saltBytes = salt.getBytes();

        for (int i = 0; i < 4; i++) {
            
        	String hex = Integer.toHexString(0xFF & saltBytes[i]);
            
        	if (hex.length() == 1) {
                sb.append('0');
            }

        	sb.append(hex);
        }

        for (int i = 0; i < hashed.length; i++) {
            
        	String hex = Integer.toHexString(0xFF & hashed[i]);
            
        	if (hex.length() == 1) {
                sb.append('0');
            }
            
        	sb.append(hex);
        }

        return sb.toString();
    }

    public static boolean verifyPassword(String password, String hashedPassword) throws Exception {

    	MessageDigest digest = MessageDigest.getInstance("MD5");

        byte [] hashedBytes = new BigInteger(hashedPassword, 16).toByteArray();
        
        String salt = new String(hashedBytes, 0, 4);
        String toBeHashed = salt + password;

        byte [] hashed = digest.digest(toBeHashed.getBytes("UTF-8"));
        
        for (int i = 0; i < hashed.length; i++) {
            if (hashed[i] != hashedBytes[i+4]) {
            	return false;
            }
        }

        return true;
    }

    private static final String KEY_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String SECRET_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+/";

    public static String generateRandomAccessKey() {
        return getRandomString(20, KEY_CHARS);
    }

    public static String generateRandomAccessSecret() {
        return getRandomString(40, SECRET_CHARS);
    }
    
    private static String getRandomString(int len, final String validChars) {
        
    	StringBuilder sb = new StringBuilder(len);
        
        for (int i = 0; i < len; i++) {
            sb.append(validChars.charAt(rand.nextInt(validChars.length())));
        }
        
        return sb.toString();
    }
    
    public static void checkTimeStamp(String ts) throws AuthenticationException {
        
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date timeStamp;
        
        try {
            timeStamp = dateFormat.parse(ts);
        } catch (ParseException ex) {
            logger.error("event=checking_timestamp status=failed timestamp="+ts+" message=invalid_format", ex);
            throw new AuthenticationException(CMBErrorCodes.InvalidParameterValue, "Timestamp="+ts+" is not valid");
        }

        Date now = new Date();

        if (now.getTime() - REQUEST_VALIDITY_PERIOD_MS < timeStamp.getTime() && now.getTime() + REQUEST_VALIDITY_PERIOD_MS > timeStamp.getTime()) {
            return;
        }

        logger.error("event=checking_timestamp status=failed timestamp="+ts+" serverTime="+dateFormat.format(now)+" message=timestamp_out_of_range");
        throw new AuthenticationException(CMBErrorCodes.RequestExpired, "Request timestamp="+ts+" must be within 900 seconds of the server time");
    }

    public static void checkExpiration(String expiration) throws AuthenticationException {
    	
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date timeStamp;
        
        try {
            timeStamp = dateFormat.parse(expiration);
        } catch (ParseException e) {
            logger.error("event=checking_expiration status=failed expiration="+expiration+" message=invalid_format", e);
            throw new AuthenticationException(CMBErrorCodes.InvalidParameterValue, "Expiration="+expiration+" is not valid");
        }

        Date now = new Date();
        
        if (now.getTime() < timeStamp.getTime()) {
            return;
        }

        logger.error("event=checking_timestamp status=failed expiration="+expiration+" serverTime="+dateFormat.format(now)+" message=request_expired");
        throw new AuthenticationException(CMBErrorCodes.RequestExpired, "Request expiration="+expiration+" already expired");
    }

    public static String generateSignature(URL url, Map<String, String> parameters, String version, String algorithm, String accessSecret) throws Exception {

        String data = null;

        if (version.equals("1")) {
            data = constructV1DataToSign(parameters);
        } else if (version.equals("2")) {
            parameters.put("SignatureMethod", algorithm);
            data = constructV2DataToSign(url, parameters);
        } else {
            return null;
        }
        
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(accessSecret.getBytes("UTF-8"), algorithm));
        byte[] bytes = mac.doFinal(data.getBytes("UTF-8"));
        String signature = new String(Base64.encodeBase64(bytes));
        
        return signature;
    }

    private static String constructV1DataToSign(Map<String, String> parameters) {

    	StringBuilder data = new StringBuilder();
        SortedMap<String, String> sortedParameters = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        sortedParameters.putAll(parameters);

        for (String key : sortedParameters.keySet()) {
            data.append(key);
            data.append(sortedParameters.get(key));
        }

        return data.toString();
    }

    private static String constructV2DataToSign(URL url, Map<String, String> parameters) throws UnsupportedEncodingException {
        
    	StringBuilder sb = new StringBuilder();
        sb.append("POST").append("\n");
        sb.append(normalizeURL(url)).append("\n");
        sb.append(normalizeResourcePath(url.getPath())).append("\n");
        sb.append(normalizeQueryString(parameters));
        
        return sb.toString();
    }

    private static String normalizeURL(URL url) {
        
    	String normalizedUrl = url.getHost().toLowerCase();
        
        // account for apache http client omitting standard ports

        if (url.getPort() != 80 && url.getPort() != 443) {
            normalizedUrl += ":" + url.getPort();
        }

        return normalizedUrl;
    }

    private static String normalizeResourcePath(String resourcePath) throws UnsupportedEncodingException {
    	
    	String normalizedResourcePath = null;

        if (resourcePath == null || resourcePath.length() == 0) {
        	normalizedResourcePath =  "/";
        } else {
        	normalizedResourcePath = urlEncode(resourcePath, true);
        }
        
        return normalizedResourcePath;
    }

    private static String normalizeQueryString(Map<String, String> parameters) throws UnsupportedEncodingException {

        SortedMap<String, String> sorted = new TreeMap<String, String>();
        sorted.putAll(parameters);

        StringBuilder builder = new StringBuilder();
        Iterator<Map.Entry<String, String>> pairs = sorted.entrySet().iterator();

        while (pairs.hasNext()) {

            Map.Entry<String, String> pair = pairs.next();
            String key = pair.getKey();
            String value = pair.getValue();
            builder.append(urlEncode(key, false));
            builder.append("=");
            builder.append(urlEncode(value, false));

            if (pairs.hasNext()) {
                builder.append("&");
            }
        }

        return builder.toString();
    }
    
    private static String urlEncode(String value, boolean isPath) throws UnsupportedEncodingException {
        
    	if (value == null) {
        	return "";
        }

        String encoded = URLEncoder.encode(value, "UTF-8").replace("+", "%20").replace("*", "%2A").replace("%7E", "~");

        if (isPath) {
            encoded = encoded.replace("%2F", "/");
        }

        return encoded;
    }
}
