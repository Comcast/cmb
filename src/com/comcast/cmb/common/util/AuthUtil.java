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

package com.comcast.cmb.common.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.SigningAlgorithm;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.HttpUtils;
import com.comcast.cqs.controller.CQSHttpServletRequest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * Utility functions for authentication
 * @author michael, bwolf
 * 
 */
public class AuthUtil {

    private static final Logger logger = Logger.getLogger(AuthUtil.class);
    private static final int REQUEST_VALIDITY_PERIOD_MS = 900000; //15 mins
    private static final Random rand = new SecureRandom();
    protected static final String DEFAULT_ENCODING = "UTF-8";
    
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
        
    	checkTimeStampWithFormat(ts, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    }

    public static void checkTimeStampV4(String ts) throws AuthenticationException {
    	checkTimeStampWithFormat(ts, "yyyyMMdd'T'HHmmss'Z'");
    }
    
    public static void checkTimeStampWithFormat(String ts, String format) throws AuthenticationException {
        
    	SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date timeStamp;
        
        try {
            timeStamp = dateFormat.parse(ts);
        } catch (ParseException ex) {
            logger.error("event=checking_timestamp timestamp="+ts+" error_code=invalid_format", ex);
            throw new AuthenticationException(CMBErrorCodes.InvalidParameterValue, "Timestamp="+ts+" is not valid");
        }

        Date now = new Date();

        if (now.getTime() - REQUEST_VALIDITY_PERIOD_MS < timeStamp.getTime() && now.getTime() + REQUEST_VALIDITY_PERIOD_MS > timeStamp.getTime()) {
            return;
        }

        logger.error("event=checking_timestamp timestamp=" + ts + " serverTime=" + dateFormat.format(now) + " error_code=timestamp_out_of_range");
        throw new AuthenticationException(CMBErrorCodes.RequestExpired, "Request timestamp " + ts + " must be within 900 seconds of the server time");
    }
    public static void checkExpiration(String expiration) throws AuthenticationException {
    	
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date timeStamp;
        
        try {
            timeStamp = dateFormat.parse(expiration);
        } catch (ParseException e) {
            logger.error("event=checking_expiration expiration=" + expiration + " error_code=invalid_format", e);
            throw new AuthenticationException(CMBErrorCodes.InvalidParameterValue, "Expiration " + expiration +" is not valid");
        }

        Date now = new Date();
        
        if (now.getTime() < timeStamp.getTime()) {
            return;
        }

        logger.error("event=checking_timestamp expiration=" + expiration + " server_time=" + dateFormat.format(now) + " error_code=request_expired");
        throw new AuthenticationException(CMBErrorCodes.RequestExpired, "Request with expiration " + expiration + " already expired");
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

    public static String generateSignatureV4(HttpServletRequest request, URL url, Map<String, String> parameters, Map<String, String> headers, String version, String algorithm, String accessSecret) throws Exception {

    	/* Example of authorization header value
    	 * AWS4-HMAC-SHA256 Credential=XK1MWJAYYGQ41ECH06WG/20131126/us-east-1/us-east-1/aws4_request, SignedHeaders=host;user-agent;x-amz-date, Signature=18541c4db00d098414c0bae7394450d1deada902699a45de02849dbcb336f9e3
    	*/
    	String authorizationHeader = request.getHeader("authorization");
    	String credentialPart=authorizationHeader.substring(authorizationHeader.indexOf("Credential=") + "Credential=".length());
    	String [] credentialPartArray=credentialPart.split("/");
    	
        String regionName = credentialPartArray[2];
        String serviceName = credentialPartArray[3];

        String dateTime=request.getHeader("X-Amz-Date");
        String dateStamp = credentialPartArray[1];

        String scope=credentialPart.substring(credentialPart.indexOf("/")+1, credentialPart.indexOf(","));
    	
    	String payloadString=getPayload(request);
        String contentSha256= BinaryUtils.toHex(hash(payloadString));
        Map <String, String> filteredHeaders= filterHeader(headers);
        
        String stringToSign = getStringToSign("AWS4-"+algorithm, dateTime, scope, getCanonicalRequest(request,contentSha256, parameters, filteredHeaders ));


        byte[] secret = ("AWS4" + accessSecret).getBytes();
        byte[] date = sign(dateStamp, secret, SigningAlgorithm.HmacSHA256);
        byte[] region = sign(regionName, date, SigningAlgorithm.HmacSHA256);
        byte[] service = sign(serviceName, region, SigningAlgorithm.HmacSHA256);
        byte[] signing = sign("aws4_request", service, SigningAlgorithm.HmacSHA256);

        byte[] signatureBytes = sign(stringToSign.getBytes(), signing, SigningAlgorithm.HmacSHA256);


        String signature= BinaryUtils.toHex(signatureBytes);
        
        return signature;
    }
    
    public static byte[] sign(String stringData, byte[] key, SigningAlgorithm algorithm) throws AmazonClientException {
        try {
            byte[] data = stringData.getBytes("UTF-8");
            return sign(data, key, algorithm);
        } catch (Exception e) {
            throw new AmazonClientException("Unable to calculate a request signature: " + e.getMessage(), e);
        }
    }

    protected static byte[] sign(byte[] data, byte[] key, SigningAlgorithm algorithm) throws AmazonClientException {
        try {
            Mac mac = Mac.getInstance(algorithm.toString());
            mac.init(new SecretKeySpec(key, algorithm.toString()));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new AmazonClientException("Unable to calculate a request signature: " + e.getMessage(), e);
        }
    }
    
    protected static String getStringToSign(String algorithm, String dateTime, String scope, String canonicalRequest) {
        String stringToSign =
                algorithm + "\n" +
                        dateTime + "\n" +
                        scope + "\n" +
                        BinaryUtils.toHex(hash(canonicalRequest));
        logger.debug("AWS4 String to Sign: '\"" + stringToSign + "\"");
        return stringToSign;
    }

    public static byte[] hash(String text) throws AmazonClientException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(text.getBytes("UTF-8"));
            return md.digest();
        } catch (Exception e) {
            throw new AmazonClientException("Unable to compute hash while signing request: " + e.getMessage(), e);
        }
    }
    
    protected static String getCanonicalRequest(HttpServletRequest request, String contentSha256, Map<String, String> parameters, Map<String, String> headers) {


    	
        String canonicalRequest = null;
        canonicalRequest=request.getMethod() + "\n" +
						getResourcePath(request)+"\n"+
						getCanonicalizedQueryString(request, parameters) + "\n" +
                        getCanonicalizedHeaderString(headers) + "\n" +
                        getSignedHeadersString(headers) + "\n" +
                        contentSha256;
        logger.debug("AWS4 Canonical Request: '\"" + canonicalRequest + "\"");
        
        return canonicalRequest;
    }
   
    protected static String getCanonicalizedQueryString(HttpServletRequest request, Map <String, String> parameters) {
    		return "";
    }
    
    private static String getResourcePath(HttpServletRequest request){
    	String path=request.getRequestURI();
    	return path;
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

    private static Map< String, String> filterHeader(Map<String, String> headers){
    	Map <String, String> filteredHeaders =new HashMap<String, String>();
    	String authorizationString =headers.get("Authorization");
    	String singnedHeadersString = authorizationString.substring(authorizationString.indexOf("SignedHeaders=")+ new String("SignedHeaders=").length(),
    																authorizationString.indexOf(", Signature"));
    	String [] headersArray=singnedHeadersString.split(";");
    	//dealing with lower case letter
    	Map <String, String> lowerCaseHeaders =new HashMap<String, String>();
    	for(Entry <String, String> entry:headers.entrySet()){
    			lowerCaseHeaders.put(entry.getKey().toLowerCase(), entry.getKey());
    	}
    	
    	for(String currentHeaderName:headersArray){
    		if(lowerCaseHeaders.containsKey(currentHeaderName.trim())){
    			filteredHeaders.put(lowerCaseHeaders.get(currentHeaderName.trim()), headers.get(lowerCaseHeaders.get(currentHeaderName.trim())));
    		}
    	}
    	return filteredHeaders;
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

        if (url.getPort() > 0 && url.getPort() != 80 && url.getPort() != 443) {
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

    protected static String getCanonicalizedResourcePath(String resourcePath) {
        if (resourcePath == null || resourcePath.length() == 0) {
            return "/";
        } else {
            String value = HttpUtils.urlEncode(resourcePath, true);
            if (value.startsWith("/")) {
                return value;
            } else {
                return "/".concat(value);
            }
        }
    }
    
    protected static String getCanonicalizedHeaderString(Map<String, String> headers) {
        List<String> sortedHeaders = new ArrayList<String>();
        sortedHeaders.addAll(headers.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        StringBuilder buffer = new StringBuilder();
        for (String header : sortedHeaders) {
            buffer.append(header.toLowerCase().replaceAll("\\s+", " ") + ":" + headers.get(header).replaceAll("\\s+", " "));
            buffer.append("\n");
        }

        return buffer.toString();
    }
    
    protected static String getSignedHeadersString(Map<String, String> headers) {
        List<String> sortedHeaders = new ArrayList<String>();
        sortedHeaders.addAll(headers.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        StringBuilder buffer = new StringBuilder();
        for (String header : sortedHeaders) {
            if (buffer.length() > 0) buffer.append(";");
            buffer.append(header.toLowerCase());
        }

        return buffer.toString();
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
    
    private static String getPayload(HttpServletRequest reqeust) throws UnsupportedEncodingException {

    	return encodeParameters(reqeust);
    }
    
    /**
     * Creates an encoded query string from all the parameters in the specified
     * request.
     *
     * @param request
     *            The request containing the parameters to encode.
     *
     * @return Null if no parameters were present, otherwise the encoded query
     *         string for the parameters present in the specified request.
     */
    public static String encodeParameters(HttpServletRequest request) {
    	CQSHttpServletRequest wrappedRequest=(CQSHttpServletRequest)request;
        List<NameValuePair> nameValuePairs = null;
        String parameterName=null;
        if (wrappedRequest.getPostParameterNames().hasMoreElements()) {
            nameValuePairs = new ArrayList<NameValuePair>();
            while (wrappedRequest.getPostParameterNames().hasMoreElements()) {
            	parameterName=wrappedRequest.getPostParameterNames().nextElement();
                nameValuePairs.add(new BasicNameValuePair(parameterName, 
                		wrappedRequest.getPostParameter(parameterName)));
            }
        }

        String encodedParams = "";
        if (nameValuePairs != null) {
            encodedParams = URLEncodedUtils.format(nameValuePairs, DEFAULT_ENCODING);
        }

        return encodedParams;
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
