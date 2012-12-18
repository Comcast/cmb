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

/**
 * class captures all the error codes
 * @author aseem, bwolf, baosen, michael
 * 
 * Class is immutable
 */

public class CMBErrorCodes {
	
    public static final CMBErrorCodes AccessDenied = new CMBErrorCodes(403, "AccessDenied"); // Access to the resource is denied.

    public static final CMBErrorCodes AuthFailure = new CMBErrorCodes(401, "AuthFailure"); // A value used for authentication could not be validated, such as Signature. For an example, see Example Response for AuthFailure Error.

    public static final CMBErrorCodes ConflictingQueryParameter = new CMBErrorCodes(400, "ConflictingQueryParameter"); // The query parameter <parameter> is invalid. Its structure conflicts with that of another parameter.

    public static final CMBErrorCodes InternalError = new CMBErrorCodes(500, "InternalError"); // There is an internal problem.
    
    public static final CMBErrorCodes InvalidAccessKeyId = new CMBErrorCodes(401, "InvalidAccessKeyId"); // not able to validate the provided access credentials.

    public static final CMBErrorCodes InvalidAction = new CMBErrorCodes(400, "InvalidAction"); // The action specified was invalid.

    public static final CMBErrorCodes InvalidAddress = new CMBErrorCodes(404, "InvalidAddress"); // The address <address> is not valid for this web service.

    public static final CMBErrorCodes InvalidHttpRequest = new CMBErrorCodes(400, "InvalidHttpRequest"); // Invalid HTTP request. Reason: <reason>.

    public static final CMBErrorCodes InvalidParameterCombination = new CMBErrorCodes(400, "InvalidParameterCombination"); // Two parameters were specified that cannot be used together, such as Timestamp and Expires.

    public static final CMBErrorCodes InvalidParameterValue = new CMBErrorCodes(400, "InvalidParameterValue"); // One or more parameters cannot be validated.

    public static final CMBErrorCodes InvalidQueryParameter = new CMBErrorCodes(400, "InvalidQueryParameter"); // The query parameter <parameter> is invalid. Please see service documentation for correct syntax.

    public static final CMBErrorCodes InvalidRequest = new CMBErrorCodes(400, "InvalidRequest"); // The service cannot handle the request. Request is invalid.

    public static final CMBErrorCodes InvalidSecurity = new CMBErrorCodes(403, "InvalidSecurity"); // The provided security credentials are not valid. Reason: <reason>.

    public static final CMBErrorCodes InvalidSecurityToken = new CMBErrorCodes(400, "InvalidSecurityToken"); // The security token used in the request is invalid. Reason: <reason>.

    public static final CMBErrorCodes MalformedVersion = new CMBErrorCodes(400, "MalformedVersion"); // Version not well formed: <version>. Must be in YYYY-MM-DD format.

    public static final CMBErrorCodes InvalidMessageContents = new CMBErrorCodes(400, "InvalidMessageContents"); // The message contains characters outside the allowed set.

    public static final CMBErrorCodes MessageTooLong = new CMBErrorCodes(400, "MessageTooLong"); // The message size cannot exceed 64 KB.

    public static final CMBErrorCodes MissingClientTokenId = new CMBErrorCodes(403, "MissingClientTokenId"); // Request must contain AWSAccessKeyId.

    public static final CMBErrorCodes MissingCredentials = new CMBErrorCodes(401, "MissingCredentials"); // Not able to authenticate the request: access credentials are missing.

    public static final CMBErrorCodes MissingParameter = new CMBErrorCodes(400, "MissingParameter"); // A required parameter is missing.

    public static final CMBErrorCodes InvalidAttributeName = new CMBErrorCodes(400, "InvalidAttributeName"); // An attribute name is invalid.

    public static final CMBErrorCodes InvalidAttributeValue = new CMBErrorCodes(400, "InvalidAttributeValue"); // An attribute value is invalid.

    public static final CMBErrorCodes NoSuchVersion = new CMBErrorCodes(400, "NoSuchVersion"); // An incorrect version was specified in the request.

    public static final CMBErrorCodes NotAuthorizedToUseVersion = new CMBErrorCodes(401, "NotAuthorizedToUseVersion"); 
    
    public static final CMBErrorCodes QueueNameExists = new CMBErrorCodes(400, "QueueNameExists"); // The queue name exists
    
    public static final CMBErrorCodes ReadCountOutOfRange = new CMBErrorCodes(400, "ReadCountOutOfRange"); // The value for MaxNumberOfMessages is not valid (must be from 1 to 10).

    public static final CMBErrorCodes RequestExpired = new CMBErrorCodes(400, "RequestExpired"); // The timestamp used with the signature has expired.

    public static final CMBErrorCodes RequestThrottled = new CMBErrorCodes(503, "RequestThrottled"); // Request is throttled.

    public static final CMBErrorCodes ServiceUnavailable = new CMBErrorCodes(503, "ServiceUnavailable"); // A required server needed by CQS is unavailable. This error is often temporary; resend the request after a short wait.

    public static final CMBErrorCodes X509ParseError = new CMBErrorCodes(400, "X509ParseError"); // Could not parse X.509 certificate.
    
    public static final CMBErrorCodes NotFound = new CMBErrorCodes(404, "NotFound"); // The resource was not found.

    public static final CMBErrorCodes InvalidSignature = new CMBErrorCodes(430, "SignatureDoesNotMatch"); // invalid signature

    public static final CMBErrorCodes InvalidSignatureVersion = new CMBErrorCodes(430, "InvalidSignatureVersion"); // invalid signature version

    public static final CMBErrorCodes ValidationError = new CMBErrorCodes(400, "ValidationError"); // The queue name exists

    private final int httpCode;
    private final String cmbCode;

    public CMBErrorCodes(int httpCode, String cmbCode) {
        this.httpCode = httpCode;
        this.cmbCode = cmbCode;
    }
    
    public int getHttpCode() {
        return httpCode;
    }
    
    public String getCMBCode() {
        return cmbCode;
    }
}
