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
package com.comcast.cmb.common.persistence;

import com.comcast.cmb.common.model.IPermission;
import java.util.List;

/**
 * Represents the persistence of a Permission
 * @author michael, baosen
 */
public interface IPermissionPersistence {
	
    /**
     * Return a list of permissions for that user
     * @param userId
     * @return
     */
    public List<IPermission> getPermissionsByUser(String userId);

    /**
     * Return a list of permissions given a topic ARN or a queue ARN
     * @param arn
     * @return
     */
    public List<IPermission> getPermissionsByARN(String arn);

    /**
     *
     * @param userId
     * @param action
     * @param arn
     * @return
     */
    public boolean isUserAuthorized(String userId, String action, String arn);
}
