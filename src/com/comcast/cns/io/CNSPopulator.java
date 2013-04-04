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
package com.comcast.cns.io;

import com.comcast.cmb.common.model.ReceiptModule;

/**
 * Class to generate API response for receipt id meta-data
 * @author jorge, bwolf
 *
 */
public class CNSPopulator {

	public static String getResponseMetadata() {
		return "\t<ResponseMetadata><RequestId>" + ReceiptModule.getReceiptId() + "</RequestId></ResponseMetadata>\n";
	}
}
