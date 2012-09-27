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
package com.comcast.cmb.test.tools;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

public class AWSCredentialsHolder {
	
	private static AWSCredentials awsCreds;
	
	static {
		
		String cmbCredentialsFileName = null;

		try {
			if (System.getProperty("cmb.awscredentials.propertyFile") != null) {
				cmbCredentialsFileName = System.getProperty("cmb.awscredentials.propertyFile");
			} else if (new File("AwsCredentials.properties").exists()) {
				cmbCredentialsFileName = "AwsCredentials.properties";
			} else {
				throw new IllegalArgumentException("Missing VM parameter cmb.awscredentials.propertyFile");
			}

			Properties props = new Properties();

			props.load(new FileInputStream(cmbCredentialsFileName));
			awsCreds = new BasicAWSCredentials(props.getProperty("accessKey"), props.getProperty("secretKey"));    

		} catch (Exception e) {
			throw new IllegalStateException("Caught Exception while initializing AWSCredentials", e);
		}
	}
	
	public static AWSCredentials initAwsCredentials() throws Exception {		
		return AWSCredentialsHolder.awsCreds;
	}
}

