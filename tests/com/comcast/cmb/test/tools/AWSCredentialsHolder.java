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

