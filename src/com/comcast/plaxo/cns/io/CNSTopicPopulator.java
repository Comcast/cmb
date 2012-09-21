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
package com.comcast.plaxo.cns.io;

import java.util.List;
import com.comcast.plaxo.cns.model.CNSTopic;

/**
 * API response generator for Topics
 * @author jorge
 *
 */
public class CNSTopicPopulator {
	
	private static String printTopic(String arn) {
	    	String res ="<member><TopicArn>"+arn+"</TopicArn></member>";
	    	return res;
	    }
	
	public static String getCreateTopicResponse(String arn) {
		String res = "<CreateTopicResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" +
				 "<CreateTopicResult>" +
				 "<TopicArn>"+arn+"</TopicArn>" +
				 "</CreateTopicResult>" +
				 CNSPopulator.getResponseMetadata() +
				 "</CreateTopicResponse>";
		return res;
	}
	
	public static String getDeleteTopicResponse() {
		String res = "<DeleteTopicResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" +
				CNSPopulator.getResponseMetadata() +
				 "</DeleteTopicResponse>";
		return res;
	}
	
	public static String getListTopicsResponse(List<CNSTopic> topics, String nextToken) {
		String res = "<ListTopicsResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">" +
				"<ListTopicsResult>" +
				"<Topics>";
		for(CNSTopic topic:topics) {		
			res += printTopic(topic.getArn());
		}
		if(nextToken != null) {
			res += "<NextToken>" + nextToken + "</NextToken>";
		}
    	res += "</Topics>" +
		       "</ListTopicsResult>" +
		       CNSPopulator.getResponseMetadata() +
				 "</ListTopicsResponse>";
    	return res;
	}
}
