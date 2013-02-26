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

import java.util.List;

import com.comcast.cns.model.CNSTopic;

/**
 * API response generator for Topics
 * @author jorge
 *
 */
public class CNSTopicPopulator {
	
	public static String getCreateTopicResponse(String arn) {
		StringBuffer out = new StringBuffer("<CreateTopicResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n");
		out.append("\t<CreateTopicResult>\n");
		out.append("\t\t<TopicArn>").append(arn).append("</TopicArn>\n");
		out.append("\t</CreateTopicResult>\n");
		out.append("\t").append(CNSPopulator.getResponseMetadata()).append("\n");
		out.append("</CreateTopicResponse>\n");
		return out.toString();
	}
	
	public static String getDeleteTopicResponse() {
		return "<DeleteTopicResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n\n" +	CNSPopulator.getResponseMetadata() + "\n</DeleteTopicResponse>\n";
	}
	
	public static String getListTopicsResponse(List<CNSTopic> topics, String nextToken) {
		
		StringBuffer out = new StringBuffer("<ListTopicsResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n");
		out.append("\t<ListTopicsResult>\n");
		out.append("\t\t<Topics>\n");
		
		for (CNSTopic topic:topics) {		
			out.append("\t\t\t<member><TopicArn>"+topic.getArn()+"</TopicArn></member>\n");
		}
		
		out.append("\t\t</Topics>\n");

		if (nextToken != null) {
			out.append("\t\t<NextToken>" + nextToken + "</NextToken>\n");
		}
		
		out.append("\t</ListTopicsResult>\n");
		out.append("\t").append(CNSPopulator.getResponseMetadata()).append("\n");
		out.append("</ListTopicsResponse>\n");
    	
    	return out.toString();
	}
}
