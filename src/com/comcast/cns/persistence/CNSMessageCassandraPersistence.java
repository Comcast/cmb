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
package com.comcast.cns.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSMessage.CNSMessageStructure;

/**
 * Provide Cassandra persistence for Message
 * @author bwolf, vvenkatraman
 *
 * Class is thread-safe
 */
public class CNSMessageCassandraPersistence extends CassandraPersistence {

	public CNSMessageCassandraPersistence() {
		super(CMBProperties.getInstance().getCMBCNSKeyspace());
	}

	protected Map<String, String> getColumnValues(CNSMessage m) {
		
		Map<String, String> columnValues = new HashMap<String, String>();
		
		if (m.getMessage() != null) {
			columnValues.put("message", m.getMessage());
		}
		
		if (m.getSubject() != null) {
			columnValues.put("subject", m.getSubject());
		}
	
		if (m.getTopicArn() != null) {
			columnValues.put("topicArn", m.getTopicArn());
		}
	
		if (m.getMessageStructure() != null) {
			columnValues.put("messageStructure", m.getMessageStructure().toString());
		}
		
		if (m.getUserId() != null) {
			columnValues.put("userId", m.getUserId());
		}
	
		if (m.getMessageId() != null) {
	        columnValues.put("messageId", m.getMessageId());
	    }
	
		return columnValues;
	}

	protected List<CNSMessage> getMessages(SuperSlice<UUID, String, String> superSlice, boolean skipFirst) throws CMBException {
		
		List<CNSMessage> messages = new ArrayList<CNSMessage>();
		
		if (superSlice != null && superSlice.getSuperColumns() != null) {
			
			for (HSuperColumn<UUID, String, String> superColumn: superSlice.getSuperColumns()) {
				
				if (superColumn.getColumns().size() > 0) {
					
					if (skipFirst) {
						skipFirst = false;
						continue;
					}
			
					Map<String, String> columnValues = new HashMap<String, String>();
	
					for (HColumn<String, String> column : superColumn.getColumns()) {
						columnValues.put(column.getName(), column.getValue());
					}
					
					CNSMessage message = new CNSMessage();
					
					if (columnValues.containsKey("message")) {
						message.setMessage(columnValues.get("message"));
					}
					
					if (columnValues.containsKey("topicArn")) {
						message.setTopicArn(columnValues.get("topicArn"));
					}
	
					if (columnValues.containsKey("subject")) {
						message.setSubject(columnValues.get("subject"));
					}
					
					if (columnValues.containsKey("messageStructure")) {
						message.setMessageStructure(CNSMessageStructure.valueOf(columnValues.get("messageStructure")));
					}
					
					if (columnValues.containsKey("userId")) {
						message.setUserId(columnValues.get("userId"));
					}
					
					if (columnValues.containsKey("messageId")) {
					    message.setMessageId(columnValues.get("messageId"));
					}
					
					message.checkIsValid();
	
					messages.add(message);
				}
			}
		}

		return messages;
	}
}