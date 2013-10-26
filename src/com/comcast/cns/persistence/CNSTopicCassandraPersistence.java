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

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;
import com.comcast.cqs.util.CQSErrorCodes;

import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;

/**
 * Provide Cassandra persistence for topics
 * @author aseem, bwolf, jorge, vvenkatraman, tina, michael
 *
 * Class is immutable
 */
public class CNSTopicCassandraPersistence extends CassandraPersistence implements ICNSTopicPersistence {

	private static Logger logger = Logger.getLogger(CNSTopicCassandraPersistence.class);

	private final ColumnFamilyTemplate<String, String> topicsTemplate;
	private final ColumnFamilyTemplate<String, String> topicsByUserIdTemplate;
	private final ColumnFamilyTemplate<String, String> topicAttributesTemplate;
	private final ColumnFamilyTemplate<String, String> topicStatsTemplate;

	private static final String columnFamilyTopics = "CNSTopics";
	private static final String columnFamilyTopicsByUserId = "CNSTopicsByUserId";
	private static final String columnFamilyTopicAttributes = "CNSTopicAttributes";
	private static final String columnFamilyTopicStats = "CNSTopicStats";

	public CNSTopicCassandraPersistence() {

		super(CMBProperties.getInstance().getCNSKeyspace());

		topicsTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(CMBProperties.getInstance().getWriteConsistencyLevel()), columnFamilyTopics, StringSerializer.get(), StringSerializer.get());
		topicsByUserIdTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(CMBProperties.getInstance().getWriteConsistencyLevel()), columnFamilyTopicsByUserId, StringSerializer.get(), StringSerializer.get());
		topicAttributesTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(CMBProperties.getInstance().getWriteConsistencyLevel()), columnFamilyTopicAttributes, StringSerializer.get(), StringSerializer.get());
		topicStatsTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(CMBProperties.getInstance().getWriteConsistencyLevel()), columnFamilyTopicStats, StringSerializer.get(), StringSerializer.get());
	}

	private Map<String, String> getColumnValues(CNSTopic t) {

		Map<String, String> columnValues = new HashMap<String, String>();

		if (t.getUserId() != null) {
			columnValues.put("userId", t.getUserId());
		}

		if (t.getName() != null) {
			columnValues.put("name", t.getName());
		}

		if (t.getDisplayName() != null) {
			columnValues.put("displayName", t.getDisplayName());
		}

		return columnValues;
	}	

	@Override
	public CNSTopic createTopic(String name, String displayName, String userId) throws Exception {

		String arn = Util.generateCnsTopicArn(name, CMBProperties.getInstance().getRegion(), userId);

		// disable user topic limit for now

		/*List<CNSTopic> topics = listTopics(userId, null);

		if (topics.size() >= Util.CNS_USER_TOPIC_LIMIT) {
			throw new CMBException(CNSErrorCodes.CNS_TopicLimitExceeded, "Topic limit exceeded.");
		}*/

		CNSTopic topic = getTopic(arn);

		if (topic != null) {
			return topic;
		} else {

			topic = new CNSTopic(arn, name, displayName, userId);
			topic.checkIsValid();
			insertOrUpdateRow(topic.getArn(), columnFamilyTopics, getColumnValues(topic), CMBProperties.getInstance().getWriteConsistencyLevel());
			update(topicsByUserIdTemplate, userId, topic.getArn(), "", StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
			delete(topicStatsTemplate, arn, null);
    		deleteCounter(columnFamilyTopicStats, arn, "subscriptionConfirmed", new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
    		deleteCounter(columnFamilyTopicStats, arn, "subscriptionPending", new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
    		deleteCounter(columnFamilyTopicStats, arn, "subscriptionDeleted", new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
    		incrementCounter(columnFamilyTopicStats, arn, "subscriptionConfirmed", 0, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
    		incrementCounter(columnFamilyTopicStats, arn, "subscriptionPending", 0, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
    		incrementCounter(columnFamilyTopicStats, arn, "subscriptionDeleted", 0, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
			CNSTopicAttributes attributes = new CNSTopicAttributes(arn, userId);
			PersistenceFactory.getCNSAttributePersistence().setTopicAttributes(attributes, arn);

			return topic;
		}
	}

	@Override
	public void deleteTopic(String arn) throws Exception {

		CNSTopic topic = getTopic(arn);

		if (topic == null) {
			throw new CMBException(CNSErrorCodes.CNS_NotFound, "Topic not found.");
		}

		// delete all subscriptions first

		PersistenceFactory.getSubscriptionPersistence().unsubscribeAll(topic.getArn());		

		delete(topicsTemplate, arn, null);
		delete(topicsByUserIdTemplate, topic.getUserId(), arn);
		delete(topicAttributesTemplate, arn, null);
		delete(topicStatsTemplate, arn, null);
		deleteCounter(columnFamilyTopicStats, arn, "subscriptionConfirmed", new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
		deleteCounter(columnFamilyTopicStats, arn, "subscriptionPending", new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
		deleteCounter(columnFamilyTopicStats, arn, "subscriptionDeleted", new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
	}
	
	@Override
	public long getNumberOfTopicsByUser(String userId) throws PersistenceException {
		
		if (userId == null || userId.trim().length() == 0) {
			logger.error("event=list_queues error_code=invalid_user user_id=" + userId);
			throw new PersistenceException(CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
		}
			
		String lastArn = null;
		int sliceSize;
		long numTopics = 0;

		do {
			
			sliceSize = 0;
			
			ColumnSlice<String, String> slice = readColumnSlice(columnFamilyTopicsByUserId, userId, lastArn, null, 10000, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
			
			if (slice != null && slice.getColumns().size() > 0) {
				sliceSize = slice.getColumns().size();
				numTopics += sliceSize;
				lastArn = slice.getColumns().get(sliceSize-1).getName();
			}
			
		} while (sliceSize >= 10000);
		
		return numTopics;
	}

	@Override
	public List<CNSTopic> listTopics(String userId, String nextToken) throws Exception {

		if (nextToken != null) {
			if (getTopic(nextToken) == null) {
				nextToken = null;
				//throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Invalid parameter nextToken");
			}
		}

		List<CNSTopic> topics = new ArrayList<CNSTopic>();
		ColumnSlice<String, String> slice = readColumnSlice(columnFamilyTopicsByUserId, userId, nextToken, null, 100, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());

		if (slice != null) {
			
			for (HColumn<String, String> c : slice.getColumns()) {
				
				String arn = c.getName();

				slice = readColumnSlice(columnFamilyTopics, arn, 100, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());

				if (slice != null) {

					String name = slice.getColumnByName("name").getValue();

					String displayName = null;

					if (slice.getColumnByName("displayName") != null) {
						displayName = slice.getColumnByName("displayName").getValue();
					}

					CNSTopic t = new CNSTopic(arn, name, displayName, userId);

					topics.add(t);

				} else {
					delete(topicsByUserIdTemplate, userId, arn);
				}
			}
		}

		return topics;
	}

	@Override
	public List<CNSTopic> listAllTopics(String nextToken) throws Exception {

		if (nextToken != null) {
			if (getTopic(nextToken) == null) {
				nextToken = null;
				//throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Invalid parameter nextToken");
			}
		}

		List<CNSTopic> topics = new ArrayList<CNSTopic>();

		List<Row<String, String, String>> rows = readNextNRows(columnFamilyTopics, nextToken, 100, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());

		for (Row<String, String, String> row : rows) {

			String arn = row.getKey();
			String name = row.getColumnSlice().getColumnByName("name").getValue();

			String displayName = null;

			if (row.getColumnSlice().getColumnByName("displayName") != null) {
				displayName = row.getColumnSlice().getColumnByName("displayName").getValue();
			}

			String user = row.getColumnSlice().getColumnByName("userId").getValue();
			CNSTopic topic = new CNSTopic(arn, name, displayName, user);

			topics.add(topic);

		}

		return topics;
	}

	@Override
	public CNSTopic getTopic(String arn) throws Exception {

		CNSTopic topic = null;
		ColumnSlice<String, String> slice = readColumnSlice(columnFamilyTopics, arn, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());

		if (slice != null) {

			String name = slice.getColumnByName("name").getValue();
			String displayName = null;

			if (slice.getColumnByName("displayName") != null) {
				displayName = slice.getColumnByName("displayName").getValue();
			}

			String user = slice.getColumnByName("userId").getValue();
			topic = new CNSTopic(arn, name, displayName, user);
		}

		return topic;
	}

	@Override
	public void updateTopicDisplayName(String arn, String displayName) throws Exception {

		CNSTopic topic = getTopic(arn);

		if (topic != null) {
			topic.setDisplayName(displayName);
			topic.checkIsValid();
			insertOrUpdateRow(topic.getArn(), columnFamilyTopics, getColumnValues(topic), CMBProperties.getInstance().getWriteConsistencyLevel());
		}
	}
}
