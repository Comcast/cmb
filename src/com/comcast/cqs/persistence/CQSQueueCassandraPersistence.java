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
package com.comcast.cqs.persistence;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
import com.comcast.cqs.util.Util;
/**
 * Cassandra persistence for queues
 * @author aseem, jorge, bwolf, baosen, vvenkatraman
 *
 */
public class CQSQueueCassandraPersistence extends CassandraPersistence implements ICQSQueuePersistence {
    
	private ColumnFamilyTemplate<String, String> queuesTemplateString;
	private ColumnFamilyTemplate<String, String> queuesByUserTemplateString;
	
	private static final String COLUMN_FAMILY_QUEUES = "CQSQueues";
	private static final String COLUMN_FAMILY_QUEUES_BY_USER = "CQSQueuesByUserId";

	public static final Logger logger = Logger.getLogger(CQSQueueCassandraPersistence.class);

	public CQSQueueCassandraPersistence() {
		super(CMBProperties.getInstance().getCQSKeyspace());
		queuesTemplateString = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(HConsistencyLevel.QUORUM), COLUMN_FAMILY_QUEUES, StringSerializer.get(), StringSerializer.get());
		queuesByUserTemplateString = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(HConsistencyLevel.QUORUM), COLUMN_FAMILY_QUEUES_BY_USER, StringSerializer.get(), StringSerializer.get());
	}

	@Override
	public void createQueue(CQSQueue queue) throws PersistenceException {
		
		long createdTime = Calendar.getInstance().getTimeInMillis();
		
		Map<String, String> queueData = new HashMap<String, String>();
		
		queueData.put(CQSConstants.COL_ARN, queue.getArn());
		queueData.put(CQSConstants.COL_NAME, queue.getName());
		queueData.put(CQSConstants.COL_OWNER_USER_ID, queue.getOwnerUserId());
		queueData.put(CQSConstants.COL_REGION, queue.getRegion());
		queueData.put(CQSConstants.COL_HOST_NAME, queue.getServiceEndpoint());
		queueData.put(CQSConstants.COL_VISIBILITY_TO, (new Long(queue.getVisibilityTO())).toString());
		queueData.put(CQSConstants.COL_MAX_MSG_SIZE, (new Long(queue.getMaxMsgSize())).toString());
		queueData.put(CQSConstants.COL_MSG_RETENTION_PERIOD, (new Long(queue.getMsgRetentionPeriod())).toString());
		queueData.put(CQSConstants.COL_DELAY_SECONDS, (new Long(queue.getDelaySeconds())).toString());
		queueData.put(CQSConstants.COL_POLICY, queue.getPolicy()!=null?queue.getPolicy():"");
		queueData.put(CQSConstants.COL_CREATED_TIME, (new Long(createdTime)).toString());

		insertOrUpdateRow(queue.getRelativeUrl(), COLUMN_FAMILY_QUEUES, queueData, HConsistencyLevel.QUORUM);
		
		update(queuesByUserTemplateString, queue.getOwnerUserId(), queue.getArn(), "", StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
	}
	
	@Override
	public void updateQueueAttribute(String queueURL, Map<String, String> queueData) throws PersistenceException {
		insertOrUpdateRow(queueURL, COLUMN_FAMILY_QUEUES, queueData, HConsistencyLevel.QUORUM);
	}

	@Override
	public void deleteQueue(String queueUrl) throws PersistenceException {

		if (getQueueByUrl(queueUrl) == null) {
			logger.error("event=delete_queue error_code=queue_does_not_exist queue_url=" + queueUrl);
			throw new PersistenceException (CQSErrorCodes.InvalidRequest, "No queue with the url " + queueUrl + " exists");
		}
		
		delete(queuesTemplateString, queueUrl, null);
		delete(queuesByUserTemplateString, Util.getUserIdForRelativeQueueUrl(queueUrl), Util.getArnForRelativeQueueUrl(queueUrl));
	}

	private int visitNextSetOfQueues(List<CQSQueue> queueList, String userId, String queueName_prefix, boolean containingMessagesOnly) {
		
		String firstColumn = null;
		int counter = 0;
		
		if (queueList.size() > 0) {
			firstColumn = queueList.get(queueList.size()-1).getArn();
		}
		
		Row<String, String, String> row = readRow(COLUMN_FAMILY_QUEUES_BY_USER, userId, firstColumn, null, 1000, new StringSerializer(), new StringSerializer(), new StringSerializer(), HConsistencyLevel.QUORUM);
		
		if (row != null) {
			
			boolean first = true;

			for (HColumn<String, String> c : row.getColumnSlice().getColumns()) {
				
				counter++;
				
				if (firstColumn != null && first) {
					first = false;
					continue;
				}

				String arn = c.getName();
				CQSQueue queue = new CQSQueue(Util.getNameForArn(arn), Util.getQueueOwnerFromArn(arn));
				queue.setRelativeUrl(Util.getRelativeQueueUrlForArn(arn));
				queue.setArn(arn);
				
				if (queueName_prefix != null && !queue.getName().startsWith(queueName_prefix)) {
					continue;
				}
				
				if (containingMessagesOnly) {
					try {
						if (RedisCachedCassandraPersistence.getInstance().getQueueMessageCount(queue.getRelativeUrl(), true) <= 0) {
							continue;
						}
					} catch (Exception ex) { }
				} 
				
				queueList.add(queue);
				
				if (queueList.size() >= 1000) {
					return counter;
				}
			}
		}
		
		return counter;
	}
	
	@Override
	public List<CQSQueue> listQueues(String userId, String queueName_prefix, boolean containingMessagesOnly) throws PersistenceException {
		
		if (userId == null || userId.trim().length() == 0) {
			logger.error("event=list_queues error_code=invalid_user user_id=" + userId);
			throw new PersistenceException(CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
		}
			
		List<CQSQueue> queueList = new ArrayList<CQSQueue>();
		String lastArn = null;
		int counter;

		do {
			
			counter = 0;
			
			Row<String, String, String> row = readRow(COLUMN_FAMILY_QUEUES_BY_USER, userId, lastArn, null, 1000, new StringSerializer(), new StringSerializer(), new StringSerializer(), HConsistencyLevel.QUORUM);
			
			if (row != null) {
				
				boolean first = true;

				for (HColumn<String, String> c : row.getColumnSlice().getColumns()) {
					
					counter++;
					
					if (lastArn != null && first) {
						continue;
					}

					first = false;
					lastArn = c.getName();
					CQSQueue queue = new CQSQueue(Util.getNameForArn(lastArn), Util.getQueueOwnerFromArn(lastArn));
					queue.setRelativeUrl(Util.getRelativeQueueUrlForArn(lastArn));
					queue.setArn(lastArn);
					
					if (queueName_prefix != null && !queue.getName().startsWith(queueName_prefix)) {
						continue;
					}
					
					if (containingMessagesOnly) {
						try {
							if (RedisCachedCassandraPersistence.getInstance().getQueueMessageCount(queue.getRelativeUrl(), true) <= 0) {
								continue;
							}
						} catch (Exception ex) {
							continue;
						}
					} 
					
					queueList.add(queue);
					
					if (queueList.size() >= 1000) {
						return queueList;
					}
				}
			}
		} while (counter >= 1000);
		
		return queueList;
	}
	
	private CQSQueue fillQueueFromCqlRow(me.prettyprint.hector.api.beans.Row<String, String, String> row) {
		
		String url = row.getKey();
		ColumnSlice<String, String> columnSlice = row.getColumnSlice();
		
		if (columnSlice == null || columnSlice.getColumns() == null || columnSlice.getColumns().size() <= 1) {
			return null;
		}
		
		try {
			String arn = columnSlice.getColumnByName(CQSConstants.COL_ARN).getValue();
			String name = columnSlice.getColumnByName(CQSConstants.COL_NAME).getValue();
			String ownerUserId = columnSlice.getColumnByName(CQSConstants.COL_OWNER_USER_ID).getValue();
			String region = columnSlice.getColumnByName(CQSConstants.COL_REGION).getValue();
			int visibilityTO = (new Long(columnSlice.getColumnByName(CQSConstants.COL_VISIBILITY_TO).getValue())).intValue(); 
			int maxMsgSize = (new Long(columnSlice.getColumnByName(CQSConstants.COL_MAX_MSG_SIZE).getValue())).intValue(); 
			int msgRetentionPeriod = (new Long(columnSlice.getColumnByName(CQSConstants.COL_MSG_RETENTION_PERIOD).getValue())).intValue(); 
			int delaySeconds = (new Long(columnSlice.getColumnByName(CQSConstants.COL_DELAY_SECONDS).getValue())).intValue();
			String policy = columnSlice.getColumnByName(CQSConstants.COL_POLICY).getValue();
			long createdTime = (new Long(columnSlice.getColumnByName(CQSConstants.COL_CREATED_TIME).getValue())).longValue();
			String hostName = columnSlice.getColumnByName(CQSConstants.COL_HOST_NAME) == null ? null : columnSlice.getColumnByName(CQSConstants.COL_HOST_NAME).getValue();
			CQSQueue queue = new CQSQueue(name, ownerUserId);
			queue.setRelativeUrl(url);
			queue.setServiceEndpoint(hostName);
			queue.setArn(arn);
            queue.setRegion(region);
			queue.setPolicy(policy);
			queue.setVisibilityTO(visibilityTO);
			queue.setMaxMsgSize(maxMsgSize);
			queue.setMsgRetentionPeriod(msgRetentionPeriod);
			queue.setDelaySeconds(delaySeconds);
			queue.setCreatedTime(createdTime);
			return queue;
		} catch (Exception ex) {
			return null;
		}
	}

	private CQSQueue getQueueByUrl(String queueUrl) {

		Row<String, String, String> row = readRow(COLUMN_FAMILY_QUEUES, queueUrl, 15, StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);

		if (row == null) {		    
			return null;
		}
		
	    CQSQueue queue = fillQueueFromCqlRow(row);
	    
	    return queue;
	}

	@Override
	public CQSQueue getQueue(String userId, String queueName) {
		CQSQueue queue = new CQSQueue(queueName, userId);
		return getQueueByUrl(queue.getRelativeUrl());
	}

	@Override
	public CQSQueue getQueue(String queueUrl) {
		return getQueueByUrl(queueUrl);
	}

	@Override
	public boolean updatePolicy(String queueUrl, String policy) {
		
		if (queueUrl == null || queueUrl.trim().isEmpty() || policy == null || policy.trim().isEmpty()) {
			return false;
		}
		
		update(queuesTemplateString, queueUrl, CQSConstants.COL_POLICY, policy, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		return true;
	}
}
