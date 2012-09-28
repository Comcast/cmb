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

import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.query.QueryResult;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
/**
 * Cassandra persistence for queues
 * @author aseem, jorge, bwolf, baosen, vvenkatraman
 *
 */
public class CQSQueueCassandraPersistence extends CassandraPersistence implements ICQSQueuePersistence {
    
	private ColumnFamilyTemplate<String, String> queuesTemplateString;
	
	private static final String COLUMN_FAMILY_QUEUES = "CQSQueues";
	public static final Logger logger = Logger.getLogger(CQSQueueCassandraPersistence.class);

	public CQSQueueCassandraPersistence() {
		super(CMBProperties.getInstance().getCMBCQSKeyspace());
		queuesTemplateString = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(HConsistencyLevel.QUORUM), COLUMN_FAMILY_QUEUES, StringSerializer.get(), StringSerializer.get());
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
	}
	
	@Override
	public void updateQueueAttribute(String queueURL, Map<String, String> queueData) throws PersistenceException {
		insertOrUpdateRow(queueURL, COLUMN_FAMILY_QUEUES, queueData, HConsistencyLevel.QUORUM);
	}

	@Override
	public void deleteQueue(String queueUrl) throws PersistenceException {

		if (getQueueByUrl(queueUrl) == null) {
			String message = "No queue with the url " + queueUrl + " exists";
			logger.error("event=deleteQueue status=failed message=" + message);
			throw new PersistenceException (CQSErrorCodes.InvalidRequest, message);
		}
		
		delete(queuesTemplateString, queueUrl, null);	
	}

	@Override
	public List<CQSQueue> listQueues(String userId, String queueName_prefix) throws PersistenceException {
		
		if (userId == null || userId.trim().length() == 0) {
			logger.error("event=listQueues status=failed message=invalid_user user_id=" + userId);
			throw new PersistenceException(CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
		}
			
		String query = "SELECT * from " + COLUMN_FAMILY_QUEUES + " where " + CQSConstants.COL_OWNER_USER_ID + "='" + userId +"'";
		QueryResult<CqlRows<String,String,String>> res = super.readRows(query, HConsistencyLevel.QUORUM);
		
		if (res == null) {
			return null;
		}
		
		CqlRows<String,String,String> rows = res.get();
		List<CQSQueue> queueList = new ArrayList<CQSQueue>();
		
		if (rows == null || rows.getCount() == 0) {
			return queueList;
		}

		
		for (me.prettyprint.hector.api.beans.Row<String, String, String> row : rows) {
		
			CQSQueue queue = fillQueueFromCqlRow(row);
			
			if (queue != null) {
				if (queueName_prefix != null && !queue.getName().startsWith(queueName_prefix)) {
					continue;
				}
				queueList.add(queue);
			}
		}
		
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
			
			String serviceUrl = CMBProperties.getInstance().getCQSServerUrl();
	        
	        if (serviceUrl != null && serviceUrl.endsWith("/")) {
	        	serviceUrl = serviceUrl.substring(0, serviceUrl.length()-1);
	        }
			
			try {
			
		        // if no queue found try absolute url using current service url for legacy queues
	
		        row = readRow(COLUMN_FAMILY_QUEUES, serviceUrl + "/" + queueUrl, 15, StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
				
				if (row == null) {
					
					// if this doesn't succeed either, go through all queues and look for a matching legacy queue
					
					List<Row<String, String, String>> rows = readNextNNonEmptyRows(COLUMN_FAMILY_QUEUES, null, 1000, 100, StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), HConsistencyLevel.QUORUM);
				
					if (rows != null) {
						
						for (Row<String, String, String> r : rows) {
							
							CQSQueue queue = fillQueueFromCqlRow(r);
							
							if (queue.getRelativeUrl().endsWith(queueUrl) && !queue.getRelativeUrl().startsWith(queueUrl)) {
								row = r;
								break;
							}
						}
					}
				}

			} catch (Exception ex) {
				logger.warn("event=failed_searching_for_legacy_queue url=" + queueUrl, ex);
			}
			
			if (row != null) {

				// if legacy queue found, migrate to new format
				
				logger.warn("event=found_legacy_queue url=" + serviceUrl + "/" + queueUrl);
			    
				CQSQueue queue = fillQueueFromCqlRow(row);

			    try {

			    	delete(queuesTemplateString, row.getKey(), null);
			    
			    	queue.setServiceEndpoint(serviceUrl);
			    	queue.setRelativeUrl(queueUrl);
			    	
					createQueue(queue);
					RedisCachedCassandraPersistence.getInstance().checkCacheConsistency(queue.getRelativeUrl(), false);
					//PersistenceFactory.getCQSMessagePersistence().clearQueue(queue.getRelativeUrl());

					logger.info("event=updated_legacy_queue url=" + serviceUrl + "/" + queueUrl);
				
			    } catch (Exception ex) {
					logger.warn("event=failed_to_update_legacy_queue url=" + serviceUrl + "/" + queueUrl, ex);
				}
			    
			    return queue;
			}
		}

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
