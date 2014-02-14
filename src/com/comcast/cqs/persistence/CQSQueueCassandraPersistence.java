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
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;

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
		queuesTemplateString = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(CMBProperties.getInstance().getWriteConsistencyLevel()), COLUMN_FAMILY_QUEUES, StringSerializer.get(), StringSerializer.get());
		queuesByUserTemplateString = new ThriftColumnFamilyTemplate<String, String>(keyspaces.get(CMBProperties.getInstance().getWriteConsistencyLevel()), COLUMN_FAMILY_QUEUES_BY_USER, StringSerializer.get(), StringSerializer.get());
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
		queueData.put(CQSConstants.COL_WAIT_TIME_SECONDS, (new Long(queue.getReceiveMessageWaitTimeSeconds())).toString());
		queueData.put(CQSConstants.COL_NUMBER_PARTITIONS, (new Long(queue.getNumberOfPartitions())).toString());
		queueData.put(CQSConstants.COL_NUMBER_SHARDS, (new Long(queue.getNumberOfShards())).toString());
		queueData.put(CQSConstants.COL_COMPRESSED, (new Boolean(queue.isCompressed())).toString());
		queueData.put(CQSConstants.COL_ACTIVEACTIVE, (new Boolean(queue.isActiveActive())).toString());

		insertOrUpdateRow(queue.getRelativeUrl(), COLUMN_FAMILY_QUEUES, queueData, CMBProperties.getInstance().getWriteConsistencyLevel());
		
		update(queuesByUserTemplateString, queue.getOwnerUserId(), queue.getArn(), "", StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
	}
	
	@Override
	public void updateQueueAttribute(String queueURL, Map<String, String> queueData) throws PersistenceException {
		insertOrUpdateRow(queueURL, COLUMN_FAMILY_QUEUES, queueData, CMBProperties.getInstance().getWriteConsistencyLevel());
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

	@Override
	public List<CQSQueue> listQueues(String userId, String queueNamePrefix, boolean containingMessagesOnly) throws PersistenceException {
		
		if (userId == null || userId.trim().length() == 0) {
			logger.error("event=list_queues error_code=invalid_user user_id=" + userId);
			throw new PersistenceException(CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
		}
			
		List<CQSQueue> queueList = new ArrayList<CQSQueue>();
		String lastArn = null;
		int counter;

		do {
			
			counter = 0;
			
			ColumnSlice<String, String> slice = readColumnSlice(COLUMN_FAMILY_QUEUES_BY_USER, userId, lastArn, null, 1000, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
			
			if (slice != null) {
				
				boolean first = true;

				for (HColumn<String, String> c : slice.getColumns()) {
					
					counter++;
					
					if (lastArn != null && first) {
						first = false;
						continue;
					}

					first = false;
					lastArn = c.getName();
					CQSQueue queue = new CQSQueue(Util.getNameForArn(lastArn), Util.getQueueOwnerFromArn(lastArn));
					queue.setRelativeUrl(Util.getRelativeQueueUrlForArn(lastArn));
					queue.setArn(lastArn);
					queue.setCreatedTime(c.getClock());
					
					if (queueNamePrefix != null && !queue.getName().startsWith(queueNamePrefix)) {
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
	
	@Override
	public long getNumberOfQueuesByUser(String userId) throws PersistenceException {
		
		if (userId == null || userId.trim().length() == 0) {
			logger.error("event=list_queues error_code=invalid_user user_id=" + userId);
			throw new PersistenceException(CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
		}
			
		String lastArn = null;
		int sliceSize;
		long numQueues = 0;

		do {
			
			sliceSize = 0;
			ColumnSlice<String, String> slice = readColumnSlice(COLUMN_FAMILY_QUEUES_BY_USER, userId, lastArn, null, 10000, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
			
			if (slice != null && slice.getColumns().size() > 0) {
				sliceSize = slice.getColumns().size();
				numQueues += sliceSize;
				lastArn = slice.getColumns().get(sliceSize-1).getName();
			}
			
		} while (sliceSize >= 10000);
		
		return numQueues;
	}

	private CQSQueue fillQueueFromCqlSlice(String url, ColumnSlice<String, String> slice) {
		
		if (slice == null || slice.getColumns() == null || slice.getColumns().size() <= 1) {
			return null;
		}
		
		try {
			String arn = slice.getColumnByName(CQSConstants.COL_ARN).getValue();
			String name = slice.getColumnByName(CQSConstants.COL_NAME).getValue();
			String ownerUserId = slice.getColumnByName(CQSConstants.COL_OWNER_USER_ID).getValue();
			String region = slice.getColumnByName(CQSConstants.COL_REGION).getValue();
			int visibilityTO = (new Long(slice.getColumnByName(CQSConstants.COL_VISIBILITY_TO).getValue())).intValue(); 
			int maxMsgSize = (new Long(slice.getColumnByName(CQSConstants.COL_MAX_MSG_SIZE).getValue())).intValue(); 
			int msgRetentionPeriod = (new Long(slice.getColumnByName(CQSConstants.COL_MSG_RETENTION_PERIOD).getValue())).intValue(); 
			int delaySeconds = slice.getColumnByName(CQSConstants.COL_DELAY_SECONDS) == null ? 0 : (new Long(slice.getColumnByName(CQSConstants.COL_DELAY_SECONDS).getValue())).intValue();
			int waitTimeSeconds = slice.getColumnByName(CQSConstants.COL_WAIT_TIME_SECONDS) == null ? 0 : (new Long(slice.getColumnByName(CQSConstants.COL_WAIT_TIME_SECONDS).getValue())).intValue();
			int numPartitions = slice.getColumnByName(CQSConstants.COL_NUMBER_PARTITIONS) == null ? CMBProperties.getInstance().getCQSNumberOfQueuePartitions() : (new Long(slice.getColumnByName(CQSConstants.COL_NUMBER_PARTITIONS).getValue())).intValue();
			int numShards = slice.getColumnByName(CQSConstants.COL_NUMBER_SHARDS) == null ? 1 : (new Long(slice.getColumnByName(CQSConstants.COL_NUMBER_SHARDS).getValue())).intValue();
			String policy = slice.getColumnByName(CQSConstants.COL_POLICY).getValue();
			long createdTime = (new Long(slice.getColumnByName(CQSConstants.COL_CREATED_TIME).getValue())).longValue();
			String hostName = slice.getColumnByName(CQSConstants.COL_HOST_NAME) == null ? null : slice.getColumnByName(CQSConstants.COL_HOST_NAME).getValue();
			boolean isCompressed = slice.getColumnByName(CQSConstants.COL_COMPRESSED) == null ? false : (new Boolean(slice.getColumnByName(CQSConstants.COL_COMPRESSED).getValue())).booleanValue();
			boolean isActiveActive = slice.getColumnByName(CQSConstants.COL_ACTIVEACTIVE) == null ? false : (new Boolean(slice.getColumnByName(CQSConstants.COL_ACTIVEACTIVE).getValue())).booleanValue();
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
			queue.setReceiveMessageWaitTimeSeconds(waitTimeSeconds);
			queue.setNumberOfPartitions(numPartitions);
			queue.setNumberOfShards(numShards);
			queue.setCreatedTime(createdTime);
			queue.setCompressed(isCompressed);
			queue.setActiveActive(isActiveActive);
			return queue;
		} catch (Exception ex) {
			return null;
		}
	}

	private CQSQueue getQueueByUrl(String queueUrl) {
		ColumnSlice<String, String> slice = readColumnSlice(COLUMN_FAMILY_QUEUES, queueUrl, 15, StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getReadConsistencyLevel());
		if (slice == null) {		    
			return null;
		}
	    CQSQueue queue = fillQueueFromCqlSlice(queueUrl, slice);
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
