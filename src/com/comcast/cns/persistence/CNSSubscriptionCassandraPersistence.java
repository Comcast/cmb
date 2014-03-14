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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftSuperCfTemplate;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;

import com.comcast.cmb.common.persistence.AbstractCassandraPersistence;
import com.comcast.cmb.common.persistence.CassandraPersistenceFactory;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.util.Util;
import com.comcast.cqs.model.CQSQueue;

/**
 * 
 * Column-name for CNSTopicSubscriptions table is composite(Endpoint, Protocol). row-key is topicARn
 * @author aseem, bwolf, vvenkatraman, tina, jorge
 * 
 * Class is immutable
 */
public class CNSSubscriptionCassandraPersistence implements ICNSSubscriptionPersistence {
	
    private static Logger logger = Logger.getLogger(CNSSubscriptionCassandraPersistence.class);

	private static final String columnFamilySubscriptions = "CNSTopicSubscriptions";
	private static final String columnFamilySubscriptionsIndex = "CNSTopicSubscriptionsIndex";
	private static final String columnFamilySubscriptionsUserIndex = "CNSTopicSubscriptionsUserIndex";
	private static final String columnFamilySubscriptionsTokenIndex = "CNSTopicSubscriptionsTokenIndex";
	private static final String columnFamilyTopicStats = "CNSTopicStats";
	
	private static final String KEYSPACE = CMBProperties.getInstance().getCNSKeyspace();
	private static final AbstractCassandraPersistence cassandraHandler = CassandraPersistenceFactory.getInstance(KEYSPACE);
	
	public CNSSubscriptionCassandraPersistence() {
	}

	@SuppressWarnings("serial")
    private Map<String, String> getIndexColumnValues(final String endpoint, final CnsSubscriptionProtocol protocol) {
	    return new HashMap<String, String>() {{
	      put(getEndpointAndProtoIndexVal(endpoint, protocol), "");  
	    }};
	}
		
	private Map<String, String> getColumnValues(CNSSubscription s) {
		
		Map<String, String> columnValues = new HashMap<String, String>();
		
		if (s.getEndpoint() != null) {
			columnValues.put("endPoint", s.getEndpoint());
		}
		
		if (s.getToken() != null) {
			columnValues.put("token", s.getToken());
		}
		
		if (s.getArn() != null) {
		    columnValues.put("subArn", s.getArn());
		}
		
		if (s.getUserId() != null) {
			columnValues.put("userId", s.getUserId());
		}
		
		if (s.getConfirmDate() != null) {
			columnValues.put("confirmDate", s.getConfirmDate().getTime() + "");
		}
		
		if (s.getProtocol() != null) {
			columnValues.put("protocol", s.getProtocol().toString());
		}
		
		if (s.getRequestDate() != null) {
			columnValues.put("requestDate", s.getRequestDate().getTime() + "");
		}
		
		columnValues.put("authenticateOnSubscribe", s.isAuthenticateOnUnsubscribe() + "");
		columnValues.put("isConfirmed", s.isConfirmed() + "");
		columnValues.put("rawMessageDelivery", s.getRawMessageDelivery() + "");

		return columnValues;
	}
	
	private static CNSSubscription getSubscriptionFromMap(Map<String, String> map) {
		
	    String subArn = map.get("subArn");
	    
	    CNSSubscription s = new CNSSubscription(subArn);
	    
	    s.setEndpoint(map.get("endPoint"));
	    s.setUserId(map.get("userId"));
	    
	    String confirmDate = map.get("confirmDate");
	    
	    if (confirmDate != null) {
	    	s.setConfirmDate(new Date(Long.parseLong(confirmDate)));
	    }
	    
        String requestDate = map.get("requestDate");
        
        if (requestDate != null) {
        	s.setRequestDate(new Date(Long.parseLong(requestDate)));
        }
        
        String protocol = map.get("protocol");
        
        if (protocol != null) {
        	s.setProtocol(CnsSubscriptionProtocol.valueOf(protocol));
        }
        
        String isConfirmed = map.get("isConfirmed");
        
        if (isConfirmed != null) {
        	s.setConfirmed(Boolean.parseBoolean(isConfirmed));
        }
        
        s.setToken(map.get("token"));
        
        String authenticateOnSubscribe = map.get("authenticateOnSubscribe");
        
        if (authenticateOnSubscribe != null) {
        	s.setAuthenticateOnUnsubscribe(Boolean.parseBoolean(authenticateOnSubscribe));
        }
        
        String rawMessage = map.get("rawMessageDelivery");
        if (rawMessage != null) {
        	s.setRawMessageDelivery(Boolean.parseBoolean(rawMessage));
        }
        
        return s;
	}
	
	private static String getEndpointAndProtoIndexVal(String endpoint, CnsSubscriptionProtocol protocol) {
	    return protocol.name() + ":" + endpoint;
	}
	
	private static String getEndpointAndProtoIndexValEndpoint(String composite) {
        String []arr = composite.split(":");
        if (arr.length < 2) {
            throw new IllegalArgumentException("Bad format for EndpointAndProtocol composite. Must be of the form <protocol>:<endpoint>. Got:" + composite);
        }
        StringBuffer sb = new StringBuffer(arr[1]);
        for (int i = 2; i < arr.length; i++) {
            sb.append(":").append(arr[i]);
        }
        return sb.toString();
	}
	
	private static CnsSubscriptionProtocol getEndpointAndProtoIndexValProtocol(String composite) {
        String []arr = composite.split(":");
        if (arr.length < 2) {
            throw new IllegalArgumentException("Bad format for EndpointAndProtocol composite. Must be of the form <protocol>:<endpoint>. Got:" + composite);
        }
        return CnsSubscriptionProtocol.valueOf(arr[0]);
    }
	
	/**
	 * Single method to update the Subscriptions CF and all index CFs
	 * @param endpoint
	 * @param protocol
	 * @param subscription
	 * @param userId
	 * @param topicArn
	 */	
    @SuppressWarnings("serial")
    private void insertOrUpdateSubsAndIndexes(final CNSSubscription subscription, Integer ttl) throws CMBException {
		
        Composite superColumnName = new Composite(subscription.getEndpoint(), subscription.getProtocol().name());
        
        subscription.checkIsValid();
        
        cassandraHandler.insertSuperColumn(columnFamilySubscriptions, subscription.getTopicArn(), StringSerializer.get(), superColumnName, ttl, 
                new CompositeSerializer(), getColumnValues(subscription), 
                StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getWriteConsistencyLevel());
        
        cassandraHandler.insertRow(subscription.getArn(), columnFamilySubscriptionsIndex, getIndexColumnValues(subscription.getEndpoint(), subscription.getProtocol()), CMBProperties.getInstance().getWriteConsistencyLevel(), ttl);
        cassandraHandler.insertRow(subscription.getUserId(), columnFamilySubscriptionsUserIndex, new HashMap<String, String>() {{ put(subscription.getArn(), "");}}, CMBProperties.getInstance().getWriteConsistencyLevel(), ttl);
        cassandraHandler.insertRow(subscription.getToken(), columnFamilySubscriptionsTokenIndex, new HashMap<String, String>() {{ put(subscription.getArn(), "");}}, CMBProperties.getInstance().getWriteConsistencyLevel(), ttl);
	}
	
	@Override
	public CNSSubscription subscribe(String endpoint, CnsSubscriptionProtocol protocol, String topicArn, String userId) throws Exception {
		
		// subscription is unique by protocol + endpoint + topic

		final CNSSubscription subscription = new CNSSubscription(endpoint, protocol, topicArn, userId);

		CNSTopic t = PersistenceFactory.getTopicPersistence().getTopic(topicArn);
		
		if (t == null) {
			throw new TopicNotFoundException("Resource not found.");
		}
		
		// check if queue exists for cqs endpoints

		if (protocol.equals(CnsSubscriptionProtocol.cqs)) {
			
			CQSQueue queue = PersistenceFactory.getQueuePersistence().getQueue(com.comcast.cqs.util.Util.getRelativeQueueUrlForArn(endpoint));
			
			if (queue == null) {
				throw new CMBException(CMBErrorCodes.NotFound, "Queue with arn " + endpoint + " does not exist.");
			}
		}
		
		subscription.setArn(Util.generateCnsTopicSubscriptionArn(topicArn, protocol, endpoint));
	
		// attempt to delete existing subscription
		
		/*Composite superColumnName = new Composite(subscription.getEndpoint(), subscription.getProtocol().name());
		
		HSuperColumn<Composite, String, String> superCol = readColumnFromSuperColumnFamily(columnFamilySubscriptions, subscription.getTopicArn(), superColumnName, new StringSerializer(), new CompositeSerializer(), StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getReadConsistencyLevel());
		
		if (superCol != null) {
			CNSSubscription exisitingSub = extractSubscriptionFromSuperColumn(superCol, topicArn);
            deleteIndexes(exisitingSub.getArn(), exisitingSub.getUserId(), exisitingSub.getToken());
			deleteSuperColumn(subscriptionsTemplate, exisitingSub.getTopicArn(), superColumnName);
		}*/	
		
		// then set confirmation stuff and update cassandra
		
		CNSSubscription retrievedSubscription = getSubscription(subscription.getArn());
		
		if (!CMBProperties.getInstance().getCNSRequireSubscriptionConfirmation()) {

			subscription.setConfirmed(true);
			subscription.setConfirmDate(new Date());
			
			insertOrUpdateSubsAndIndexes(subscription, null);
			
			if (retrievedSubscription==null) {
				cassandraHandler.incrementCounter(columnFamilyTopicStats, subscription.getTopicArn(), "subscriptionConfirmed", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
			}
			
		} else {
		
			// protocols that cannot confirm subscriptions (e.g. redisPubSub)
			// get an automatic confirmation here
			if (! protocol.canConfirmSubscription()) {
				subscription.setConfirmed(true);
				subscription.setConfirmDate(new Date());
				insertOrUpdateSubsAndIndexes(subscription, null);
				
				// auto confirm subscription to cqs queue by owner
			} else if (protocol.equals(CnsSubscriptionProtocol.cqs)) {
			
				String queueOwner = com.comcast.cqs.util.Util.getQueueOwnerFromArn(endpoint);
				
				if (queueOwner != null && queueOwner.equals(userId)) {
					
					subscription.setConfirmed(true);
					subscription.setConfirmDate(new Date());
					
		            insertOrUpdateSubsAndIndexes(subscription, null);
		            if (retrievedSubscription==null) {
		            	cassandraHandler.incrementCounter(columnFamilyTopicStats, subscription.getTopicArn(), "subscriptionConfirmed", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
		            }
				} else {

                    // use cassandra ttl to implement expiration after 3 days 
		            insertOrUpdateSubsAndIndexes(subscription, 3*24*60*60);
		            if (retrievedSubscription==null) {
		            	cassandraHandler.incrementCounter(columnFamilyTopicStats, subscription.getTopicArn(), "subscriptionPending", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
		            }
				}
			
			} else {
				
				// use cassandra ttl to implement expiration after 3 days 
			    insertOrUpdateSubsAndIndexes(subscription, 3*24*60*60);			    
			    if (retrievedSubscription==null) {
			    	cassandraHandler.incrementCounter(columnFamilyTopicStats, subscription.getTopicArn(), "subscriptionPending", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
			    }
			}
		}

		CNSSubscriptionAttributes attributes = new CNSSubscriptionAttributes(topicArn, subscription.getArn(), userId);
		PersistenceFactory.getCNSAttributePersistence().setSubscriptionAttributes(attributes, subscription.getArn());

		return subscription;
	}

	@Override
	public CNSSubscription getSubscription(String arn) throws Exception {
		
	    //read form index to get composite col-name
	    ColumnSlice<String, String> slice = cassandraHandler.readColumnSlice(columnFamilySubscriptionsIndex, arn, 1, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());	 
	    		
		if (slice != null) {
		    //get Column from main table
		    String colName = slice.getColumns().get(0).getName();
		    CnsSubscriptionProtocol protocol = getEndpointAndProtoIndexValProtocol(colName);
		    String endpoint = getEndpointAndProtoIndexValEndpoint(colName);
		    
		    Composite superColumnName = new Composite(endpoint, protocol.name());
		    
		    HSuperColumn<Composite, String, String> superCol = cassandraHandler.readColumnFromSuperColumnFamily(columnFamilySubscriptions, Util.getCnsTopicArn(arn), superColumnName, StringSerializer.get(), new CompositeSerializer(), StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getReadConsistencyLevel());
		    
		    if (superCol != null) {
		        CNSSubscription s = extractSubscriptionFromSuperColumn(superCol, Util.getCnsTopicArn(arn));
		        s.checkIsValid();
	            return s;
		    }		    
		}
		
		return null;
	}

	private static CNSSubscription extractSubscriptionFromSuperColumn(HSuperColumn<Composite, String, String> superCol, String topicArn) {
	    
	    Map<String, String> messageMap = new HashMap<String, String>(superCol.getColumns().size());
	    
        for (HColumn<String, String> column : superCol.getColumns()) {
            messageMap.put(column.getName(), column.getValue());
        }
        
        CNSSubscription sub = getSubscriptionFromMap(messageMap);
        sub.setTopicArn(topicArn);
        
        return sub;
	}

    @Override
    /**
     * List all subscription for a user, unconfirmed subscriptions will not reveal their arns. Pagination for more than 100 subscriptions.
     * @param nextToken initially null, on subsequent calls arn of last result from prior call
     * @param protocol optional filter by protocol (this parameter is not part of official AWS API)
     * @return list of subscriptions. If nextToken is not null, the subscription corresponding to it is not returned.
     * @throws Exception
     */
	public List<CNSSubscription> listSubscriptions(String nextToken, CnsSubscriptionProtocol protocol, String userId) throws Exception {
        return listSubscriptions(nextToken, protocol, userId, true);
    }
    
    private List<CNSSubscription> listSubscriptions(String nextToken, CnsSubscriptionProtocol protocol, String userId, boolean hidePendingArn) throws Exception {
		
        if (nextToken != null) {
            if (getSubscription(nextToken) == null) {
                throw new SubscriberNotFoundException("Subscriber not found for arn " + nextToken);
            }
        }  
        
        //Algorithm is to keep reading in chunks of 100 till we've seen 500 from the nextToken-ARN         
		List<CNSSubscription> l = new ArrayList<CNSSubscription>();
		//read form index to get sub-arn        
		
        while (l.size() < 100) {
        	
            ColumnSlice<String, String> slice = cassandraHandler.readColumnSlice(columnFamilySubscriptionsUserIndex, userId, nextToken, null, 500, StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getReadConsistencyLevel());     

            if (slice == null) {
                return l;
            }
            
            //get Column from main table
            List<HColumn<String, String>> cols = slice.getColumns();
            
            if (nextToken != null) {
                cols.remove(0);
            }
            
            if (cols.size() == 0) {
            	return l;
            }
            
            for (HColumn<String, String> col : cols) {
            	
                String subArn = col.getName();
                CNSSubscription subscription = getSubscription(subArn);
                
                if (subscription == null) {
                    throw new IllegalStateException("Subscriptions-user-index contains subscription-arn which doesn't exist in subscriptions-index. subArn:" + subArn);
                }
                
                // ignore invalid subscriptions coming from Cassandra
                
                try {
                	subscription.checkIsValid();
                } catch (CMBException ex) {
                	logger.error("event=invalid_subscription " + subscription.toString(), ex);
                	continue;
                }
                
                if (protocol != null && subscription.getProtocol() != protocol) {
                	continue;
                }
                
                if (hidePendingArn) {
                    if (subscription.isConfirmed()) {
                        l.add(subscription);
                    }  else {
                        subscription.setArn("PendingConfirmation");
                        l.add(subscription);
                    }
                } else {
                    l.add(subscription);
                }
                
                if (l.size() == 100) {
                    return l;
                }
            }

            nextToken = cols.get(cols.size() - 1).getName();
        }
		
		return l;
	}

	@Override
	public List<CNSSubscription> listAllSubscriptions(String nextToken, CnsSubscriptionProtocol protocol, String userId) throws Exception {
	    return listSubscriptions(nextToken, protocol, userId, false);
	}

	@Override
	public List<CNSSubscription> listSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol) throws Exception {
		return listSubscriptionsByTopic(nextToken, topicArn, protocol, 100);
	}
	
	@Override
	public List<CNSSubscription> listSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol, int pageSize) throws Exception {
	    return listSubscriptionsByTopic(nextToken, topicArn, protocol, pageSize, true);
	}
	/**
	 * Enumerate all subs in a topic
	 * @param nextToken The ARN of the last sub-returned or null is first time call.
	 * @param topicArn
	 * @param protocol
	 * @param pageSize
	 * @param hidePendingArn
	 * @return The list of subscriptions given a topic. Note: if nextToken is provided, the returned list will not contain it for convenience
	 * @throws Exception
	 */
	public List<CNSSubscription> listSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol, int pageSize, boolean hidePendingArn) throws Exception {

	    if (nextToken != null) {
            if (getSubscription(nextToken) == null) {
                throw new SubscriberNotFoundException("Subscriber not found for arn " + nextToken);
            }
        }
	    
		//read from index to get composite-col-name corresponding to nextToken
		Composite nextTokenComposite = null;
		
		if (nextToken != null) {
		    ColumnSlice<String, String> slice = cassandraHandler.readColumnSlice(columnFamilySubscriptionsIndex, nextToken, 1, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		    if (slice == null) {
		    	throw new IllegalArgumentException("Could not find any subscription with arn " + nextToken);
		    }
		    //get Column from main table
		    String colName = slice.getColumns().get(0).getName();
		    CnsSubscriptionProtocol tokProtocol = getEndpointAndProtoIndexValProtocol(colName);
		    String endpoint = getEndpointAndProtoIndexValEndpoint(colName);
		    nextTokenComposite = new Composite(endpoint, tokProtocol.name());
		}

		List<CNSSubscription> l = new ArrayList<CNSSubscription>();
		
		CNSTopic t = PersistenceFactory.getTopicPersistence().getTopic(topicArn);
		
		if (t == null) {
			throw new TopicNotFoundException("Resource not found.");
		}
		
		//Read pageSize at a time
		List<HSuperColumn<Composite, String, String>> cols = cassandraHandler.readColumnsFromSuperColumnFamily(columnFamilySubscriptions, topicArn, StringSerializer.get(), new CompositeSerializer(), StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getReadConsistencyLevel(), nextTokenComposite, null, pageSize);
		
		if (nextToken != null && cols.size() > 0) {
		    cols.remove(0);
		}
		
		while (l.size() < pageSize) {
			
		    if (cols == null || cols.size() == 0) {
		        return l;
		    }
		    
		    for (HSuperColumn<Composite, String, String> col : cols) {
		    	
		        CNSSubscription sub = extractSubscriptionFromSuperColumn(col, topicArn);
		        
                // ignore invalid subscriptions coming from Cassandra
                
                try {
                	sub.checkIsValid();
                } catch (CMBException ex) {
                	logger.error("event=invalid_subscription " + sub.toString(), ex);
                	continue;
                }
		        
		        if (protocol != null && protocol != sub.getProtocol()) {
		        	continue;
		        }
		        
		        if (hidePendingArn) {
		            if (sub.isConfirmed()) {
		                l.add(sub);
		            } else {
		                sub.setArn("PendingConfirmation");
		                l.add(sub);
		            }
		        } else {
		            l.add(sub);
		        }
		        
		        if (l.size() == pageSize) {
		            return l;
		        }
		    }
		    
		    nextTokenComposite = cols.get(cols.size() - 1).getName();
		    cols = cassandraHandler.readColumnsFromSuperColumnFamily(columnFamilySubscriptions, topicArn, StringSerializer.get(), new CompositeSerializer(), StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getReadConsistencyLevel(), nextTokenComposite, null, pageSize);
		    
		    if (cols.size() > 0) {
		        cols.remove(0);
		    }
		}
		
		return l;
	}

	@Override
	public List<CNSSubscription> listAllSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol) throws Exception {
	      return listSubscriptionsByTopic(nextToken, topicArn, protocol, 100, false);
	}

	@Override
	public CNSSubscription confirmSubscription(boolean authenticateOnUnsubscribe, String token, String topicArn) throws Exception {
		
	    //get Sub-arn given token
	    ColumnSlice<String, String> slice = cassandraHandler.readColumnSlice(columnFamilySubscriptionsTokenIndex, token, 1, StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getReadConsistencyLevel());
	    
	    if (slice == null) {
	        throw new CMBException(CMBErrorCodes.NotFound, "Resource not found.");
	    }
	    
	    //get Column from main table
	    String subArn = slice.getColumns().get(0).getName();
	    
	    //get Subscription given subArn
		final CNSSubscription s = getSubscription(subArn);	
		
		if (s == null) {
			throw new SubscriberNotFoundException("Could not find subscription given subscription arn " + subArn);
		}
		
		s.setAuthenticateOnUnsubscribe(authenticateOnUnsubscribe);
        s.setConfirmed(true);
        s.setConfirmDate(new Date());
        
        //re-insert with no TTL. will clobber the old one which had ttl
        insertOrUpdateSubsAndIndexes(s, null);    
        
        cassandraHandler.decrementCounter(columnFamilyTopicStats, s.getTopicArn(), "subscriptionPending", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
        cassandraHandler.incrementCounter(columnFamilyTopicStats, s.getTopicArn(), "subscriptionConfirmed", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
        
        return s;
	}

	private void deleteIndexesAll(List <CNSSubscription> subscriptionList) {
		List <String> subArnList=new LinkedList<String>();
		List <String> userIdList=new LinkedList<String>();
		List <String> tokenList=new LinkedList<String>();
		for(CNSSubscription sub:subscriptionList){
			subArnList.add(sub.getArn());
			userIdList.add(sub.getUserId());
			tokenList.add(sub.getToken());
		}
		cassandraHandler.deleteBatch(columnFamilySubscriptionsIndex, subArnList, null, StringSerializer.get(),CMBProperties.getInstance().getWriteConsistencyLevel(), StringSerializer.get()); //delete from the index cf
		cassandraHandler.deleteBatch(columnFamilySubscriptionsUserIndex, userIdList, subArnList, StringSerializer.get(), CMBProperties.getInstance().getWriteConsistencyLevel(), StringSerializer.get()); //delete from the index cf
		cassandraHandler.deleteBatch(columnFamilySubscriptionsTokenIndex, tokenList, subArnList, StringSerializer.get(), CMBProperties.getInstance().getWriteConsistencyLevel(), StringSerializer.get()); //delete from the index cf
 	}
	
	private void deleteIndexes(String subArn, String userId, String token) {
		cassandraHandler.delete(columnFamilySubscriptionsIndex, subArn, null, StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getWriteConsistencyLevel()); //delete from the index cf
		cassandraHandler.delete(columnFamilySubscriptionsUserIndex, userId, subArn, StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getWriteConsistencyLevel());
		cassandraHandler.delete(columnFamilySubscriptionsTokenIndex, token, subArn, StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getWriteConsistencyLevel());
	}
	
	@Override
	public void unsubscribe(String arn) throws Exception {

		CNSSubscription s = getSubscription(arn);
		
		if (s != null) {

			deleteIndexes(arn, s.getUserId(), s.getToken());
			Composite superColumnName = new Composite(s.getEndpoint(), s.getProtocol().name());
			cassandraHandler.deleteSuperColumn(columnFamilySubscriptions, Util.getCnsTopicArn(arn), superColumnName, new StringSerializer(), new CompositeSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());

			if (s.isConfirmed()) {
				cassandraHandler.decrementCounter(columnFamilyTopicStats, s.getTopicArn(), "subscriptionConfirmed", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
			} else {
				cassandraHandler.decrementCounter(columnFamilyTopicStats, s.getTopicArn(), "subscriptionPending", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
			}
			
			cassandraHandler.incrementCounter(columnFamilyTopicStats, s.getTopicArn(), "subscriptionDeleted", 1, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
		}
	}
	
	public long getCountSubscription(String topicArn, String columnName) throws Exception {
		return cassandraHandler.getCounter(columnFamilyTopicStats, topicArn, columnName, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
	}

    @Override
    public void unsubscribeAll(String topicArn) throws Exception {
    	
    	int pageSize=1000;
    	
		String nextToken = null;
		List<CNSSubscription> subs = listSubscriptionsByTopic(nextToken, topicArn, null, pageSize, false);

		//Note: for pagination to work we need the nextToken's corresponding sub to not be deleted.
		CNSSubscription nextTokenSub=null;
		while (subs.size() > 0) {
			//if retrieve subscription is less than page size, delete all index. 
			if(subs.size()<pageSize){
				deleteIndexesAll(subs);
				break;
			}
			else{
				//keep the last subscription for pagination purpose.
				nextTokenSub = subs.get(subs.size() - 1);
				nextToken = nextTokenSub.getArn();
				subs.remove(subs.size() - 1);
				deleteIndexesAll(subs);
				subs = listSubscriptionsByTopic(nextToken, topicArn, null, pageSize, false);	
				deleteIndexes(nextTokenSub.getArn(), nextTokenSub.getUserId(), nextTokenSub.getToken());
			}
		}
		//set counter;
		int subscriptionConfirmedNum=(int)cassandraHandler.getCounter(columnFamilyTopicStats, topicArn, "subscriptionConfirmed", StringSerializer.get(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		int subscriptionPendingNum=(int)cassandraHandler.getCounter(columnFamilyTopicStats, topicArn, "subscriptionPending", StringSerializer.get(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
		cassandraHandler.incrementCounter(columnFamilyTopicStats, topicArn, "subscriptionDeleted", subscriptionConfirmedNum+subscriptionPendingNum, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());			
		cassandraHandler.decrementCounter(columnFamilyTopicStats, topicArn, "subscriptionConfirmed", subscriptionConfirmedNum, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
		cassandraHandler.decrementCounter(columnFamilyTopicStats, topicArn, "subscriptionPending", subscriptionPendingNum, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());

		// delete the entire row from Subscriptions cf    

		//cassandraHandler.deleteSuperColumn(columnFamilySubscriptions, topicArn, null, new StringSerializer(), new CompositeSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
		
		cassandraHandler.delete(columnFamilySubscriptions, topicArn, null, new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getWriteConsistencyLevel());
    }

	@Override
	public void setRawMessageDelivery(String subscriptionArn, boolean rawMessageDelivery) throws Exception{
		CNSSubscription sub;
		sub = getSubscription(subscriptionArn);
		if (sub != null) {
			sub.setRawMessageDelivery(rawMessageDelivery);
			insertOrUpdateSubsAndIndexes(sub, null);
		}
	}
}
