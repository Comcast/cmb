package com.comcast.cmb.common.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;

public abstract class AbstractCassandraPersistence {
	
	protected static Random rand = new Random();
	
	public static final String CMB_KEYSPACE = CMBProperties.getInstance().getCMBKeyspace();
	public static final String CQS_KEYSPACE = CMBProperties.getInstance().getCQSKeyspace();
	public static final String CNS_KEYSPACE = CMBProperties.getInstance().getCNSKeyspace();
    
	public static class CMB_SERIALIZER { 
		public static final CmbStringSerializer STRING_SERIALIZER = new CmbStringSerializer();
		public static final CmbCompositeSerializer COMPOSITE_SERIALIZER = new CmbCompositeSerializer();
		public static final CmbLongSerializer LONG_SERIALIZER = new CmbLongSerializer();
	};
	
	public static final String CNS_TOPICS = "CNSTopics";
	public static final String CNS_TOPICS_BY_USER_ID = "CNSTopicsByUserId";
	public static final String CNS_TOPIC_SUBSCRIPTIONS = "CNSTopicSubscriptions";
	public static final String CNS_TOPIC_SUBSCRIPTIONS_INDEX = "CNSTopicSubscriptionsIndex";
	public static final String CNS_TOPIC_SUBSCRIPTIONS_USER_INDEX = "CNSTopicSubscriptionsUserIndex";
	public static final String CNS_TOPIC_SUBSCRIPTIONS_TOKEN_INDEX = "CNSTopicSubscriptionsTokenIndex";
	public static final String CNS_TOPIC_ATTRIBUTES = "CNSTopicAttributes";
	public static final String CNS_SUBSCRIPTION_ATTRIBUTES = "CNSSubscriptionAttributes";
	public static final String CNS_TOPIC_STATS = "CNSTopicStats";
	public static final String CNS_WORKERS = "CNSWorkers";
	public static final String CNS_API_SERVERS = "CNSAPIServers";
	
	public static final String CQS_QUEUES = "CQSQueues";
	public static final String CQS_QUEUES_BY_USER_ID = "CQSQueuesByUserId";
	public static final String CQS_PARTITIONED_QUEUE_MESSAGES = "CQSPartitionedQueueMessages";
	public static final String CQS_API_SERVERS = "CQSAPIServers";
	
	public static final String CMB_USERS = "Users";
	
	public static abstract class CmbSerializer {
	}
	
	public static class CmbStringSerializer extends CmbSerializer {
	}

	public static class CmbCompositeSerializer extends CmbSerializer {
	}
	
	public static class CmbLongSerializer extends CmbSerializer {
	}

	public static abstract class CmbComposite {
		public CmbComposite() {
		}
		public CmbComposite(List<?> l) {
		}
		public CmbComposite(Object... os) {
		}
		public abstract Object get(int i);
		public abstract int compareTo(CmbComposite c);
	}

	public static abstract class CmbColumn<N, V> {
		public CmbColumn() {
		}
		public abstract N getName();
		public abstract V getValue();
		public abstract long getClock();
	}
	
	public static abstract class CmbRow<K, N, V> {
		public CmbRow() {
		}
		public abstract K getKey();
		public abstract CmbColumnSlice<N, V> getColumnSlice();
	}
	
	public static abstract class CmbColumnSlice<N, V> {
		public CmbColumnSlice() {
		}
		public abstract CmbColumn<N, V> getColumnByName(N name);
		public abstract List<CmbColumn<N, V>> getColumns();
		public abstract int size();
	}
	
	public static abstract class CmbSuperColumnSlice<SN, N, V> {
		public CmbSuperColumnSlice() {
		}
		public abstract CmbSuperColumn<SN, N, V> getColumnByName(SN name);
		public abstract List<CmbSuperColumn<SN, N, V>> getSuperColumns();
	}
	
	public static abstract class CmbSuperColumn<SN, N, V> {
		public CmbSuperColumn() {
		}
		public abstract SN getName();
		public abstract List<CmbColumn<N, V>> getColumns();
	}

	public static final String CLUSTER_NAME = CMBProperties.getInstance().getClusterName();
	public static final String CLUSTER_URL = CMBProperties.getInstance().getClusterUrl();
	
	private static long counter = 0;

	public static synchronized long newTime(long t, boolean isHidden) {   
        t = t << 21;
        //top 2 bits are 0. 64th and 63rd.
        //set 21st bit if hidden
        if (isHidden) {
            t |= 0x0000000000100000L;
        }
        //add 20 bit counter
        if (counter == 1048575) {
            counter = 0;
        }
        t += counter++;
        return t;
    }
    
    public static long getTimestampFromHash(long t){
    	t = t >> 21;
        return t;
    }

	public java.util.UUID getTimeUUID(long timeMillis) throws InterruptedException {
		return new java.util.UUID(newTime(timeMillis, false), com.eaio.uuid.UUIDGen.getClockSeqAndNode());
	}

	public java.util.UUID getUniqueTimeUUID(long millis) {
		return new java.util.UUID(com.eaio.uuid.UUIDGen.createTime(millis),	com.eaio.uuid.UUIDGen.getClockSeqAndNode());
	}

	public long getTimeLong(long timeMillis) throws InterruptedException {
		long newTime = timeMillis * 1000000000 + (System.nanoTime() % 1000000) * 1000 + rand.nextInt(999999); 
		return newTime;
	}
	
	public abstract boolean isAlive();
	
	public abstract CmbComposite getCmbComposite(List<?> l);
	
	public abstract CmbComposite getCmbComposite(Object... os);

	public abstract <K, N, V> void update(String keyspace, String columnFamily, K key, N column, V value, 
			CmbSerializer keySerializer, CmbSerializer nameSerializer, CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, SN, N, V> void insertSuperColumn(String keyspace, String columnFamily, K key, CmbSerializer keySerializer, SN superName, Integer ttl, 
			CmbSerializer superNameSerializer, Map<N, V> subColumnNameValues, CmbSerializer columnSerializer,
			CmbSerializer valueSerializer)	throws PersistenceException;

	public abstract <K, SN, N, V> void insertSuperColumns(
			String keyspace, String columnFamily, K key, CmbSerializer keySerializer,
			Map<SN, Map<N, V>> superNameSubColumnsMap, int ttl,
			CmbSerializer superNameSerializer, CmbSerializer columnSerializer,
			CmbSerializer valueSerializer)	throws PersistenceException;

	public abstract <K, N, V> List<CmbRow<K, N, V>> readNextNNonEmptyRows(
			String keyspace, String columnFamily, K lastKey, int numRows, int numCols,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer,
			CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, N, V> List<CmbRow<K, N, V>> readNextNRows(
			String keyspace, String columnFamily, K lastKey, int numRows, int numCols,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer,
			CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, N, V> List<CmbRow<K, N, V>> readNextNRows(
			String keyspace, String columnFamily, K lastKey, N whereColumn, V whereValue,
			int numRows, int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, N, V> List<CmbRow<K, N, V>> readNextNRows(
			String keyspace, String columnFamily, K lastKey, Map<N, V> columnValues,
			int numRows, int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, N, V> CmbColumnSlice<N, V> readColumnSlice(
			String keyspace, String columnFamily, K key, N firstColumnName, N lastColumnName,
			int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, SN, N, V> CmbSuperColumnSlice<SN, N, V> readRowFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, SN firstColumnName, SN lastColumnName,
			int numCols, CmbSerializer keySerializer,
			CmbSerializer superNameSerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, SN, N, V> CmbSuperColumn<SN, N, V> readColumnFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, SN columnName,
			CmbSerializer keySerializer, CmbSerializer superNameSerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readMultipleColumnsFromSuperColumnFamily(
			String keyspace, String columnFamily, Collection<K> keys,
			Collection<SN> columnNames, CmbSerializer keySerializer,
			CmbSerializer superNameSerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer) throws PersistenceException;

	public abstract <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readColumnsFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, CmbSerializer keySerializer,
			CmbSerializer superNameSerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer,
			 SN firstCol, SN lastCol, int numCol) throws PersistenceException;

	public abstract <K, N, V> void insertRow(String keyspace, K rowKey,
			String columnFamily, Map<N, V> columnValues,
			CmbSerializer keySerializer, CmbSerializer nameSerializer,
			CmbSerializer valueSerializer, Integer ttl) throws PersistenceException;

	public abstract <K, N, V> void insertRows(
			String keyspace, Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			CmbSerializer keySerializer, CmbSerializer nameSerializer,
			CmbSerializer valueSerializer, Integer ttl) throws PersistenceException;

	public abstract <K, N> void delete(String keyspace, String columnFamily, K key, N column, 
			CmbSerializer keySerializer, CmbSerializer columnSerializer) throws PersistenceException;

	public abstract <K, N> void deleteBatch(String keyspace, String columnFamily,
			List<K> keyList, List<N> columnList, CmbSerializer keySerializer,
			 CmbSerializer columnSerializer) throws PersistenceException;

	public abstract <K, SN, N> void deleteSuperColumn(
			String keyspace, String superColumnFamily, K key, SN superColumn, CmbSerializer keySerializer, CmbSerializer superColumnSerializer) throws PersistenceException;

	public abstract <K, N> int getCount(String keyspace, String columnFamily, K key,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer) throws PersistenceException;

	public abstract <K, N> void incrementCounter(String keyspace, String columnFamily, K rowKey,
			String columnName, int incrementBy, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer) throws PersistenceException;

	public abstract <K, N> void decrementCounter(String keyspace, String columnFamily, K rowKey,
			String columnName, int decrementBy, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer) throws PersistenceException;

	public abstract <K, N> void deleteCounter(String keyspace, String columnFamily, K rowKey,
			N columnName, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer) throws PersistenceException;

	public abstract <K, N> long getCounter(String keyspace, String columnFamily, K rowKey,
			N columnName, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer) throws PersistenceException;

}