package com.comcast.cmb.common.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.comcast.cmb.common.util.CMBProperties;

public abstract class AbstractCassandraPersistence {
	
	protected static Random rand = new Random();
	
	public static enum CMB_SERIALIZER { STRING_SERIALIZER, COMPOSITE_SERIALIZER };
	
	public static class CmbColumn<N, V> {
		public CmbColumn(N name, V value) {
			this.name = name;
			this.value = value;
		}
		public N name;
		public V value;
	}
	
	public static class CmbRow<K, N, V> {
		public CmbRow(K key, List<CmbColumn<N, V>> row) {
			this.key = key;
			this.row = row;
		}
		public K key;
		public List<CmbColumn<N, V>> row;
	}
	
	public static class CmbColumnSlice<N, V> {
		public CmbColumnSlice(List<CmbColumn<N, V>> slice) {
			this.slice = slice;
		}
		public List<CmbColumn<N, V>> slice;
	}
	
	public static class CmbSuperColumnSlice<SN, N, V> {
		public CmbSuperColumnSlice(List<CmbSuperColumn<SN, N, V>> slice) {
			this.slice = slice;
		}
		public List<CmbSuperColumn<SN, N, V>> slice;
	}
	
	public static class CmbSuperColumn<SN, N, V> {
		public CmbSuperColumn(SN superName, List<CmbColumn<N, V>> columns) {
			this.superName = superName;
			this.columns = columns;
		}
		public SN superName;
		public List<CmbColumn<N, V>> columns;
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

	public abstract <K, N, V> void update(String keyspace, String columnFamily, K key, N column, V value, 
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER nameSerializer, CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, SN, N, V> void insertSuperColumn(String keyspace, String columnFamily, K key, CMB_SERIALIZER keySerializer, SN superName, Integer ttl, 
			CMB_SERIALIZER superNameSerializer, Map<N, V> subColumnNameValues, CMB_SERIALIZER columnSerializer,
			CMB_SERIALIZER valueSerializer)	throws Exception;

	public abstract <K, SN, N, V> void insertSuperColumns(
			String keyspace, String columnFamily, K key, CMB_SERIALIZER keySerializer,
			Map<SN, Map<N, V>> superNameSubColumnsMap, int ttl,
			CMB_SERIALIZER superNameSerializer, CMB_SERIALIZER columnSerializer,
			CMB_SERIALIZER valueSerializer)	throws Exception;

	public abstract <K, N, V> List<CmbRow<K, N, V>> readNextNNonEmptyRows(
			String keyspace, String columnFamily, K lastKey, int numRows, int numCols,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer,
			CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, N, V> List<CmbRow<K, N, V>> readNextNRows(
			String keyspace, String columnFamily, K lastKey, int numRows, int numCols,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer,
			CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, N, V> List<CmbRow<K, N, V>> readNextNRows(
			String keyspace, String columnFamily, K lastKey, N whereColumn, V whereValue,
			int numRows, int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, N, V> List<CmbRow<K, N, V>> readNextNRows(
			String keyspace, String columnFamily, K lastKey, Map<N, V> columnValues,
			int numRows, int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, N, V> CmbColumnSlice<N, V> readColumnSlice(
			String keyspace, String columnFamily, K key, N firstColumnName, N lastColumnName,
			int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, SN, N, V> CmbSuperColumnSlice<SN, N, V> readRowFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, SN firstColumnName, SN lastColumnName,
			int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, SN, N, V> CmbSuperColumn<SN, N, V> readColumnFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, SN columnName,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readMultipleColumnsFromSuperColumnFamily(
			String keyspace, String columnFamily, Collection<K> keys,
			Collection<SN> columnNames, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception;

	public abstract <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readColumnsFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer,
			 SN firstCol, SN lastCol, int numCol) throws Exception;

	public abstract <K, N, V> void insertRow(K rowKey,
			String keyspace, String columnFamily, Map<N, V> columnValues,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER nameSerializer,
			CMB_SERIALIZER valueSerializer, Integer ttl) throws Exception;

	public abstract <K, N, V> void insertRows(
			String keyspace, Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER nameSerializer,
			CMB_SERIALIZER valueSerializer, Integer ttl) throws Exception;

	public abstract <K, N> void delete(String keyspace, String columnFamily, K key, N column, 
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnSerializer) throws Exception;

	public abstract <K, N> void deleteBatch(String keyspace, String columnFamily,
			List<K> keyList, List<N> columnList, CMB_SERIALIZER keySerializer,
			 CMB_SERIALIZER columnSerializer) throws Exception;

	public abstract <K, SN, N> void deleteSuperColumn(
			String keyspace, String superColumnFamily, K key, SN superColumn, CMB_SERIALIZER keySerializer, CMB_SERIALIZER superColumnSerializer) throws Exception;

	public abstract <K, N> int getCount(String keyspace, String columnFamily, K key,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer) throws Exception;

	public abstract <K, N> void incrementCounter(String keyspace, String columnFamily, K rowKey,
			String columnName, int incrementBy, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception;

	public abstract <K, N> void decrementCounter(String keyspace, String columnFamily, K rowKey,
			String columnName, int decrementBy, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception;

	public abstract <K, N> void deleteCounter(String keyspace, String columnFamily, K rowKey,
			N columnName, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception;

	public abstract <K, N> long getCounter(String keyspace, String columnFamily, K rowKey,
			N columnName, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception;

}