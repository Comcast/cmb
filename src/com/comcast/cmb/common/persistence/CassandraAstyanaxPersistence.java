package com.comcast.cmb.common.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.SuperCfTemplate;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.mutation.MutationResult;

public class CassandraAstyanaxPersistence extends AbstractCassandraPersistence {

	@Override
	public boolean isAlive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Keyspace getKeySpace(HConsistencyLevel consistencyLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> void update(String columnFamily, K key, N column, V value, Serializer<K> keySerializer, Serializer<N> nameSerializer, Serializer<V> valueSerializer, HConsistencyLevel level)
			throws HectorException {
		// TODO Auto-generated method stub

	}

	@Override
	public <K, SN, N, V> MutationResult insertSuperColumn(String columnFamily,
			K key, Serializer<K> keySerializer, SN superName, Integer ttl,
			Serializer<SN> superNameSerializer, Map<N, V> subColumnNameValues,
			Serializer<N> columnSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) throws HectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> MutationResult insertSuperColumns(String columnFamily,
			K key, Serializer<K> keySerializer,
			Map<SN, Map<N, V>> superNameSubColumnsMap, int ttl,
			Serializer<SN> superNameSerializer, Serializer<N> columnSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level)
			throws HectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <N, V> V read(ColumnFamilyTemplate<String, N> template, String key,
			N column, V returnType) throws HectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<Row<K, N, V>> readNextNRows(String columnFamily,
			K lastKey, int numRows, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<Row<K, N, V>> readNextNRows(String columnFamily,
			K lastKey, int numRows, int numCols, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<Row<K, N, V>> readNextNNonEmptyRows(
			String columnFamily, K lastKey, int numRows, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<Row<K, N, V>> readNextNRows(String columnFamily,
			K lastKey, N whereColumn, V whereValue, int numRows, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<Row<K, N, V>> readNextNRows(String columnFamily,
			K lastKey, Map<N, V> columnValues, int numRows, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> ColumnSlice<N, V> readColumnSlice(String columnFamily,
			K key, int numCols, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> ColumnSlice<N, V> readColumnSlice(String columnFamily,
			K key, N firstColumnName, N lastColumnName, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> SuperSlice<SN, N, V> readRowFromSuperColumnFamily(
			String columnFamily, K key, SN firstColumnName, SN lastColumnName,
			int numCols, Serializer<K> keySerializer,
			Serializer<SN> superNameSerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> HSuperColumn<SN, N, V> readColumnFromSuperColumnFamily(
			String columnFamily, K key, SN columnName,
			Serializer<K> keySerializer, Serializer<SN> superNameSerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> List<HSuperColumn<SN, N, V>> readMultipleColumnsFromSuperColumnFamily(
			String columnFamily, Collection<K> keys,
			Collection<SN> columnNames, Serializer<K> keySerializer,
			Serializer<SN> superNameSerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> List<HSuperColumn<SN, N, V>> readColumnsFromSuperColumnFamily(
			String columnFamily, K key, Serializer<K> keySerializer,
			Serializer<SN> superNameSerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level, SN firstCol, SN lastCol, int numCol) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MutationResult insertOrUpdateRow(String rowKey, String columnFamily,
			Map<String, String> columnValues, HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MutationResult insertRow(String rowKey, String columnFamily,
			Map<String, String> columnValues, HConsistencyLevel level,
			Integer ttl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> MutationResult insertRow(K rowKey, String columnFamily,
			Map<N, V> columnValues, Serializer<K> keySerializer,
			Serializer<N> nameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level, Integer ttl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> MutationResult insertRows(
			Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			Serializer<K> keySerializer, Serializer<N> nameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level, Integer ttl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N> void delete(String columnFamily, K key, N column, 
			Serializer<K> keySerializer, Serializer<N> columnSerializer, 
			HConsistencyLevel level) throws HectorException {
		// TODO Auto-generated method stub

	}

	@Override
	public <K, N> void deleteBatch(String columnFamily, List<K> keyList,
			List<N> columnList, Serializer<K> keySerializer,
			HConsistencyLevel level, Serializer<N> columnSerializer)
			throws HectorException {
		// TODO Auto-generated method stub

	}

	@Override
	public <K, SN, N> void deleteSuperColumn(
			String superColumnFamily, K key, SN superColumn, Serializer<K> keySerializer, Serializer<SN> superColumnSerializer,
			HConsistencyLevel level)
			throws HectorException {
		// TODO Auto-generated method stub

	}

	@Override
	public UUID getTimeUUID(long timeMillis) throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UUID getUniqueTimeUUID(long millis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getTimeLong(long timeMillis) throws InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <K, N> int getCount(String columnFamily, K key,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			HConsistencyLevel level) throws HectorException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <K, N> void incrementCounter(String columnFamily, K rowKey,
			String columnName, int incrementBy, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, HConsistencyLevel level) {
		// TODO Auto-generated method stub

	}

	@Override
	public <K, N> void decrementCounter(String columnFamily, K rowKey,
			String columnName, int decrementBy, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, HConsistencyLevel level) {
		// TODO Auto-generated method stub

	}

	@Override
	public <K, N> void deleteCounter(String columnFamily, K rowKey,
			N columnName, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, HConsistencyLevel level) {
		// TODO Auto-generated method stub

	}

	@Override
	public <K, N> long getCounter(String columnFamily, K rowKey, N columnName,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			HConsistencyLevel level) {
		// TODO Auto-generated method stub
		return 0;
	}

}
