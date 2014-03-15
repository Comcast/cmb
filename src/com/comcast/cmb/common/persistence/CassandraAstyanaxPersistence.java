package com.comcast.cmb.common.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.comcast.cmb.common.util.CMBProperties;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraAstyanaxPersistence extends AbstractCassandraPersistence {
	
	private static Map<String, Keyspace> keyspaces = new HashMap<String, Keyspace>();
	
	private void init() {
		
		List<String> keyspaceNames = new ArrayList<String>();
		keyspaceNames.add(CMBProperties.getInstance().getCMBKeyspace());
		keyspaceNames.add(CMBProperties.getInstance().getCNSKeyspace());
		keyspaceNames.add(CMBProperties.getInstance().getCQSKeyspace());
		
		for (String k : keyspaceNames) {
		
			AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
			.forCluster(CLUSTER_NAME)
			.forKeyspace(CMBProperties.getInstance().getCMBKeyspace())
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
			.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
					)
					.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("CMBAstyananxConnectionPool")
					//.setPort(9160)
					.setMaxConnsPerHost(1)
					.setSeeds(AbstractCassandraPersistence.CLUSTER_URL)
							)
							.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
							.buildKeyspace(ThriftFamilyFactory.getInstance());
	
			context.start();
			Keyspace keyspace = context.getClient();
			
			keyspaces.put(CMBProperties.getInstance().getCMBKeyspace(), keyspace);
		}
	}

	@Override
	public boolean isAlive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <K, N, V> void update(String keyspace, String columnFamily, K key,
			N column, V value, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER nameSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, SN, N, V> void insertSuperColumn(String keyspace,
			String columnFamily, K key, CMB_SERIALIZER keySerializer,
			SN superName, Integer ttl, CMB_SERIALIZER superNameSerializer,
			Map<N, V> subColumnNameValues, CMB_SERIALIZER columnSerializer,
			CMB_SERIALIZER valueSerializer) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, SN, N, V> void insertSuperColumns(String keyspace,
			String columnFamily, K key, CMB_SERIALIZER keySerializer,
			Map<SN, Map<N, V>> superNameSubColumnsMap, int ttl,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNNonEmptyRows(
			String keyspace, String columnFamily, K lastKey, int numRows,
			int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace,
			String columnFamily, K lastKey, int numRows, int numCols,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer,
			CMB_SERIALIZER valueSerializer) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace,
			String columnFamily, K lastKey, N whereColumn, V whereValue,
			int numRows, int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace,
			String columnFamily, K lastKey, Map<N, V> columnValues,
			int numRows, int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> CmbColumnSlice<N, V> readColumnSlice(String keyspace,
			String columnFamily, K key, N firstColumnName, N lastColumnName,
			int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> CmbSuperColumnSlice<SN, N, V> readRowFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, SN firstColumnName,
			SN lastColumnName, int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> CmbSuperColumn<SN, N, V> readColumnFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, SN columnName,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readMultipleColumnsFromSuperColumnFamily(
			String keyspace, String columnFamily, Collection<K> keys,
			Collection<SN> columnNames, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readColumnsFromSuperColumnFamily(
			String keyspace, String columnFamily, K key,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer,
			CMB_SERIALIZER valueSerializer, SN firstCol, SN lastCol, int numCol)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> void insertRow(K rowKey, String keyspace,
			String columnFamily, Map<N, V> columnValues,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER nameSerializer,
			CMB_SERIALIZER valueSerializer, Integer ttl) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N, V> void insertRows(String keyspace,
			Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER nameSerializer,
			CMB_SERIALIZER valueSerializer, Integer ttl) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> void delete(String keyspace, String columnFamily, K key,
			N column, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnSerializer) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> void deleteBatch(String keyspace, String columnFamily,
			List<K> keyList, List<N> columnList, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnSerializer) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, SN, N> void deleteSuperColumn(String keyspace,
			String superColumnFamily, K key, SN superColumn,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER superColumnSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> int getCount(String keyspace, String columnFamily, K key,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <K, N> void incrementCounter(String keyspace, String columnFamily,
			K rowKey, String columnName, int incrementBy,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> void decrementCounter(String keyspace, String columnFamily,
			K rowKey, String columnName, int decrementBy,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> void deleteCounter(String keyspace, String columnFamily,
			K rowKey, N columnName, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> long getCounter(String keyspace, String columnFamily,
			K rowKey, N columnName, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}
