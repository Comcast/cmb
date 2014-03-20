package com.comcast.cmb.common.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraAstyanaxPersistence extends AbstractCassandraPersistence {
	
	private static Map<String, Keyspace> keyspaces = new HashMap<String, Keyspace>();
	
	public CassandraAstyanaxPersistence() {
		initPersistence();
	}
	
	private void initPersistence() {
		
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
			N column, V value, CmbSerializer keySerializer,
			CmbSerializer nameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, SN, N, V> void insertSuperColumn(String keyspace,
			String columnFamily, K key, CmbSerializer keySerializer,
			SN superName, Integer ttl, CmbSerializer superNameSerializer,
			Map<N, V> subColumnNameValues, CmbSerializer columnSerializer,
			CmbSerializer valueSerializer) throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, SN, N, V> void insertSuperColumns(String keyspace,
			String columnFamily, K key, CmbSerializer keySerializer,
			Map<SN, Map<N, V>> superNameSubColumnsMap, int ttl,
			CmbSerializer superNameSerializer,
			CmbSerializer columnSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNNonEmptyRows(
			String keyspace, String columnFamily, K lastKey, int numRows,
			int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace,
			String columnFamily, K lastKey, int numRows, int numCols,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer,
			CmbSerializer valueSerializer) throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace,
			String columnFamily, K lastKey, N whereColumn, V whereValue,
			int numRows, int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace,
			String columnFamily, K lastKey, Map<N, V> columnValues,
			int numRows, int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> CmbColumnSlice<N, V> readColumnSlice(String keyspace,
			String columnFamily, K key, N firstColumnName, N lastColumnName,
			int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> CmbSuperColumnSlice<SN, N, V> readRowFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, SN firstColumnName,
			SN lastColumnName, int numCols, CmbSerializer keySerializer,
			CmbSerializer superNameSerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> CmbSuperColumn<SN, N, V> readColumnFromSuperColumnFamily(
			String keyspace, String columnFamily, K key, SN columnName,
			CmbSerializer keySerializer, CmbSerializer superNameSerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readMultipleColumnsFromSuperColumnFamily(
			String keyspace, String columnFamily, Collection<K> keys,
			Collection<SN> columnNames, CmbSerializer keySerializer,
			CmbSerializer superNameSerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readColumnsFromSuperColumnFamily(
			String keyspace, String columnFamily, K key,
			CmbSerializer keySerializer, CmbSerializer superNameSerializer,
			CmbSerializer columnNameSerializer,
			CmbSerializer valueSerializer, SN firstCol, SN lastCol, int numCol)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, N, V> void insertRow(String keyspace, K rowKey, 
			String columnFamily, Map<N, V> columnValues,
			CmbSerializer keySerializer, CmbSerializer nameSerializer,
			CmbSerializer valueSerializer, Integer ttl) throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N, V> void insertRows(String keyspace,
			Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			CmbSerializer keySerializer, CmbSerializer nameSerializer,
			CmbSerializer valueSerializer, Integer ttl) throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> void delete(String keyspace, String columnFamily, K key,
			N column, CmbSerializer keySerializer,
			CmbSerializer columnSerializer) throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> void deleteBatch(String keyspace, String columnFamily,
			List<K> keyList, List<N> columnList, CmbSerializer keySerializer,
			CmbSerializer columnSerializer) throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, SN, N> void deleteSuperColumn(String keyspace,
			String superColumnFamily, K key, SN superColumn,
			CmbSerializer keySerializer, CmbSerializer superColumnSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> int getCount(String keyspace, String columnFamily, K key,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <K, N> void incrementCounter(String keyspace, String columnFamily,
			K rowKey, String columnName, int incrementBy,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> void decrementCounter(String keyspace, String columnFamily,
			K rowKey, String columnName, int decrementBy,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer)
			throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> void deleteCounter(String keyspace, String columnFamily,
			K rowKey, N columnName, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer) throws PersistenceException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <K, N> long getCounter(String keyspace, String columnFamily,
			K rowKey, N columnName, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer) throws PersistenceException {
		// TODO Auto-generated method stub
		return 0;
	}
}
