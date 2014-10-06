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
package com.comcast.cmb.common.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraAstyanaxPersistence extends AbstractDurablePersistence {
	
	// TODO: timeout exception
	
	private static Map<String, Keyspace> keyspaces = new HashMap<String, Keyspace>();
	
	private static Logger logger = Logger.getLogger(CassandraAstyanaxPersistence.class);
	
	private static CassandraAstyanaxPersistence instance;
	
	public static CassandraAstyanaxPersistence getInstance() {
		
		if (instance == null) {
			instance = new CassandraAstyanaxPersistence();
		}
		
		return instance;
	}
	
	private CassandraAstyanaxPersistence() {
		initPersistence();
	}
	
	private void initPersistence() {
		
		List<String> keyspaceNames = new ArrayList<String>();
		keyspaceNames.add(CMBProperties.getInstance().getCMBKeyspace());
		keyspaceNames.add(CMBProperties.getInstance().getCNSKeyspace());
		keyspaceNames.add(CMBProperties.getInstance().getCQSKeyspace());
		
		String dataCenter = CMBProperties.getInstance().getCassandraDataCenter();
		String username = CMBProperties.getInstance().getCassandraUsername();
		String password = CMBProperties.getInstance().getCassandraPassword();		
		
		for (String k : keyspaceNames) {

			ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("CMBAstyananxConnectionPool")
			.setMaxConnsPerHost(CMBProperties.getInstance().getAstyanaxMaxConnectionsPerNode())
			.setSocketTimeout(CMBProperties.getInstance().getCassandraThriftSocketTimeOutMS())
			.setConnectTimeout(CMBProperties.getInstance().getAstyanaxConnectionWaitTimeOutMS())
			.setSeeds(AbstractDurablePersistence.CLUSTER_URL);
			
			if (username != null && password != null) {
				connectionPoolConfiguration.setAuthenticationCredentials(new SimpleAuthenticationCredentials(username, password));
			}

			if (dataCenter != null && !dataCenter.equals("")) {
				connectionPoolConfiguration.setLocalDatacenter(dataCenter);
			}
		
			AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
			.forCluster(CLUSTER_NAME)
			.forKeyspace(k)
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()  
			.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
			.setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
			.setDefaultReadConsistencyLevel(ConsistencyLevel.valueOf("CL_"+CMBProperties.getInstance().getReadConsistencyLevel()))
			.setDefaultWriteConsistencyLevel(ConsistencyLevel.valueOf("CL_"+CMBProperties.getInstance().getWriteConsistencyLevel())))
					.withConnectionPoolConfiguration(connectionPoolConfiguration)
							.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
							.buildKeyspace(ThriftFamilyFactory.getInstance());
			context.start();
			Keyspace keyspace = context.getClient();
			keyspaces.put(k, keyspace);
		}
	}
	
	private Keyspace getKeyspace(String keyspace) {
		return keyspaces.get(keyspace);
	}

	private static Serializer getSerializer(CmbSerializer s) throws PersistenceException {
		if (s instanceof CmbStringSerializer) {
			return StringSerializer.get();
		} else if (s instanceof CmbCompositeSerializer) {
			return CompositeSerializer.get();
		} else if (s instanceof CmbLongSerializer) {
			return LongSerializer.get();
		}
		throw new PersistenceException(CMBErrorCodes.InternalError, "Unknown serializer " + s);
	}

	private static Object getComposite(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof CmbAstyanaxComposite) {
			return ((CmbAstyanaxComposite)o).getComposite();
		}
		return o;
	}

	private static Collection<Object> getComposites(Collection l) {
		Collection<Object> r = new ArrayList<Object>();
		if (l == null) {
			return null;
		}
		for (Object o : l) {
			r.add(getComposite(o));
		}
		return r;
	}

	private static Object getCmbComposite(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Composite) {
			return new CmbAstyanaxComposite((Composite)o);
		}
		return o;
	}

	@Override
	public CmbComposite getCmbComposite(List<?> l) {
		return new CmbAstyanaxComposite(l);
	}

	@Override
	public CmbComposite getCmbComposite(Object... os) {
		return new CmbAstyanaxComposite(os);
	}

	public static class CmbAstyanaxComposite extends CmbComposite {
		private final Composite composite;
		public CmbAstyanaxComposite(List<?> l) {
			composite = new Composite(l);
		}
		public CmbAstyanaxComposite(Object... os) {
			composite = new Composite(os);
		}
		public CmbAstyanaxComposite(Composite composite) {
			this.composite = composite;
		}
		public Composite getComposite() {
			return composite;
		}
		@Override
		public Object get(int i) {
			return composite.get(i);
		}
		@Override
		public String toString() {
			return composite.toString();
		}
		@Override
		public int compareTo(CmbComposite c) {
			return this.composite.compareTo(((CmbAstyanaxComposite)c).getComposite());
		}
		@Override
		public boolean equals(Object o) {
			if (!(o instanceof CmbAstyanaxComposite)) {
				return false;
			}
			return this.composite.equals(((CmbAstyanaxComposite)o).getComposite());
		}
	}

	public static class CmbAstyanaxColumn<N, V> extends CmbColumn<N, V> {
		private Column<N> astyanaxColumn;
		public CmbAstyanaxColumn(Column<N> astyanaxColumn) {
			this.astyanaxColumn = astyanaxColumn;
		}
		@Override
		public N getName() {
			return (N)getCmbComposite(astyanaxColumn.getName());
		}
		@Override
		public V getValue() {
			//TODO: support types other than String as well
			return (V)astyanaxColumn.getValue(StringSerializer.get());
		}
		@Override
		public long getClock() {
			return astyanaxColumn.getTimestamp();
		}
	}

	public static class CmbAstyanaxRow<K, N, V> extends CmbRow<K, N, V> {
		private K key;
		private ColumnList<N> columns;
		public CmbAstyanaxRow(Row<K, N> row) {
			this.key = row.getKey();
			this.columns = row.getColumns();
		}
		public CmbAstyanaxRow(K key, ColumnList<N> columns) {
			this.key = key;
			this.columns = columns;
		}
		@Override
		public K getKey() {
			return this.key;
		}
		@Override
		public CmbColumnSlice<N, V> getColumnSlice() {
			return new CmbAstyanaxColumnSlice<N, V>(columns);
		}
	}

	public static class CmbAstyanaxColumnSlice<N, V> extends CmbColumnSlice<N, V> {
		private ColumnList<N> astyanaxColumns;
		private List<CmbColumn<N, V>> columns = null;
		public CmbAstyanaxColumnSlice(ColumnList<N> columns) {
			this.astyanaxColumns = columns;
		}
		@Override
		public CmbAstyanaxColumn<N, V> getColumnByName(N name) {
			if (astyanaxColumns.getColumnByName(name) != null) {
				return new CmbAstyanaxColumn<N, V>(astyanaxColumns.getColumnByName(name));
			} else {
				return null;
			}
		}
		private void loadColumns() {
			if (columns == null) {
				columns = new ArrayList<CmbColumn<N, V>>();
				for (Column<N> c : astyanaxColumns) {
					columns.add(new CmbAstyanaxColumn<N, V>(c));
				}
			}
		}
		@Override
		public List<CmbColumn<N, V>> getColumns() {
			loadColumns();
			return columns;
		}
		@Override
		public int size() {
			loadColumns();
			return columns.size();
		}
	}

	private <K, N, V> List<CmbRow<K, N, V>> getRows(List<Row<K, N>> rows) throws PersistenceException {
		List<CmbRow<K, N, V>> l = new ArrayList<CmbRow<K, N, V>>();
		for (Row<K, N> r : rows) {
			l.add(new CmbAstyanaxRow<K, N, V>(r));
		}
		return l;
	}
	
	private <K, N, V> List<CmbRow<K, N, V>> getRows(Rows<K, N> rows) throws PersistenceException {
		List<CmbRow<K, N, V>> l = new ArrayList<CmbRow<K, N, V>>();
		Iterator<Row<K, N>> iter = rows.iterator();
		while (iter.hasNext()) {
			Row<K, N> r = iter.next();
			l.add(new CmbAstyanaxRow<K, N, V>(r));
		}
		return l;
	}

	@Override
	public boolean isAlive() {

		// TODO: implement
		
		return true;
	}
	
	private static ConcurrentHashMap<String, ColumnFamily> cf = new ConcurrentHashMap<String, ColumnFamily>();
	
	static {
		
		ColumnFamily<String, String> CF_CNS_TOPICS =
				new ColumnFamily<String, String>(
						CNS_TOPICS,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_TOPICS, CF_CNS_TOPICS);

		ColumnFamily<String, String> CF_CNS_TOPICS_BY_USER_ID =
				new ColumnFamily<String, String>(
						CNS_TOPICS_BY_USER_ID,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_TOPICS_BY_USER_ID, CF_CNS_TOPICS_BY_USER_ID);

		ColumnFamily<String, Composite> CF_CNS_TOPIC_SUBSCRIPTIONS =
				new ColumnFamily<String, Composite>(
						CNS_TOPIC_SUBSCRIPTIONS,  
						StringSerializer.get(), 
						CompositeSerializer.get()); 
		cf.put(CNS_TOPIC_SUBSCRIPTIONS, CF_CNS_TOPIC_SUBSCRIPTIONS);

		ColumnFamily<String, String> CF_CNS_TOPIC_SUBSCRIPTIONS_INDEX =
				new ColumnFamily<String, String>(
						CNS_TOPIC_SUBSCRIPTIONS_INDEX,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_TOPIC_SUBSCRIPTIONS_INDEX, CF_CNS_TOPIC_SUBSCRIPTIONS_INDEX);

		ColumnFamily<String, String> CF_CNS_TOPIC_SUBSCRIPTIONS_USER_INDEX =
				new ColumnFamily<String, String>(
						CNS_TOPIC_SUBSCRIPTIONS_USER_INDEX,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_TOPIC_SUBSCRIPTIONS_USER_INDEX, CF_CNS_TOPIC_SUBSCRIPTIONS_USER_INDEX);
		
		ColumnFamily<String, String> CF_CNS_TOPIC_SUBSCRIPTIONS_TOKEN_INDEX =
				new ColumnFamily<String, String>(
						CNS_TOPIC_SUBSCRIPTIONS_TOKEN_INDEX,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_TOPIC_SUBSCRIPTIONS_TOKEN_INDEX, CF_CNS_TOPIC_SUBSCRIPTIONS_TOKEN_INDEX);

		ColumnFamily<String, String> CF_CNS_TOPIC_ATTRIBUTES =
				new ColumnFamily<String, String>(
						CNS_TOPIC_ATTRIBUTES,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_TOPIC_ATTRIBUTES, CF_CNS_TOPIC_ATTRIBUTES);

		ColumnFamily<String, String> CF_CNS_SUBSCRIPTION_ATTRIBUTES =
				new ColumnFamily<String, String>(
						CNS_SUBSCRIPTION_ATTRIBUTES,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_SUBSCRIPTION_ATTRIBUTES, CF_CNS_SUBSCRIPTION_ATTRIBUTES);

		ColumnFamily<String, String> CF_CNS_TOPIC_STATS =
				new ColumnFamily<String, String>(
						CNS_TOPIC_STATS,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_TOPIC_STATS, CF_CNS_TOPIC_STATS);

		ColumnFamily<String, String> CF_CNS_WORKERS =
				new ColumnFamily<String, String>(
						CNS_WORKERS,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_WORKERS, CF_CNS_WORKERS);

		ColumnFamily<String, String> CF_CNS_API_SERVERS =
				new ColumnFamily<String, String>(
						CNS_API_SERVERS,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CNS_API_SERVERS, CF_CNS_API_SERVERS);
	
		ColumnFamily<String, String> CF_CQS_QUEUES =
				new ColumnFamily<String, String>(
						CQS_QUEUES,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CQS_QUEUES, CF_CQS_QUEUES);
	
		ColumnFamily<String, String> CF_CQS_QUEUES_BY_USER_ID =
				new ColumnFamily<String, String>(
						CQS_QUEUES_BY_USER_ID,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CQS_QUEUES_BY_USER_ID, CF_CQS_QUEUES_BY_USER_ID);

		ColumnFamily<String, Composite> CF_CQS_PARTITIONED_QUEUE_MESSAGES =
				new ColumnFamily<String, Composite>(
						CQS_PARTITIONED_QUEUE_MESSAGES,  
						StringSerializer.get(), 
						CompositeSerializer.get()); 
		cf.put(CQS_PARTITIONED_QUEUE_MESSAGES, CF_CQS_PARTITIONED_QUEUE_MESSAGES);

		ColumnFamily<String, String> CF_CQS_API_SERVERS =
				new ColumnFamily<String, String>(
						CQS_API_SERVERS,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CQS_API_SERVERS, CF_CQS_API_SERVERS);

		ColumnFamily<String, String> CF_CMB_USERS =
				new ColumnFamily<String, String>(
						CMB_USERS,  
						StringSerializer.get(), 
						StringSerializer.get()); 
		cf.put(CMB_USERS, CF_CMB_USERS);
	}

	private static ColumnFamily getColumnFamily(String columnFamily) {
		return cf.get(columnFamily);
	}

	@Override
	public <K, N, V> void update(String keyspace, String columnFamily, K key,
			N column, V value, CmbSerializer keySerializer,
			CmbSerializer nameSerializer, CmbSerializer valueSerializer, Integer ttl)
			throws PersistenceException {
		
		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=update column_family=" + columnFamily + " key=" + key + " column=" + column + " value=" + value);

		try {
			MutationBatch m = 
					getKeyspace(keyspace).
					prepareMutationBatch();
			m.withRow((ColumnFamily)getColumnFamily(columnFamily), key).
				putColumn(getComposite(column), value, getSerializer(valueSerializer), ttl);
			m.execute();
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
		}
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readAllRows(
			String keyspace, String columnFamily, int numRows, int numCols,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer,
			CmbSerializer valueSerializer)
			throws PersistenceException {

		long ts1 = System.currentTimeMillis();
		logger.debug("event=read_next_n_non_empty_rows cf=" + columnFamily + " num_rows=" + numRows + " num_cols=" + numCols);

		try {

		    /*OperationResult<Rows<K, N>> or = getKeyspace(keyspace).
		    		prepareQuery(getColumnFamily(columnFamily)).
		    		getRowRange(lastKey, null, null, null, numRows). // this wouldn't work in Astyanax, hence getting rid of fake row range queries with random partitioner
		    		withColumnRange(new RangeBuilder().setLimit(numCols).build()).
		    		execute();
		    rows = or.getResult();*/
		    
		    OperationResult<Rows<K, N>> or = 
		    		getKeyspace(keyspace).
		    		prepareQuery(getColumnFamily(columnFamily)).
		    		getAllRows().
		    		setRowLimit(numRows).
		    		setIncludeEmptyRows(false).
		    		execute();

	    	return getRows(or.getResult());
		} catch (NotFoundException ex){
				//ignore. 
				return null;
		} catch (ConnectionException ex) {
		
			throw new PersistenceException(ex);

		} finally {

			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
		}
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readRowsByIndex(String keyspace,
			String columnFamily, N whereColumn, V whereValue,
			int numRows, int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {

		long ts1 = System.currentTimeMillis();
		logger.debug("event=read_nextn_rows cf=" + columnFamily + " num_rows=" + numRows + " num_cols=" + numCols);

		try {

			IndexQuery<K, N> query = 
					getKeyspace(keyspace).
					prepareQuery(getColumnFamily(columnFamily)).
					searchWithIndex().
					setRowLimit(numRows);
			
			if (whereColumn != null && whereValue != null) {
				query.addExpression().whereColumn(whereColumn).equals().value(whereValue, getSerializer(valueSerializer));
			}
			
			OperationResult<Rows<K, N>> or = query.execute();
			Rows<K, N> rows = or.getResult();
			return getRows(rows);
		} catch (NotFoundException ex){
			//ignore. 
			return null;
		} catch (ConnectionException ex) {
		
			throw new PersistenceException(ex);

		} finally {

			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
		}
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readRowsByIndices(String keyspace,
			String columnFamily, Map<N, V> columnValues,
			int numRows, int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {
		
		long ts1 = System.currentTimeMillis();
		logger.debug("event=read_nextn_rows cf=" + columnFamily + " num_rows=" + numRows + " num_cols=" + numCols);

		try {

			IndexQuery<K, N> query = getKeyspace(keyspace).
					prepareQuery(getColumnFamily(columnFamily)).
					searchWithIndex().setRowLimit(numRows);
			
			if (columnValues != null) {
				for (N columnName : columnValues.keySet()) {
					query.addExpression().whereColumn(columnName).equals().value(columnValues.get(columnName), getSerializer(valueSerializer));
				}
			}
			
			OperationResult<Rows<K, N>> or = query.execute();
			Rows<K, N> rows = or.getResult();
			return getRows(rows);
		} catch (NotFoundException ex){
			//ignore. 
			return null;
		} catch (ConnectionException ex) {
		
			throw new PersistenceException(ex);

		} finally {

			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
		}
	}

	@Override
	public <K, N, V> CmbColumnSlice<N, V> readColumnSlice(String keyspace,
			String columnFamily, K key, N firstColumnName, N lastColumnName,
			int numCols, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer, CmbSerializer valueSerializer)
			throws PersistenceException {

		long ts1 = System.currentTimeMillis();
		logger.debug("event=read_column_slice cf=" + columnFamily + " key=" + key);

		try {

		    RowQuery<K, N> rq = 
		    		getKeyspace(keyspace).
		    		prepareQuery(getColumnFamily(columnFamily)).
		    		getKey(key).
		    		withColumnRange(getComposite(firstColumnName), getComposite(lastColumnName), false, numCols);
		    ColumnList<N> columns = rq.execute().getResult();
		    
		    if (columns == null || columns.isEmpty()) {
		    	return null;
		    }
			
			return new CmbAstyanaxColumnSlice(columns);
		} catch (NotFoundException ex){
			//ignore. 
			return null;
		} catch (ConnectionException ex) {
		
			throw new PersistenceException(ex);

		} finally {

			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
		}
	}

	@Override
	public <K, N, V> void insertRow(String keyspace, K rowKey, 
			String columnFamily, Map<N, V> columnValues,
			CmbSerializer keySerializer, CmbSerializer nameSerializer,
			CmbSerializer valueSerializer, Integer ttl) throws PersistenceException {
		
		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=insert_row column_family=" + columnFamily + " key=" + rowKey);

		try {
			MutationBatch m = getKeyspace(keyspace).prepareMutationBatch();
			ColumnListMutation<N> clm = m.withRow((ColumnFamily<K, N>)getColumnFamily(columnFamily), rowKey);
			for (N columnName : columnValues.keySet()) {
				clm.putColumn((N)getComposite(columnName), columnValues.get(columnName), getSerializer(valueSerializer), ttl);
				CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
			}
			OperationResult<Void> result = m.execute();
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
		}
	}

	@Override
	public <K, N, V> void insertRows(String keyspace,
			Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			CmbSerializer keySerializer, CmbSerializer nameSerializer,
			CmbSerializer valueSerializer, Integer ttl) throws PersistenceException {

		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=insert_rows column_family=" + columnFamily);

		try {
			MutationBatch m = getKeyspace(keyspace).prepareMutationBatch();
			for (K rowKey : rowColumnValues.keySet()) { 
				ColumnListMutation<N> clm = m.withRow((ColumnFamily<K, N>)getColumnFamily(columnFamily), rowKey);
				for (N columnName : rowColumnValues.get(rowKey).keySet()) {
					clm.putColumn((N)getComposite(columnName), rowColumnValues.get(rowKey).get(columnName), getSerializer(valueSerializer), ttl);
					CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
				}
			}
			OperationResult<Void> result = m.execute();
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
		}
	}

	@Override
	public <K, N> void delete(String keyspace, String columnFamily, K key,
			N column, CmbSerializer keySerializer,
			CmbSerializer columnSerializer) throws PersistenceException {

		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=delete column_family=" + columnFamily + " key=" + key + " column=" + column);

		try {
			MutationBatch m = getKeyspace(keyspace).prepareMutationBatch();
			ColumnListMutation<N> clm = m.withRow((ColumnFamily<K, N>)getColumnFamily(columnFamily), key);
			if (column != null) {
				clm.deleteColumn((N)getComposite(column));
			} else {
				clm.delete();
			}
			OperationResult<Void> result = m.execute();
		} catch (NotFoundException ex){
			//ignore. 
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
		}
	}

	@Override
	public <K, N> void deleteBatch(String keyspace, String columnFamily,
			List<K> keyList, List<N> columnList, CmbSerializer keySerializer,
			CmbSerializer columnSerializer) throws PersistenceException {

		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=delete_batch column_family=" + columnFamily);

		try {
			MutationBatch m = getKeyspace(keyspace).prepareMutationBatch();
			if (columnList == null || columnList.isEmpty()) {
				for (K k : keyList) {
					ColumnListMutation<N> clm = m.withRow((ColumnFamily<K, N>)getColumnFamily(columnFamily), k);
					clm.delete();
					CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
				}
			} else {
				// TODO: review this logic with jane 
				for (int i=0; i< keyList.size();i++) {
					ColumnListMutation<N> clm = m.withRow((ColumnFamily<K, N>)getColumnFamily(columnFamily), keyList.get(i));
					clm.deleteColumn((N)getComposite(columnList.get(i)));
					CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
				}
			}
			OperationResult<Void> result = m.execute();
		} catch (NotFoundException ex){
			//ignore. 
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
		}
	}

	@Override
	public <K, N> int getCount(String keyspace, String columnFamily, K key,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer)
					throws PersistenceException {

		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=increment_counter column_family=" + columnFamily);

		try {

			int count = 
					getKeyspace(keyspace).
					prepareQuery(getColumnFamily(columnFamily)).
					getKey(key).
					getCount().
					execute().
					getResult();
			return count;
		} catch (NotFoundException ex){
			//ignore. 
			return 0;
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
		}
	}

	@Override
	public <K, N> void incrementCounter(String keyspace, String columnFamily,
			K rowKey, String columnName, int incrementBy,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer)
			throws PersistenceException {
		
		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=increment_counter column_family=" + columnFamily);

		try {
			getKeyspace(keyspace).
				prepareColumnMutation(getColumnFamily(columnFamily), rowKey, columnName).
				incrementCounterColumn(incrementBy).
				execute();
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
		}
	}

	@Override
	public <K, N> void decrementCounter(String keyspace, String columnFamily,
			K rowKey, String columnName, int decrementBy,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer)
			throws PersistenceException {

		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=decrement_counter column_family=" + columnFamily);

		try {
			getKeyspace(keyspace).prepareColumnMutation(getColumnFamily(columnFamily), rowKey, columnName)
		    .incrementCounterColumn(-decrementBy)
		    .execute();
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
		}
	}

	@Override
	public <K, N> void deleteCounter(String keyspace, String columnFamily,
			K rowKey, N columnName, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer) throws PersistenceException {
		
		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=delete_counter column_family=" + columnFamily);

		try {
			getKeyspace(keyspace).
				prepareColumnMutation(getColumnFamily(columnFamily),rowKey,columnName).
				deleteCounterColumn().execute();
		} catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
		}
	}

	@Override
	public <K, N> long getCounter(String keyspace, String columnFamily,
			K rowKey, N columnName, CmbSerializer keySerializer,
			CmbSerializer columnNameSerializer) throws PersistenceException {
		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=get_counter column_family=" + columnFamily);
		try {
			Column<N> result = 
					getKeyspace(keyspace).
					prepareQuery((ColumnFamily<K, N>)getColumnFamily(columnFamily)).
					getKey(rowKey).
					getColumn(columnName).
					execute().
					getResult();
			Long counterValue = result.getLongValue();
			return counterValue;
		} catch (ConnectionException ex) {
			return 0;
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
		}
	}

	@Override
	public <K, N, V> CmbColumn<N, V> readColumn(String keyspace,
			String columnFamily, K key, N columnName,
			CmbSerializer keySerializer, CmbSerializer columnNameSerializer,
			CmbSerializer valueSerializer) throws PersistenceException {
		long ts1 = System.currentTimeMillis();	    
		logger.debug("event=get_column column_family=" + columnFamily + " column_name=" + columnName);
		try {
			Column<N> column = 
					(Column<N>)getKeyspace(keyspace).
					prepareQuery(getColumnFamily(columnFamily)).
					getKey(key).
					getColumn(getComposite(columnName)).
					execute().
					getResult();
			return new CmbAstyanaxColumn(column);
		} catch (NotFoundException ex){
			//ignore. This might happen when C* data expired.
			return null;
		}
		catch (ConnectionException ex) {
			throw new PersistenceException(ex);
		} finally {
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
		}
	}
}
