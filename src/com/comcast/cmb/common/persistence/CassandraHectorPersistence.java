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
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import me.prettyprint.cassandra.connection.DynamicLoadBalancingPolicy;
import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.connection.RoundRobinBalancingPolicy;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.model.MultigetCountQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.SuperCfTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperRows;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.persistence.AbstractCassandraPersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.AbstractCassandraPersistence.CmbColumn;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;

/**
 * This class represents generic functionality for all Cassandra persistence objects
 * @author aseem, bwolf, vvenkatraman, jorge, baosen, michael
 */
public class CassandraHectorPersistence extends AbstractCassandraPersistence {
	
	//TODO: surround everything with try-finally
	
	private static final int hectorPoolSize = CMBProperties.getInstance().getHectorPoolSize();
	private static final String hectorBalancingPolicy = CMBProperties.getInstance().getHectorBalancingPolicy();
	private static final Map<String, String> credentials = CMBProperties.getInstance().getHectorCredentials();
	
	protected String keyspaceName = CMBProperties.getInstance().getCMBKeyspace();

	protected Cluster cluster;
	protected Map<String, Keyspace> keyspaces;
	
	private static Logger logger = Logger.getLogger(CassandraHectorPersistence.class);
	
	private static class SimpleConsistencyPolicy implements ConsistencyLevelPolicy {
		
		private final HConsistencyLevel level;
	
		public SimpleConsistencyPolicy(HConsistencyLevel l) {
			level = l;
		}
		
		@Override
		public HConsistencyLevel get(OperationType arg0, String arg1) {
			return level;
		}
		
		@Override
		public HConsistencyLevel get(OperationType arg0) {
			return level;
		}
	}
	
	/**
	 * Initialize the internal handlers to hector. Should be called only once in the begenning
	 */
	private void initPersistence() {
		
	    long ts1 = System.currentTimeMillis();
	    
	    CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();
	    
	    cassandraHostConfigurator.setHosts(CLUSTER_URL);
	    cassandraHostConfigurator.setMaxActive(hectorPoolSize);
	    cassandraHostConfigurator.setCassandraThriftSocketTimeout(CMBProperties.getInstance().getCassandraThriftSocketTimeOutMS());
	    
	    cassandraHostConfigurator.setAutoDiscoverHosts(CMBProperties.getInstance().isHectorAutoDiscovery());
	    cassandraHostConfigurator.setAutoDiscoveryDelayInSeconds(CMBProperties.getInstance().getHectorAutoDiscoveryDelaySeconds());
	    
	    String dataCenter = CMBProperties.getInstance().getHectorAutoDiscoveryDataCenter();
	    
	    if (dataCenter != null && !dataCenter.equals("")) {
	    	cassandraHostConfigurator.setAutoDiscoveryDataCenter(dataCenter);
	    }

	    // some other settings we may be interested in down the road, see here for more details:
	    // https://github.com/rantav/hector/wiki/User-Guide
	    
	    if (hectorBalancingPolicy != null) {
	    	if (hectorBalancingPolicy.equals("LeastActiveBalancingPolicy")) {
	    		cassandraHostConfigurator.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
	    	} else if (hectorBalancingPolicy.equals("RoundRobinBalancingPolicy")) {
	    		cassandraHostConfigurator.setLoadBalancingPolicy(new RoundRobinBalancingPolicy()); //default
	    	} else if (hectorBalancingPolicy.equals("DynamicLoadBalancingPolicy")) {
	    		cassandraHostConfigurator.setLoadBalancingPolicy(new DynamicLoadBalancingPolicy());
	    	}
	    }
	    
	    //cassandraHostConfigurator.setExhaustedPolicy(ExhaustedPolicy.WHEN_EXHAUSTED_GROW);
	    
	    cluster = HFactory.getOrCreateCluster(AbstractCassandraPersistence.CLUSTER_NAME, cassandraHostConfigurator, credentials);
		keyspaces = new HashMap<String, Keyspace>();

		List<String> keyspaceNames = new ArrayList<String>();
		keyspaceNames.add(CMBProperties.getInstance().getCMBKeyspace());
		keyspaceNames.add(CMBProperties.getInstance().getCNSKeyspace());
		keyspaceNames.add(CMBProperties.getInstance().getCQSKeyspace());
		
		//TODO: for now back to one consistency level for everything
		
		for (String k : keyspaceNames) {
			Keyspace keyspace = HFactory.createKeyspace(k, cluster, new SimpleConsistencyPolicy(CMBProperties.getInstance().getWriteConsistencyLevel()));
			keyspaces.put(k, keyspace);
		}
		
		long ts2 = System.currentTimeMillis();
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
	}
	
	/* (non-Javadoc)
	 * @see com.comcast.cmb.common.persistence.IPersistence#isAlive()
	 */
	@Override
	public boolean isAlive() {
		
		boolean alive = true;
		
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
		List<String> names = new ArrayList<String>();
		
		for (KeyspaceDefinition k : keyspaces) {
			names.add(k.getName());
		}
		
		alive &= names.contains(CMBProperties.getInstance().getCQSKeyspace());
		alive &= names.contains(CMBProperties.getInstance().getCNSKeyspace());
		alive &= names.contains(CMBProperties.getInstance().getCMBKeyspace());
		
		return alive;
	}
	
	private Keyspace getKeyspace(String keyspace) {
		return keyspaces.get(keyspace);
	}
	
	private static Serializer getSerializer(CMB_SERIALIZER s) throws Exception {
		if (s == CMB_SERIALIZER.STRING_SERIALIZER) {
			return StringSerializer.get();
		}
		throw new CMBException(CMBErrorCodes.InternalError, "Unknown serializer " + s);
	}
	
	private <K, N, V> List<CmbRow<K, N, V>> getRows(List<Row<K, N, V>> rows) throws Exception {
		
		List<CmbRow<K, N, V>> l = new ArrayList<CmbRow<K, N, V>>();
		
		for (Row<K, N, V> r : rows) {
			List<CmbColumn<N, V>> cmbColumns = new ArrayList<CmbColumn<N, V>>();
			ColumnSlice<N, V> cs = r.getColumnSlice();
			List<HColumn<N, V>> cl = cs.getColumns();
			for (HColumn<N, V> hc : cl) {
				cmbColumns.add(new CmbColumn<N, V>(hc.getName(), hc.getValue()));
			}
			l.add(new CmbRow<K, N, V>(r.getKey(), cmbColumns));
		}
		
		return l;
	}
	
	private <N, V> CmbColumnSlice<N, V> getSlice(ColumnSlice<N, V> slice) throws Exception  {
		List<CmbColumn<N, V>> cl = new ArrayList<CmbColumn<N, V>>();
		for (HColumn<N, V> s : slice.getColumns()) {
			cl.add(new CmbColumn<N, V>(s.getName(), s.getValue()));
		}
		return new CmbColumnSlice<N,V>(cl);
	}
	
	private <SN, N, V> CmbSuperColumn<SN, N, V> getSuperColumn(HSuperColumn<SN, N, V> superColumn) throws Exception {
		List<CmbColumn<N, V>> columns = new ArrayList<CmbColumn<N, V>>();
		for (HColumn<N, V> c : superColumn.getColumns()) {
			columns.add(new CmbColumn<N, V>(c.getName(), c.getValue()));
		}
		return new CmbSuperColumn<SN, N, V>(superColumn.getName(), columns);
	}
	
	private <SN, N, V> List<CmbSuperColumn<SN, N, V>> getSuperColumns(List<HSuperColumn<SN, N, V>> superColumns) throws Exception {
		List<CmbSuperColumn<SN, N, V>> l = new ArrayList<CmbSuperColumn<SN, N, V>>();
		for (HSuperColumn<SN, N, V> superColumn : superColumns) {
			List<CmbColumn<N, V>> columns = new ArrayList<CmbColumn<N, V>>();
			for (HColumn<N, V> c : superColumn.getColumns()) {
				columns.add(new CmbColumn<N, V>(c.getName(), c.getValue()));
			}
			l.add(new CmbSuperColumn<SN, N, V>(superColumn.getName(), columns));
		}
		return l;
	}

	private <SN, N, V> CmbSuperColumnSlice<SN, N, V> getSuperColumnSlice(SuperSlice<SN, N, V> superSlice) throws Exception {
		List<CmbSuperColumn<SN, N, V>> l = new ArrayList<CmbSuperColumn<SN, N, V>>();
		for (HSuperColumn<SN, N, V> sc : superSlice.getSuperColumns()) {
			l.add(getSuperColumn(sc));
		}
		return new CmbSuperColumnSlice<SN, N, V>(l);
	}

	@Override
	public <K, N, V> void update(String keyspace, String columnFamily, K key, N column, V value, 
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER nameSerializer, CMB_SERIALIZER valueSerializer) throws Exception {
		long ts1 = System.currentTimeMillis();	    
        logger.debug("event=update column_family=" + columnFamily + " key=" + key + " column=" + column + " value=" + value);
		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
		mutator.addInsertion(key, columnFamily, HFactory.createColumn(column, value, getSerializer(nameSerializer), getSerializer(valueSerializer)));
		mutator.execute();
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
	}

	@Override
	public <K, SN, N, V> void insertSuperColumn(String keyspace, String columnFamily, K key, CMB_SERIALIZER keySerializer, SN superName, Integer ttl, 
			CMB_SERIALIZER superNameSerializer, Map<N, V> subColumnNameValues, CMB_SERIALIZER columnSerializer,
			CMB_SERIALIZER valueSerializer) throws Exception {
	    
		long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=insert_super_column key=" + key + " cf=" + columnFamily + " super_name=" + superName + " ttl=" + (ttl == null ? "null" : ttl) + " sub_column_values=" + subColumnNameValues);

		List<HColumn<N, V>> subColumns = new ArrayList<HColumn<N, V>>();
		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
		
		for (N name : subColumnNameValues.keySet()) {
			
			V value = subColumnNameValues.get(name);
			HColumn<N, V> subColumn = HFactory.createColumn(name, value, getSerializer(columnSerializer), getSerializer(valueSerializer));
			
			if (ttl != null) {
			    subColumn.setTtl(ttl);
			}
			
			subColumns.add(subColumn);
		}
		
		HSuperColumn<SN, N, V> superColumn = HFactory.createSuperColumn(superName, subColumns, System.currentTimeMillis(), getSerializer(superNameSerializer), getSerializer(columnSerializer), getSerializer(valueSerializer));
		mutator.insert(key, columnFamily, superColumn);
		
		long ts2 = System.currentTimeMillis();
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
	}

	@Override
	public <K, SN, N, V> void insertSuperColumns(String keyspace, String columnFamily, K key, CMB_SERIALIZER keySerializer,
			Map<SN, Map<N, V>> superNameSubColumnsMap, int ttl,
			CMB_SERIALIZER superNameSerializer, CMB_SERIALIZER columnSerializer,
			CMB_SERIALIZER valueSerializer)
			throws Exception {
		
	    long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=insert_super_columns cf=" + columnFamily + " columns=" + superNameSubColumnsMap);
	    
		List<HColumn<N, V>> subColumns = new ArrayList<HColumn<N, V>>();
		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
		
		for (SN superName : superNameSubColumnsMap.keySet()) {
			
			Map<N, V> subColumnsMap = superNameSubColumnsMap.get(superName);
			
			if (subColumnsMap != null) {
				
				subColumns.clear();
				
				for (N name : subColumnsMap.keySet()) {
					V value = subColumnsMap.get(name);
					HColumn<N, V> subColumn = HFactory.createColumn(name, value, getSerializer(columnSerializer), getSerializer(valueSerializer));
					subColumn.setTtl(ttl);
					subColumns.add(subColumn);
				}
				
				HSuperColumn<SN, N, V> superColumn = HFactory.createSuperColumn(superName, subColumns, System.currentTimeMillis(), getSerializer(superNameSerializer), getSerializer(columnSerializer),	getSerializer(valueSerializer));
				mutator.addInsertion(key, columnFamily, superColumn);
				superColumn = null;
			}
		}
		
		mutator.execute();
		
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, superNameSubColumnsMap.size());
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNNonEmptyRows(String keyspace, String columnFamily, K lastKey, int numRows, int numCols,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer,
			CMB_SERIALIZER valueSerializer) throws Exception {
	    
	    long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=read_nextn_nonempty_rows cf=" + columnFamily + " last_key=" + lastKey + " num_rows=" + numRows + " num_cols" + numCols);
	    
	    try {
	    	
	        int pageSize = 100;

	        List<Row<K, N, V>> rows = new ArrayList<Row<K, N, V>>();
	        RangeSlicesQuery<K, N, V> rangeSlicesQuery;

	        // page through rows in increments of 100 until the desired number of
	        // rows is found

	        while (true) {

	            rangeSlicesQuery = HFactory.createRangeSlicesQuery(getKeyspace(keyspace), getSerializer(keySerializer),
	            		getSerializer(columnNameSerializer), getSerializer(valueSerializer))
	                    .setColumnFamily(columnFamily)
	                    .setRange(null, null, false, numCols).setRowCount(pageSize)
	                    .setKeys(lastKey, null);

	            QueryResult<OrderedRows<K, N, V>> result = rangeSlicesQuery.execute();
	            
	            CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
	            
	            OrderedRows<K, N, V> orderedRows = result.get();
	            Iterator<Row<K, N, V>> rowsIterator = orderedRows.iterator();

	            // skip last row

	            if (lastKey != null && rowsIterator.hasNext()) {
	                rowsIterator.next();
	            }

	            // return if there are no more rows in cassandra

	            if (!rowsIterator.hasNext()) {
	                return getRows(rows);
	            }

	            while (rowsIterator.hasNext()) {

	                Row<K, N, V> row = rowsIterator.next();

	                lastKey = row.getKey();

	                if (row.getColumnSlice().getColumns().isEmpty()) {
	                    continue;
	                }

	                rows.add(row);

	                // return if we have the desired number of rows

	                if (rows.size() >= numRows) {
	                    return getRows(rows);
	                }
	            }
	        }
	        
	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace, String columnFamily, K lastKey, int numRows, int numCols,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer,
			CMB_SERIALIZER valueSerializer) throws Exception {

	    long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=read_nextn_rows cf=" + columnFamily + " last_key=" + lastKey + " num_rows=" + numRows + " num_cols=" + numCols);
	    
		List<Row<K, N, V>> rows = new ArrayList<Row<K, N, V>>();

		RangeSlicesQuery<K, N, V> rangeSlicesQuery = HFactory.createRangeSlicesQuery(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(columnNameSerializer), getSerializer(valueSerializer))
				.setColumnFamily(columnFamily)
				.setRange(null, null, false, numCols).setRowCount(numRows)
				.setKeys(lastKey, null);

		QueryResult<OrderedRows<K, N, V>> result = rangeSlicesQuery.execute();
		
		OrderedRows<K, N, V> orderedRows = result.get();
		Iterator<Row<K, N, V>> rowsIterator = orderedRows.iterator();

		if (lastKey != null && rowsIterator != null && rowsIterator.hasNext()) {
			rowsIterator.next();
		}

		while (rowsIterator.hasNext()) {

			Row<K, N, V> row = rowsIterator.next();

			if (row.getColumnSlice().getColumns().isEmpty()) {
				continue;
			}

			rows.add(row);
		}

		long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

		return getRows(rows);
	}


	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace, String columnFamily, K lastKey, N whereColumn, V whereValue,
			int numRows, int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception {

		Map<N, V> columnValues = new HashMap<N, V>();
		columnValues.put(whereColumn, whereValue);

		return readNextNRows(keyspace, columnFamily, lastKey, columnValues, numRows, numCols, keySerializer, columnNameSerializer, valueSerializer);
	}

	@Override
	public <K, N, V> List<CmbRow<K, N, V>> readNextNRows(String keyspace, String columnFamily, K lastKey, Map<N, V> columnValues,
			int numRows, int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception {

        long ts1 = System.currentTimeMillis();
        
        logger.debug("event=read_nextn_rows cf=" + columnFamily + " last_key=" + lastKey + " num_rows=" + numRows + " num_cols=" + numCols + " values=" + columnValues);

		List<Row<K, N, V>> rows = new ArrayList<Row<K, N, V>>();

		IndexedSlicesQuery<K, N, V> indexedSlicesQuery = HFactory.createIndexedSlicesQuery(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(columnNameSerializer), getSerializer(valueSerializer))
				.setColumnFamily(columnFamily)
				.setRange(null, null, false, numCols).setRowCount(numRows)
				.setStartKey(lastKey);

		for (N key : columnValues.keySet()) {
			indexedSlicesQuery.addEqualsExpression(key, columnValues.get(key));
		}

		QueryResult<OrderedRows<K, N, V>> result = indexedSlicesQuery.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

		OrderedRows<K, N, V> orderedRows = result.get();
		Iterator<Row<K, N, V>> rowsIterator = orderedRows.iterator();

		if (lastKey != null && rowsIterator != null && rowsIterator.hasNext()) {
			rowsIterator.next();
		}

		while (rowsIterator.hasNext()) {

			Row<K, N, V> row = rowsIterator.next();

			if (row.getColumnSlice().getColumns().isEmpty()) {
				continue;
			}

			rows.add(row);
		}

        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      

		return getRows(rows);
	}

	@Override
	public <K, N, V> CmbColumnSlice<N, V> readColumnSlice(String keyspace, String columnFamily, K key, N firstColumnName, N lastColumnName,
			int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception {
        
		long ts1 = System.currentTimeMillis();
		
		logger.debug("event=read_row cf=" + columnFamily + " key=" + key + " first_col=" + firstColumnName + " last_col=" + lastColumnName + " num_cols=" + numCols);

		SliceQuery<K, N, V> sliceQuery = HFactory.createSliceQuery(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(columnNameSerializer), getSerializer(valueSerializer))
				.setColumnFamily(columnFamily)
				.setRange(firstColumnName, lastColumnName, false, numCols)
				.setKey(key);
		
		QueryResult<ColumnSlice<N, V>> result = sliceQuery.execute();
		ColumnSlice<N, V> slice = result.get();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      

        if (slice == null || slice.getColumns() == null || slice.getColumns().isEmpty()) {
			return null;
		}
		
		return getSlice(slice);

		/*RangeSlicesQuery<K, N, V> rangeSliceQuery = HFactory.createRangeSlicesQuery(keyspace, keySerializer, columnNameSerializer, valueSerializer)
		.setColumnFamily(columnFamily)
		.setKeys(key, key)
		.setRange(firstColumnName, lastColumnName, false, numCols);
		//.setRowCount(1);

		OrderedRows<K, N, V> orderedRows = rangeSliceQuery.execute().get();
		
        Iterator<Row<K, N, V>> rowsIterator = orderedRows.iterator();
        Row<K, N, V> row = null;
        
        if (rowsIterator.hasNext()) {
                row = rowsIterator.next();
                // Venu 02/07/12: Checking for null rows or null column slice.
                if (row == null || row.getColumnSlice() == null
                                || row.getColumnSlice().getColumns() == null
                                || row.getColumnSlice().getColumns().isEmpty()) {
                        return null;
                }
        }
        
    	return row.getColumnSlice();*/
	}

	@Override
	public <K, SN, N, V> CmbSuperColumnSlice<SN, N, V> readRowFromSuperColumnFamily(String keyspace, String columnFamily, K key, SN firstColumnName, SN lastColumnName,
			int numCols, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception {

	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=read_row_from_super_column_family cf=" + columnFamily + "key=" + key  +" first_column=" + firstColumnName + " last_column_name=" + lastColumnName + " num_cols=" + numCols);

	    try {
	        
	        SuperSliceQuery<K, SN, N, V> rangeSlicesQuery = HFactory.createSuperSliceQuery(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(superNameSerializer), getSerializer(columnNameSerializer), getSerializer(valueSerializer))
	        		.setColumnFamily(columnFamily)
	                .setRange(firstColumnName, lastColumnName, false, numCols)
	                .setKey(key);

	        QueryResult<SuperSlice<SN, N, V>> result = rangeSlicesQuery.execute();

	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
	        
	        SuperSlice<SN, N, V> superSlice = result.get();
	        
	        if (superSlice.getSuperColumns() == null || superSlice.getSuperColumns().size() == 0) {
	            return null;
	        }
	        
	        return getSuperColumnSlice(superSlice);
	        
	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }
	}
	
	@Override
	public <K, SN, N, V> CmbSuperColumn<SN, N, V> readColumnFromSuperColumnFamily(String keyspace, String columnFamily, K key, SN columnName,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception {

	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=read_column_from_super_column_family cf=" + columnFamily + " key=" + key + "column_name=" + columnName);

        try {
	       
	        SuperColumnQuery<K, SN, N, V> superColumnQuery = HFactory.createSuperColumnQuery(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(superNameSerializer), getSerializer(columnNameSerializer), getSerializer(valueSerializer))
	        		.setColumnFamily(columnFamily)
	                .setSuperName(columnName)
	                .setKey(key);

	        QueryResult<HSuperColumn<SN, N, V>> result = superColumnQuery.execute();

	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
	        
	        HSuperColumn<SN, N, V> superColumn = result.get();
	        
	        if (superColumn == null) {
	            return null;
	        }
	        
	        return getSuperColumn(superColumn);
	        
	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }
	}

	@Override
	public <K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readMultipleColumnsFromSuperColumnFamily(String keyspace, String columnFamily, Collection<K> keys,
			Collection<SN> columnNames, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer) throws Exception {
		
		List<HSuperColumn<SN, N, V>> list = new ArrayList<HSuperColumn<SN, N, V>>();

	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=read_column_from_super_column_family cf=" + columnFamily + " key_count=" + keys.size() + "column_count=" + columnNames.size());

        try {
	       
	    	MultigetSuperSliceQuery<K, SN, N, V> query = HFactory.createMultigetSuperSliceQuery(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(superNameSerializer), getSerializer(columnNameSerializer), getSerializer(valueSerializer))
	    		.setColumnFamily(columnFamily)
	    		.setColumnNames(columnNames)
	    		.setKeys(keys);

	        QueryResult<SuperRows<K, SN, N, V>> result = query.execute();
	        SuperRows<K, SN, N, V> rows = result.get();
	        Iterator<SuperRow<K, SN, N, V>> iter = rows.iterator();
	        
	        while (iter.hasNext()) {
	        	SuperRow<K, SN, N, V> row = iter.next();
	        	SuperSlice<SN, N, V> slice = row.getSuperSlice();
	        	List<HSuperColumn<SN, N, V>> columns = slice.getSuperColumns();
	        	list.addAll(columns);
	        }

	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
	        
	        return getSuperColumns(list);
	        
	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }
	}

	@Override
	public 	<K, SN, N, V> List<CmbSuperColumn<SN, N, V>> readColumnsFromSuperColumnFamily(String keyspace, String columnFamily, K key, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER superNameSerializer,
			CMB_SERIALIZER columnNameSerializer, CMB_SERIALIZER valueSerializer,
			 SN firstCol, SN lastCol, int numCol) throws Exception {
		
	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=read_columns_from_super_column_family cf=" + columnFamily + " key=" + key);

        try {
	    	
	        SuperSliceQuery<K, SN, N, V> superSliceQuery = HFactory.createSuperSliceQuery(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(superNameSerializer), getSerializer(columnNameSerializer), getSerializer(valueSerializer))
	        .setColumnFamily(columnFamily)
	        .setKey(key)
	        .setRange(firstCol, lastCol, false, numCol);

	        QueryResult<SuperSlice<SN, N, V>> result = superSliceQuery.execute();

	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

	        return getSuperColumns(result.get().getSuperColumns());

	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }	    
	}

	@Override
	public <K, N, V> void insertRow(K rowKey,
			String keyspace, String columnFamily, Map<N, V> columnValues,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER nameSerializer,
			CMB_SERIALIZER valueSerializer, Integer ttl) throws Exception {

	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=insert_row key=" + rowKey + " cf=" + columnFamily + " ttl=" + (ttl == null ? "null" : ttl));
	    
		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));

		for (N key : columnValues.keySet()) {
			
			HColumn<N, V> col = HFactory.createColumn(key, columnValues.get(key), getSerializer(nameSerializer), getSerializer(valueSerializer));
			
			if (ttl != null) {
				col.setTtl(ttl);
			}
			
			mutator.addInsertion(rowKey, columnFamily, col);
		}

		MutationResult result = mutator.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);

		long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}

	@Override
	public <K, N, V> void insertRows(String keyspace, Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER nameSerializer,
			CMB_SERIALIZER valueSerializer, Integer ttl) throws Exception {

	    long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=insert_rows row_column_values=" + rowColumnValues + " cf=" + columnFamily + " ttl=" + (ttl == null ? "null" : ttl));

		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));

		for (K rowKey : rowColumnValues.keySet()) {
			
			Map<N, V> columnValues = rowColumnValues.get(rowKey);
			
			for (N key : columnValues.keySet()) {
				
				HColumn<N, V> col = HFactory.createColumn(key, columnValues.get(key), getSerializer(nameSerializer), getSerializer(valueSerializer));
				
				if (ttl != null) {
					col.setTtl(ttl);
				}
				
				mutator.addInsertion(rowKey, columnFamily, col);
			}
		}

		MutationResult result = mutator.execute();

		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);

		long ts2 = System.currentTimeMillis();
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}

	/**
	 * Read rows by CQL query. Consider using readNextNRows() instead.
	 * 
	 * @param queryString
	 *            CQL query string
	 * @param level
	 *            consistency level
	 * @return query result
	 */
	/*public QueryResult<CqlRows<String, String, String>> readRows(String queryString, HConsistencyLevel level) {
	    long ts1 = System.currentTimeMillis();
	    logger.debug("event=read_rows query=" + queryString);
		Keyspace keyspace = keyspaces.get(level);
		CqlQuery<String, String, String> query = new CqlQuery<String, String, String>(keyspace, new StringSerializer(), new StringSerializer(),	new StringSerializer());
		query.setQuery(queryString);
		QueryResult<CqlRows<String, String, String>> res = query.execute();
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
		long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
		return res;
	}*/

	@Override
	public <K, N> void delete(String keyspace, String columnFamily, K key, N column, 
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnSerializer) throws Exception {
		
	    long ts1 = System.currentTimeMillis();
        logger.debug("event=delete key=" + key + " column=" + column + " cf=" + columnFamily);
		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));

		if (column != null) {
			mutator.addDeletion(key, columnFamily, column, getSerializer(columnSerializer));
		} else {
			mutator.addDeletion(key, columnFamily);
		}
		
		mutator.execute();
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}

	@Override
	public <K, N> void deleteBatch(String keyspace, String columnFamily,
			List<K> keyList, List<N> columnList, CMB_SERIALIZER keySerializer,
			 CMB_SERIALIZER columnSerializer) throws Exception {
		
        long ts1 = System.currentTimeMillis();
        
		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
	
		if (columnList==null ||columnList.isEmpty()) {
			for (int i=0; i< keyList.size();i++) {
				mutator.addDeletion(keyList.get(i), columnFamily);
			}			
		} else {
			for (int i=0; i< keyList.size();i++) {
				mutator.addDeletion(keyList.get(i), columnFamily, columnList.get(i), getSerializer(columnSerializer));
			}
		}
		
		mutator.execute();
			
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}

	@Override
	public <K, SN, N> void deleteSuperColumn(String keyspace, String superColumnFamily, K key, SN superColumn, CMB_SERIALIZER keySerializer, CMB_SERIALIZER superColumnSerializer) throws Exception {
		
	    long ts1 = System.currentTimeMillis();
        logger.debug("event=delete key=" + key + " super_column=" + superColumn + " cf=" + superColumnFamily);
		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));

		if (superColumn != null) {
			mutator.addSuperDelete(key, superColumnFamily, superColumn, getSerializer(superColumnSerializer));
		} else {
			mutator.addSuperDelete(key, superColumnFamily, null, null);
		}
		
		mutator.execute();
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}
	
	@Override
	public <K, N> int getCount(String keyspace, String columnFamily, K key,
			CMB_SERIALIZER keySerializer, CMB_SERIALIZER columnNameSerializer) throws Exception {

		long ts1 = System.currentTimeMillis();

		logger.debug("event=get_count cf=" + columnFamily + " key=" + key);

		@SuppressWarnings("unchecked")
		MultigetCountQuery<K, N> query = new MultigetCountQuery<K, N>(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(columnNameSerializer))
		.setColumnFamily(columnFamily).setKeys(key)
		.setRange(null, null, 2000000000);

		QueryResult<Map<K, Integer>> result = query.execute();

		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

		Map<K, Integer> resultRow = result.get();

		if (resultRow.containsKey(key)) {

			int count = resultRow.get(key);
			long ts2 = System.currentTimeMillis();
			CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
			return count;
		}

		return -1;
	}
	
	@Override
	public <K, N> void incrementCounter(String keyspace, String columnFamily, K rowKey,
			String columnName, int incrementBy, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception {

		logger.debug("event=increment_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName + " inc=" + incrementBy);
		
		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
		mutator.incrementCounter(rowKey, columnFamily, columnName, incrementBy);
		mutator.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
	}

	@Override
	public <K, N> void decrementCounter(String keyspace, String columnFamily, K rowKey,
			String columnName, int decrementBy, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception {
        
		long ts1 = System.currentTimeMillis();

		logger.debug("event=decrement_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName + " dec=" + decrementBy);

		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
		mutator.decrementCounter(rowKey, columnFamily, columnName, decrementBy);
		mutator.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}
	
	@Override
	public <K, N> void deleteCounter(String keyspace, String columnFamily, K rowKey,
			N columnName, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception {
        
		long ts1 = System.currentTimeMillis();

		logger.debug("event=decrement_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName);

		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
		mutator.deleteCounter(rowKey, columnFamily, columnName, getSerializer(columnNameSerializer));
		mutator.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}

	@Override
	public <K, N> long getCounter(String keyspace, String columnFamily, K rowKey,
			N columnName, CMB_SERIALIZER keySerializer,
			CMB_SERIALIZER columnNameSerializer) throws Exception {
		
        long ts1 = System.currentTimeMillis();
	    logger.debug("event=get_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName);
        
        try {
	    	
	        CounterQuery<K, N> countQuery = HFactory.createCounterColumnQuery(getKeyspace(keyspace), getSerializer(keySerializer), getSerializer(columnNameSerializer));
	        countQuery.setColumnFamily(columnFamily).setKey(rowKey).setName(columnName);
	        QueryResult<HCounterColumn<N>> result = countQuery.execute();
	        
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
	        
	        if (result.get() == null) {
	            return 0;
	        }

	        return result.get().getValue();
	        
	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }
	}
}
