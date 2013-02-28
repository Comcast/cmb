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
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
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
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.ReceiptModule;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;


/**
 * This class represents generic functionality for all Cassandra persistence objects
 * @author aseem, bwolf, vvenkatraman, jorge, baosen, michael
 */
public class CassandraPersistence {
	/**
	 * To support varying level of consistency levels for each query, we must
	 * hold on to a keyspace instance per consistency-level.
	 * 
	 * Class is not thread-safe. It should be confined to use within a
	 * single-thread
	 */
	private static final String clusterName = CMBProperties.getInstance().getClusterName();
	private static final String clusterUrl = CMBProperties.getInstance().getClusterUrl();
	private static final int hectorPoolSize = CMBProperties.getInstance().getHectorPoolSize();
	private static final String hectorBalancingPolicy = CMBProperties.getInstance().getHectorBalancingPolicy();
	
	protected String keyspaceName = CMBProperties.getInstance().getCMBCommonKeyspace();

	protected Cluster cluster;
	protected Map<HConsistencyLevel, Keyspace> keyspaces;
	
	protected static Random random = new Random();
	
	private static Logger logger = Logger.getLogger(CassandraPersistence.class);
	private static long counter = 0;
	
	/**
	 * Returns the same consistency-level as passed in the constructor
	 */
	class SimpleConsistencyPolicy implements ConsistencyLevelPolicy {
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
	
	public CassandraPersistence(String keyspaceName) {
		this.keyspaceName = keyspaceName;
		initPersistence();		
	}

	/**
	 * Initialize the internal handlers to hector. Should be called only once in the begenning
	 */
	private void initPersistence() {
		
	    long ts1 = System.currentTimeMillis();
	    
	    CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();
	    
	    cassandraHostConfigurator.setHosts(clusterUrl);
	    cassandraHostConfigurator.setMaxActive(hectorPoolSize);
	    cassandraHostConfigurator.setCassandraThriftSocketTimeout(CMBProperties.getInstance().getCassandraThriftSocketTimeOutMS());

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
	    
		cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator); 
		keyspaces = new HashMap<HConsistencyLevel, Keyspace>();
		
		for (HConsistencyLevel level : HConsistencyLevel.values()) {
			Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster, new SimpleConsistencyPolicy(level));
			keyspaces.put(level, keyspace);
		}
		
		long ts2 = System.currentTimeMillis();
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
	}
	
	public boolean isAlive() {
		
		boolean alive = true;
		
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
		List<String> names = new ArrayList<String>();
		
		for (KeyspaceDefinition k : keyspaces) {
			names.add(k.getName());
		}
		
		alive &= names.contains(CMBProperties.getInstance().getCMBCQSKeyspace());
		alive &= names.contains(CMBProperties.getInstance().getCMBCNSKeyspace());
		alive &= names.contains(CMBProperties.getInstance().getCMBCommonKeyspace());
		
		return alive;
	}
	
	public Keyspace getKeySpace(HConsistencyLevel consistencyLevel) {
		return keyspaces.get(consistencyLevel);
	}

	/**
	 * Update single key value pair in column family.
	 * 
	 * @param template
	 *            column family
	 * @param key
	 *            row key
	 * @param column
	 *            column name
	 * @param value
	 *            value K - type of row-key N - type of column name V - type of
	 *            column value
	 * @throws HectorException
	 */
	public <K, N, V> void update(ColumnFamilyTemplate<K, N> template, K key, N column, V value, Serializer<K> keySerializer, Serializer<N> nameSerializer, Serializer<V> valueSerializer) throws HectorException {
        
		long ts1 = System.currentTimeMillis();	    
		
        logger.debug("event=update column_family=" + template.getColumnFamily() + " key=" + key + " column=" + column + " value=" + value);
		
		ColumnFamilyUpdater<K, N> updater = template.createUpdater(key);
		updater.setValue(column, value, valueSerializer);
		template.update(updater);
		
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
	}

	/**
	 * Insert single key value pair for a super column.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param key
	 *            row key
	 * @param keySerializer
	 *            The serializer for the key
	 * @param superName
	 *            super column name
	 * @param ttl
	 *            The time to live
	 * @param superNameSerializer
	 *            serializer for the super column name
	 * @param subColumnNameValues
	 *            name, value pair for the sub columns
	 * @param columnSerializer
	 *            the serializer for sub column name
	 * @param valueSerializer
	 *            the serializer for sub column value K - type of row-key SN -
	 *            type of super column name N - type of sub column name V - type
	 *            of column value
	 * @throws HectorException
	 */
	public <K, SN, N, V> MutationResult insertSuperColumn(String columnFamily, K key, Serializer<K> keySerializer, SN superName, Integer ttl, 
			Serializer<SN> superNameSerializer, Map<N, V> subColumnNameValues, Serializer<N> columnSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) throws HectorException {
	    
		long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=insert_super_column key=" + key + " cf=" + columnFamily + " super_name=" + superName + " ttl=" + (ttl == null ? "null" : ttl) + " sub_column_values=" + subColumnNameValues);

		List<HColumn<N, V>> subColumns = new ArrayList<HColumn<N, V>>();
		Mutator<K> mutator = HFactory.createMutator(keyspaces.get(level), keySerializer);
		
		for (N name : subColumnNameValues.keySet()) {
			
			V value = subColumnNameValues.get(name);
			HColumn<N, V> subColumn = HFactory.createColumn(name, value, columnSerializer, valueSerializer);
			
			if (ttl != null) {
			    subColumn.setTtl(ttl);
			}
			
			subColumns.add(subColumn);
		}
		
		HSuperColumn<SN, N, V> superColumn = HFactory.createSuperColumn(superName, subColumns, Calendar.getInstance().getTimeInMillis(), superNameSerializer, columnSerializer, valueSerializer);
		MutationResult result = mutator.insert(key, columnFamily, superColumn);
		
		long ts2 = System.currentTimeMillis();
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);

		return result;
	}

	/**
	 * Insert a map of key value pairs for a super column.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param key
	 *            row key
	 * @param keySerializer
	 *            The serializer for the key
	 * @param superNameSubColumnsMap
	 *            A map of super column names as the key and a map of sub-column
	 *            name values as the value
	 * @param ttl
	 *            The time to live
	 * @param superNameSerializer
	 *            serializer for the super column name
	 * @param subColumnNameValues
	 *            name, value pair for the sub columns
	 * @param columnSerializer
	 *            the serializer for sub column name
	 * @param valueSerializer
	 *            the serializer for sub column value K - type of row-key SN -
	 *            type of super column name N - type of sub column name V - type
	 *            of column value
	 * @throws HectorException
	 */
	public <K, SN, N, V> MutationResult insertSuperColumns(String columnFamily,
			K key, Serializer<K> keySerializer,
			Map<SN, Map<N, V>> superNameSubColumnsMap, int ttl,
			Serializer<SN> superNameSerializer, Serializer<N> columnSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level)
			throws HectorException {
		
	    long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=insert_super_columns cf=" + columnFamily + " columns=" + superNameSubColumnsMap);
	    
		List<HColumn<N, V>> subColumns = new ArrayList<HColumn<N, V>>();
		Mutator<K> mutator = HFactory.createMutator(keyspaces.get(level), keySerializer);
		
		for (SN superName : superNameSubColumnsMap.keySet()) {
			
			Map<N, V> subColumnsMap = superNameSubColumnsMap.get(superName);
			
			if (subColumnsMap != null) {
				
				subColumns.clear();
				
				for (N name : subColumnsMap.keySet()) {
					V value = subColumnsMap.get(name);
					HColumn<N, V> subColumn = HFactory.createColumn(name, value, columnSerializer, valueSerializer);
					subColumn.setTtl(ttl);
					subColumns.add(subColumn);
				}
				
				HSuperColumn<SN, N, V> superColumn = HFactory.createSuperColumn(superName, subColumns, Calendar.getInstance().getTimeInMillis(), superNameSerializer, columnSerializer,	valueSerializer);
				mutator.addInsertion(key, columnFamily, superColumn);
				superColumn = null;
			}
		}
		
		MutationResult result = mutator.execute();
		
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, superNameSubColumnsMap.size());
		
        return result;
	}

	/**
	 * Read single value from column family.
	 * 
	 * @param template
	 *            column family
	 * @param key
	 *            row key
	 * @param column
	 *            column name
	 * @return value of type T N - type of column name V - type of column value
	 *         Note: Assumed the row key is a string
	 * @throws HectorException
	 * 
	 */
	@SuppressWarnings("unchecked")
	public <N, V> V read(ColumnFamilyTemplate<String, N> template, String key, N column, V returnType) throws HectorException {
		
        long ts1 = System.currentTimeMillis();
        
        logger.debug("event=read cf=" + template.getColumnFamily() + " key=" + key + " column=" + column);
        
        try {

        	ColumnFamilyResult<String, N> res = template.queryColumns(key);
            
        	if (returnType instanceof String) {
                return (V) res.getString(column);
            } else if (returnType instanceof Date) {
                return (V) res.getDate(column);
            } else if (returnType instanceof Integer) {
                return (V) res.getInteger(column);
            } else if (returnType instanceof Long) {
                return (V) res.getLong(column);
            } else if (returnType instanceof UUID) {
                return (V) res.getUUID(column);
            } else if (returnType instanceof byte[]) {
                return (V) res.getByteArray(column);
            } else if (returnType instanceof UUID) {
                return (V) res.getUUID(column);
            } else if (returnType instanceof Composite) {
            
            	HColumn<N, java.nio.ByteBuffer> col = res.getColumn(column);
            	
            	if (col == null) {
            		return (V) null;
            	}
            	
            	return (V) Composite.fromByteBuffer(col.getValue());
            
            } else if (returnType instanceof DynamicComposite) {
            
            	HColumn<N, java.nio.ByteBuffer> col = res.getColumn(column);
            	
            	if (col == null) {
            		return (V) null;
            	}
            	
            	return (V) DynamicComposite.fromByteBuffer(col.getValue());
            
            } else {
                throw new IllegalArgumentException("Unsupported type of return type: " + returnType);
            }
        	
        } finally {
            long ts2 = System.currentTimeMillis();
            CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
        }
	}

	/**
	 * Read next numRows rows with a maximum of 100 columns per row.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param lastKey
	 *            last key read before or null
	 * @param numRows
	 *            maximum number of rows to read
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 */
	public <K, N, V> List<Row<K, N, V>> readNextNRows(String columnFamily,
			K lastKey, int numRows, Serializer<K> keySerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {
		return readNextNRows(columnFamily, lastKey, numRows, 100, keySerializer, columnNameSerializer, valueSerializer, level);
	}

	/**
	 * Read next numRows rows with a maximum of numCols columns per row.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param lastKey
	 *            last key read before or null
	 * @param numRows
	 *            maximum number of rows to read
	 * @param numCols
	 *            maximum number of columns to read
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 */
	public <K, N, V> List<Row<K, N, V>> readNextNRows(String columnFamily,
			K lastKey, int numRows, int numCols, Serializer<K> keySerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {

	    long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=read_nextn_rows cf=" + columnFamily + " last_key=" + lastKey + " num_rows=" + numRows + " num_cols=" + numCols);
	    
		List<Row<K, N, V>> rows = new ArrayList<Row<K, N, V>>();
		Keyspace keyspace = keyspaces.get(level);

		RangeSlicesQuery<K, N, V> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, keySerializer, columnNameSerializer, valueSerializer)
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

		return rows;
	}

	/**
	 * Read next numRows rows with a maximum of numCols columns per row. Ensure
	 * all returned rows are non-empty rows.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param lastKey
	 *            last key read before or null
	 * @param numRows
	 *            maximum number of rows to read
	 * @param numCols
	 *            maximum number of columns to read
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 */
	public <K, N, V> List<Row<K, N, V>> readNextNNonEmptyRows(String columnFamily, K lastKey, int numRows, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer, HConsistencyLevel level) {
	    
	    long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=read_nextn_nonempty_rows cf=" + columnFamily + " last_key=" + lastKey + " num_rows=" + numRows + " num_cols" + numCols);
	    
	    try {
	    	
	        int pageSize = 100;

	        List<Row<K, N, V>> rows = new ArrayList<Row<K, N, V>>();
	        Keyspace keyspace = keyspaces.get(level);
	        RangeSlicesQuery<K, N, V> rangeSlicesQuery;

	        // page through rows in increments of 100 until the desired number of
	        // rows is found

	        while (true) {

	            rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, keySerializer,
	                    columnNameSerializer, valueSerializer)
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
	                return rows;
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
	                    return rows;
	                }
	            }
	        }
	        
	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }
	}

	/**
	 * Read next numRows rows obeying single where clause.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param lastKey
	 *            last key read before or null
	 * @param whereColumn
	 *            where clause column
	 * @param whereValue
	 *            where clause value
	 * @param numRows
	 *            maximum number of rows
	 * @param numCols
	 *            maximum number of columns
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 */
	public <K, N, V> List<Row<K, N, V>> readNextNRows(String columnFamily, K lastKey, N whereColumn, V whereValue, int numRows, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer, HConsistencyLevel level) {

		Map<N, V> columnValues = new HashMap<N, V>();
		columnValues.put(whereColumn, whereValue);

		return readNextNRows(columnFamily, lastKey, columnValues, numRows, numCols, keySerializer, columnNameSerializer, valueSerializer, level);
	}

	/**
	 * Read next numRows rows obeying complex where clause.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param lastKey
	 *            last key read before or null
	 * @param columnValues
	 *            hash map with key value pairs for where clause
	 * @param numRows
	 *            maximum number of rows to read
	 * @param numCols
	 *            maximum number of columns to read
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 */
	public <K, N, V> List<Row<K, N, V>> readNextNRows(String columnFamily, K lastKey, Map<N, V> columnValues, int numRows, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer, HConsistencyLevel level) {

        long ts1 = System.currentTimeMillis();
        
        logger.debug("event=read_nextn_rows cf=" + columnFamily + " last_key=" + lastKey + " num_rows=" + numRows + " num_cols=" + numCols + " values=" + columnValues);

		List<Row<K, N, V>> rows = new ArrayList<Row<K, N, V>>();
		Keyspace keyspace = keyspaces.get(level);

		IndexedSlicesQuery<K, N, V> indexedSlicesQuery = HFactory.createIndexedSlicesQuery(keyspace, keySerializer,	columnNameSerializer, valueSerializer)
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

		return rows;
	}

	/**
	 * Read single row by row key.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param key
	 *            row key
	 * @param numCols
	 *            maximum number of columns
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 * @return row Will get row starting from the beginning
	 */
	public <K, N, V> Row<K, N, V> readRow(String columnFamily, K key, int numCols, 
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {
		return readRow(columnFamily, key, null, null, numCols, keySerializer, columnNameSerializer, valueSerializer, level);
	}

	/**
	 * Read single row by row key.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param key
	 *            row key
	 * @param firstColumnName
	 *            the beginning of the slice column
	 * @param lastColumnName
	 *            the end of the slice column
	 * @param numCols
	 *            maximum number of columns
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 * @return row
	 */
	public <K, N, V> Row<K, N, V> readRow(String columnFamily, K key, N firstColumnName, N lastColumnName, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer, HConsistencyLevel level) {
        
		long ts1 = System.currentTimeMillis();
		
		logger.debug("event=read_row cf=" + columnFamily + " key=" + key + " first_col=" + firstColumnName + " last_col=" + lastColumnName + " num_cols=" + numCols);

		Row<K, N, V> row = null;
		Keyspace keyspace = keyspaces.get(level);

		RangeSlicesQuery<K, N, V> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, keySerializer, columnNameSerializer, valueSerializer)
				.setColumnFamily(columnFamily)
				.setRange(firstColumnName, lastColumnName, false, numCols)
				.setRowCount(1).setKeys(key, key);

		QueryResult<OrderedRows<K, N, V>> result = rangeSlicesQuery.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

		OrderedRows<K, N, V> orderedRows = result.get();
		Iterator<Row<K, N, V>> rowsIterator = orderedRows.iterator();

		if (rowsIterator.hasNext()) {
			row = rowsIterator.next();
			// Venu 02/07/12: Checking for null rows or null column slice.
			if (row == null || row.getColumnSlice() == null
					|| row.getColumnSlice().getColumns() == null
					|| row.getColumnSlice().getColumns().isEmpty()) {
				row = null;
			}
		}
		
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      

		return row;
	}

	/**
	 * Read a row slice by row key and super column slice.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param key
	 *            row key
	 * @param firstColumnName
	 *            the beginning of the slice column
	 * @param lastColumnName
	 *            the end of the slice column
	 * @param numCols
	 *            maximum number of columns
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 * @return row
	 */
	public <K, SN, N, V> SuperSlice<SN, N, V> readRowFromSuperColumnFamily(String columnFamily, K key, SN firstColumnName, SN lastColumnName, int numCols, 
			Serializer<K> keySerializer, Serializer<SN> superNameSerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {

	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=read_row_from_super_column_family cf=" + columnFamily + "key=" + key  +" first_column=" + firstColumnName + " last_column_name=" + lastColumnName + " num_cols=" + numCols);

	    try {
	        
	    	Keyspace keyspace = keyspaces.get(level);
	        
	        SuperSliceQuery<K, SN, N, V> rangeSlicesQuery = HFactory.createSuperSliceQuery(keyspace, keySerializer, superNameSerializer, columnNameSerializer, valueSerializer)
	        		.setColumnFamily(columnFamily)
	                .setRange(firstColumnName, lastColumnName, false, numCols)
	                .setKey(key);

	        QueryResult<SuperSlice<SN, N, V>> result = rangeSlicesQuery.execute();

	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
	        
	        SuperSlice<SN, N, V> superSlice = result.get();
	        
	        if (superSlice.getSuperColumns() == null || superSlice.getSuperColumns().size() == 0) {
	            return null;
	        }
	        
	        return superSlice;
	        
	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }
	}
	
	/**
	 * Read a single column by row key using SuperColumnQuery.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param key
	 *            row key
	 * @param columnName
	 *            the column key of the super column
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return list of rows K - type of row-key N - type of column name V - type
	 *         of column value
	 * @return row
	 */
	public <K, SN, N, V> HSuperColumn<SN, N, V> readColumnFromSuperColumnFamily(String columnFamily, K key, SN columnName, 
			Serializer<K> keySerializer, Serializer<SN> superNameSerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level) {

	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=read_column_from_super_column_family cf=" + columnFamily + " key=" + key + "column_name=" + columnName);

        try {
	       
	    	Keyspace keyspace = keyspaces.get(level);

	        SuperColumnQuery<K, SN, N, V> superColumnQuery = HFactory.createSuperColumnQuery(keyspace, keySerializer, superNameSerializer, columnNameSerializer, valueSerializer)
	        		.setColumnFamily(columnFamily)
	                .setSuperName(columnName)
	                .setKey(key);

	        QueryResult<HSuperColumn<SN, N, V>> result = superColumnQuery.execute();

	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
	        
	        HSuperColumn<SN, N, V> superColumn = result.get();
	        
	        if (superColumn == null) {
	            return null;
	        }
	        
	        return superColumn;
	        
	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }
	}
	/**
	 * Read multiple supercolumns from a column family given roe-key, and optional first-col, last-col and numCol
	 * @param <K>
	 * @param <SN>
	 * @param <N>
	 * @param <V>
	 * @param columnFamily
	 * @param key
	 * @param keySerializer
	 * @param superNameSerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level 
	 * @param firstCol The starting of the range 
	 * @param lastCol The end of the range
	 * @param numCol the number of columns to read
	 * @return
	 */
	public 	<K, SN, N, V> List<HSuperColumn<SN, N, V>> readColumnsFromSuperColumnFamily(String columnFamily, K key, 
			Serializer<K> keySerializer, Serializer<SN> superNameSerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
	        HConsistencyLevel level, SN firstCol, SN lastCol, int numCol) {
		
	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=read_columns_from_super_column_family cf=" + columnFamily + " key=" + key);

        try {
	    	
	        Keyspace keyspace = keyspaces.get(level);

	        SuperSliceQuery<K, SN, N, V> superSliceQuery = HFactory.createSuperSliceQuery(keyspace, keySerializer, superNameSerializer, columnNameSerializer, valueSerializer)
	        .setColumnFamily(columnFamily)
	        .setKey(key)
	        .setRange(firstCol, lastCol, false, numCol);

	        QueryResult<SuperSlice<SN, N, V>> result = superSliceQuery.execute();

	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

	        return result.get().getSuperColumns();

	    } finally {
	        long ts2 = System.currentTimeMillis();
	        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	    }	    
	}

	/**
	 * Read single row by row key.
	 * 
	 * @param columnFamily
	 *            column family
	 * @param key
	 *            row key
	 * @param numCols
	 *            maximum number of columns to read
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param valueSerializer
	 * @param level
	 *            consistency level
	 * @return null if no row found. Else return appt column slice K - type of
	 *         row-key N - type of column name V - type of column value
	 */
	public <K, N, V> ColumnSlice<N, V> readTimeUUIDRow(String columnFamily, K key, N firstColumn, N lastColumn, int numCols, 
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer, Serializer<V> valueSerializer, HConsistencyLevel level) {

		Row<K, N, V> row = readRow(columnFamily, key, firstColumn, lastColumn, numCols, keySerializer, columnNameSerializer, valueSerializer, level);

		if (row == null) {
			return null;
		}

		return row.getColumnSlice();
	}

	/**
	 * Insert or update single row.
	 * 
	 * @param rowKey
	 *            row key
	 * @param columnFamily
	 *            column family
	 * @param columnValues
	 *            hash map containing key value pairs for columns to insert or
	 *            update
	 * @param level
	 *            consistency level
	 * @return Note: This method assumes a String for key, column-name & value
	 */
	public MutationResult insertOrUpdateRow(String rowKey, String columnFamily,	Map<String, String> columnValues, HConsistencyLevel level) {
		return insertRow(rowKey, columnFamily, columnValues, level, null);
	}

	/**
	 * Insert or update single row with ttl.
	 * 
	 * @param rowKey
	 *            row key
	 * @param columnFamily
	 *            column family
	 * @param columnValues
	 *            hash map containing key value pairs for columns to insert or
	 *            update
	 * @param level
	 *            consistency level
	 * @param ttl
	 *            time to live
	 * @return Note: This method assumes a String for key, column-name & value
	 */
	public MutationResult insertRow(String rowKey, String columnFamily,	Map<String, String> columnValues, HConsistencyLevel level, Integer ttl) {
		return this.insertRow(rowKey, columnFamily, columnValues, StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), level, ttl);
	}

	/**
	 * Insert or update single row with ttl. K is the key type, N is column name
	 * type and V is the column value type
	 * 
	 * @param rowKey
	 *            row key
	 * @param columnFamily
	 *            column family
	 * @param columnValues
	 *            hash map containing key value pairs for columns to insert or
	 *            update
	 * @param level
	 *            consistency level
	 * @param ttl
	 *            time to live
	 * @return Note: This method assumes a String for key, column-name & value
	 */
	public <K, N, V> MutationResult insertRow(K rowKey, String columnFamily, Map<N, V> columnValues, 
			Serializer<K> keySerializer, Serializer<N> nameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level, Integer ttl) {

	    long ts1 = System.currentTimeMillis();
	    
        logger.debug("event=insert_row key=" + rowKey + " cf=" + columnFamily + " ttl=" + (ttl == null ? "null" : ttl));
	    
		Mutator<K> mutator = HFactory.createMutator(keyspaces.get(level), keySerializer);

		for (N key : columnValues.keySet()) {
			
			HColumn<N, V> col = HFactory.createColumn(key, columnValues.get(key), nameSerializer, valueSerializer);
			
			if (ttl != null) {
				col.setTtl(ttl);
			}
			
			mutator.addInsertion(rowKey, columnFamily, col);
		}

		MutationResult result = mutator.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);

		long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      

		return result;
	}

	/**
	 * Insert or update single row with ttl. K is the key type, N is column name
	 * type and V is the column value type
	 * 
	 * @param rowColumnValues
	 *            A map containing row keys and a map of column names, column
	 *            values
	 * @param columnFamily
	 *            column family
	 * @param level
	 *            consistency level
	 * @param ttl
	 *            time to live
	 * @return Note: This method assumes a String for key, column-name & value
	 */
	public <K, N, V> MutationResult insertRows(Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			Serializer<K> keySerializer, Serializer<N> nameSerializer, Serializer<V> valueSerializer, HConsistencyLevel level, Integer ttl) {

	    long ts1 = System.currentTimeMillis();
	    
	    logger.debug("event=insert_rows row_column_values=" + rowColumnValues + " cf=" + columnFamily + " ttl=" + (ttl == null ? "null" : ttl));

		Mutator<K> mutator = HFactory.createMutator(keyspaces.get(level), keySerializer);

		for (K rowKey : rowColumnValues.keySet()) {
			
			Map<N, V> columnValues = rowColumnValues.get(rowKey);
			
			for (N key : columnValues.keySet()) {
				
				HColumn<N, V> col = HFactory.createColumn(key, columnValues.get(key), nameSerializer, valueSerializer);
				
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

		return result;
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
	public QueryResult<CqlRows<String, String, String>> readRows(String queryString, HConsistencyLevel level) {
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
	}

	/**
	 * Delete single column value or the entire row
	 * 
	 * @param template
	 *            column family
	 * @param key
	 *            row key
	 * @param column
	 *            column name. If column is null, the entire row is deleted
	 * @throws HectorException
	 */
	public <K, N> void delete(ColumnFamilyTemplate<K, N> template, K key, N column) throws HectorException {
		
        long ts1 = System.currentTimeMillis();
        
        logger.debug("event=delete key=" + key + " cf=" + template.getColumnFamily() + " column=" + (column == null ? "null" : column));
        
		if (column != null) {
			template.deleteColumn(key, column);
		} else {
			template.deleteRow(key);
		}
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}

	/**
	 * Delete single column value or the entire row
	 * 
	 * @param template
	 *            column family
	 * @param key
	 *            row key
	 * @param superColumn
	 *            column name. If column is null, the entire row is deleted
	 * @throws HectorException
	 */
	public <K, SN, N> void deleteSuperColumn(SuperCfTemplate<K, SN, N> template, K key, SN superColumn)	throws HectorException {
		
        long ts1 = System.currentTimeMillis();
        
        logger.debug("event=delete_super_column key=" + key + " cf=" + template.getColumnFamily() + " super_column=" + (superColumn == null ? "null" : superColumn));

		if (superColumn != null) {
			template.deleteColumn(key, superColumn);
		} else {
			template.deleteRow(key);
		}
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);

        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      
	}
	
	/**
	 * Create a unique time based UUID
	 * 
	 * @param timeMillis
	 *            Time in milli seconds since Jan 1, 1970
	 * @throws InterruptedException
	 */
	public java.util.UUID getTimeUUID(long timeMillis) throws InterruptedException {
		return new java.util.UUID(newTime(timeMillis, false), com.eaio.uuid.UUIDGen.getClockSeqAndNode());
	}

	/**
	 * Create a unique time based UUID
	 * 
	 * @param millis
	 *            time
	 */
	public java.util.UUID getUniqueTimeUUID(long millis) {
		return new java.util.UUID(com.eaio.uuid.UUIDGen.createTime(millis),	com.eaio.uuid.UUIDGen.getClockSeqAndNode());
	}

	/**
	 * Create a unique time based long value
	 * 
	 * @param timeMillis
	 *            Time in milli seconds since Jan 1, 1970
	 * @throws InterruptedException
	 */
	public long getTimeLong(long timeMillis) throws InterruptedException {
		long newTime = timeMillis * 1000000000 + (System.nanoTime() % 1000000) * 1000 + random.nextInt(999999); 
		return newTime;
	}
	
	/**
	 * Return the column count.
	 * 
	 * @param columnFamily
	 *            The name of the column family.
	 * @param key
	 *            The row key (as a string)
	 * @param level
	 *            The consistency level of the keyspace.
	 */
	public <K, N> int getCount(String columnFamily, K key, Serializer<K> keySerializer, Serializer<N> columnNameSerializer,	HConsistencyLevel level) throws HectorException {
        
		long ts1 = System.currentTimeMillis();
		
		logger.debug("event=get_count cf=" + columnFamily + " key=" + key);

		@SuppressWarnings("unchecked")
		MultigetCountQuery<K, N> query = new MultigetCountQuery<K, N>(keyspaces.get(level), keySerializer, columnNameSerializer)
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

		throw new HectorException("Count not found for key " + key);
	}
	
	/**
	 * Increment Cassandra counter by incrementBy
	 * @param <K>
	 * @param <N>
	 * @param columnFamily
	 * @param rowKey
	 * @param columnName
	 * @param incrementBy
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param level
	 */
	public <K, N> void incrementCounter(String columnFamily, K rowKey, String columnName, int incrementBy, Serializer<K> keySerializer, Serializer<N> columnNameSerializer, HConsistencyLevel level) {

		logger.debug("event=increment_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName + " inc=" + incrementBy);
		
		Mutator<K> mutator = HFactory.createMutator(keyspaces.get(level), keySerializer);
		mutator.incrementCounter(rowKey, columnFamily, columnName, incrementBy);
		mutator.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
	}

	/**
	 * Decrement Cassandra counter by decrementBy
	 * 
	 * @param <K>
	 * @param <N>
	 * @param columnFamily
	 * @param rowKey
	 * @param columnName
	 * @param decrementBy
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param level
	 */
	public <K, N> void decrementCounter(String columnFamily, K rowKey, String columnName, int decrementBy, Serializer<K> keySerializer, Serializer<N> columnNameSerializer, HConsistencyLevel level) {
        
		long ts1 = System.currentTimeMillis();

		logger.debug("event=decrement_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName + " dec=" + decrementBy);

		Mutator<K> mutator = HFactory.createMutator(keyspaces.get(level), keySerializer);
		mutator.decrementCounter(rowKey, columnFamily, columnName, decrementBy);
		mutator.execute();
		
		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
        long ts2 = System.currentTimeMillis();
        CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));      

	}
	
	/**
	 * Return current value of Cassandra counter
	 * 
	 * @param <K>
	 * @param <N>
	 * @param columnFamily
	 * @param rowKey
	 * @param columnName
	 * @param keySerializer
	 * @param columnNameSerializer
	 * @param level
	 * @return
	 */
	public <K, N> long getCounter(String columnFamily, K rowKey, N columnName, Serializer<K> keySerializer, Serializer<N> columnNameSerializer, HConsistencyLevel level) {
		
        long ts1 = System.currentTimeMillis();
	    logger.debug("event=get_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName);
        
        try {
	    	
	        CounterQuery<K, N> countQuery = HFactory.createCounterColumnQuery(keyspaces.get(level), keySerializer, columnNameSerializer);
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
}
