package com.comcast.cmb.common.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.comcast.cmb.common.util.CMBProperties;

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

public abstract class AbstractCassandraPersistence {
	
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
	
	public abstract boolean isAlive();

	public abstract Keyspace getKeySpace(HConsistencyLevel consistencyLevel);

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
	public abstract <K, N, V> void update(String columnFamily, K key, N column, V value, Serializer<K> keySerializer, Serializer<N> nameSerializer, Serializer<V> valueSerializer, HConsistencyLevel level)
			throws HectorException;

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
	public abstract <K, SN, N, V> MutationResult insertSuperColumn(
			String columnFamily, K key, Serializer<K> keySerializer,
			SN superName, Integer ttl, Serializer<SN> superNameSerializer,
			Map<N, V> subColumnNameValues, Serializer<N> columnSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level)
			throws HectorException;

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
	public abstract <K, SN, N, V> MutationResult insertSuperColumns(
			String columnFamily, K key, Serializer<K> keySerializer,
			Map<SN, Map<N, V>> superNameSubColumnsMap, int ttl,
			Serializer<SN> superNameSerializer, Serializer<N> columnSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level)
			throws HectorException;

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
	public abstract <N, V> V read(ColumnFamilyTemplate<String, N> template,
			String key, N column, V returnType) throws HectorException;

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
	public abstract <K, N, V> List<Row<K, N, V>> readNextNRows(
			String columnFamily, K lastKey, int numRows,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level);

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
	public abstract <K, N, V> List<Row<K, N, V>> readNextNRows(
			String columnFamily, K lastKey, int numRows, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level);

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
	public abstract <K, N, V> List<Row<K, N, V>> readNextNNonEmptyRows(
			String columnFamily, K lastKey, int numRows, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level);

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
	public abstract <K, N, V> List<Row<K, N, V>> readNextNRows(
			String columnFamily, K lastKey, N whereColumn, V whereValue,
			int numRows, int numCols, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level);

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
	public abstract <K, N, V> List<Row<K, N, V>> readNextNRows(
			String columnFamily, K lastKey, Map<N, V> columnValues,
			int numRows, int numCols, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level);

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
	public abstract <K, N, V> ColumnSlice<N, V> readColumnSlice(
			String columnFamily, K key, int numCols,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level);

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
	public abstract <K, N, V> ColumnSlice<N, V> readColumnSlice(
			String columnFamily, K key, N firstColumnName, N lastColumnName,
			int numCols, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level);

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
	public abstract <K, SN, N, V> SuperSlice<SN, N, V> readRowFromSuperColumnFamily(
			String columnFamily, K key, SN firstColumnName, SN lastColumnName,
			int numCols, Serializer<K> keySerializer,
			Serializer<SN> superNameSerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level);

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
	public abstract <K, SN, N, V> HSuperColumn<SN, N, V> readColumnFromSuperColumnFamily(
			String columnFamily, K key, SN columnName,
			Serializer<K> keySerializer, Serializer<SN> superNameSerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level);

	public abstract <K, SN, N, V> List<HSuperColumn<SN, N, V>> readMultipleColumnsFromSuperColumnFamily(
			String columnFamily, Collection<K> keys,
			Collection<SN> columnNames, Serializer<K> keySerializer,
			Serializer<SN> superNameSerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level);

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
	public abstract <K, SN, N, V> List<HSuperColumn<SN, N, V>> readColumnsFromSuperColumnFamily(
			String columnFamily, K key, Serializer<K> keySerializer,
			Serializer<SN> superNameSerializer,
			Serializer<N> columnNameSerializer, Serializer<V> valueSerializer,
			HConsistencyLevel level, SN firstCol, SN lastCol, int numCol);

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
	public abstract MutationResult insertOrUpdateRow(String rowKey,
			String columnFamily, Map<String, String> columnValues,
			HConsistencyLevel level);

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
	public abstract MutationResult insertRow(String rowKey,
			String columnFamily, Map<String, String> columnValues,
			HConsistencyLevel level, Integer ttl);

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
	public abstract <K, N, V> MutationResult insertRow(K rowKey,
			String columnFamily, Map<N, V> columnValues,
			Serializer<K> keySerializer, Serializer<N> nameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level, Integer ttl);

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
	public abstract <K, N, V> MutationResult insertRows(
			Map<K, Map<N, V>> rowColumnValues, String columnFamily,
			Serializer<K> keySerializer, Serializer<N> nameSerializer,
			Serializer<V> valueSerializer, HConsistencyLevel level, Integer ttl);

	/**
	 * 
	 * @param columnFamily
	 * @param key
	 * @param column
	 * @param keySerializer
	 * @param columnSerializer
	 * @param level
	 * @throws HectorException
	 */
	public abstract <K, N> void delete(String columnFamily, K key, N column, 
			Serializer<K> keySerializer, Serializer<N> columnSerializer, 
			HConsistencyLevel level) throws HectorException;


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
	public abstract <K, N> void deleteBatch(String columnFamily,
			List<K> keyList, List<N> columnList, Serializer<K> keySerializer,
			HConsistencyLevel level, Serializer<N> columnSerializer)
			throws HectorException;

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
	public abstract <K, SN, N> void deleteSuperColumn(
			String superColumnFamily, K key, SN superColumn, Serializer<K> keySerializer, Serializer<SN> superColumnSerializer,
			HConsistencyLevel level)
			throws HectorException;

	/**
	 * Create a unique time based UUID
	 * 
	 * @param timeMillis
	 *            Time in milli seconds since Jan 1, 1970
	 * @throws InterruptedException
	 */
	public abstract java.util.UUID getTimeUUID(long timeMillis)
			throws InterruptedException;

	/**
	 * Create a unique time based UUID
	 * 
	 * @param millis
	 *            time
	 */
	public abstract java.util.UUID getUniqueTimeUUID(long millis);

	/**
	 * Create a unique time based long value
	 * 
	 * @param timeMillis
	 *            Time in milli seconds since Jan 1, 1970
	 * @throws InterruptedException
	 */
	public abstract long getTimeLong(long timeMillis)
			throws InterruptedException;

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
	public abstract <K, N> int getCount(String columnFamily, K key,
			Serializer<K> keySerializer, Serializer<N> columnNameSerializer,
			HConsistencyLevel level) throws HectorException;

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
	public abstract <K, N> void incrementCounter(String columnFamily, K rowKey,
			String columnName, int incrementBy, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, HConsistencyLevel level);

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
	public abstract <K, N> void decrementCounter(String columnFamily, K rowKey,
			String columnName, int decrementBy, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, HConsistencyLevel level);

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
	public abstract <K, N> void deleteCounter(String columnFamily, K rowKey,
			N columnName, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, HConsistencyLevel level);

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
	public abstract <K, N> long getCounter(String columnFamily, K rowKey,
			N columnName, Serializer<K> keySerializer,
			Serializer<N> columnNameSerializer, HConsistencyLevel level);

}