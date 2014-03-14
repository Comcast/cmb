package com.comcast.cmb.common.persistence;

import java.util.concurrent.ConcurrentHashMap;

public class CassandraPersistenceFactory {
	
	private static ConcurrentHashMap<String, AbstractCassandraPersistence> cassandraPersistenceMap = new ConcurrentHashMap<String, AbstractCassandraPersistence>();
	
	private void CassandraPersistenceFactory() {
	}

	public static AbstractCassandraPersistence getInstance(String keyspace) {
		if (!cassandraPersistenceMap.containsKey(keyspace)) {
			cassandraPersistenceMap.put(keyspace, new CassandraHectorPersistence(keyspace));
		}
		return cassandraPersistenceMap.get(keyspace);
	}
}
