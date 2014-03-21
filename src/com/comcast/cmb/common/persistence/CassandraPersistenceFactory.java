package com.comcast.cmb.common.persistence;

public class CassandraPersistenceFactory {
	
	private void CassandraPersistenceFactory() {
	}

	public static AbstractCassandraPersistence getInstance() {
		return CassandraHectorPersistence.getInstance();
	}
}
