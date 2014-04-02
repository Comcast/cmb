package com.comcast.cmb.common.persistence;

public class DurablePersistenceFactory {
	
	private void CassandraPersistenceFactory() {
	}

	public static AbstractDurablePersistence getInstance() {
		//return CassandraHectorPersistence.getInstance();
		return CassandraAstyanaxPersistence.getInstance();
	}
}
