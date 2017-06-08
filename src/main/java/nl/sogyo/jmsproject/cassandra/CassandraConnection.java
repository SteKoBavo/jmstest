package nl.sogyo.jmsproject.cassandra;

import com.datastax.driver.core.*;

public class CassandraConnection {
	private Cluster cluster;
	private Session session;
	
	public CassandraConnection(String host, String keyspace) {
		this.cluster = Cluster.builder().addContactPoint(host).build();
		this.session = this.cluster.connect(keyspace);
	}
	
	public void execute(String sql) {
		this.session.execute(sql);
	}
	
	public void close() {
		this.session.close();
		this.cluster.close();
	}
}