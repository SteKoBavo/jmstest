package nl.sogyo.jmsproject.cassandra;

public interface CassandraConnection {
	public void execute(String sql);
	public void close();
}