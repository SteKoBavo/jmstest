package nl.sogyo.jmsproject.cassandra;

public class CassandraMock implements CassandraConnection {
	private String lastSQL;
	public void execute(String sql) {
		this.lastSQL = sql;
	}
	public String getLastSQL() {
		return this.lastSQL;
	}
	public void close() {
	}
}