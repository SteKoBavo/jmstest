package nl.sogyo.jmsproject.cassandra;

import nl.sogyo.jmsproject.JMSTopic;

public class CassandraListener implements Runnable, javax.jms.MessageListener {
	private JMSTopic jmsTopic;
	private CassandraConnection cassandra;
	
	public CassandraListener(JMSTopic jmsTopic, CassandraConnection cassandra) {
		this.jmsTopic = jmsTopic;
		this.cassandra = cassandra;
	}
	
	public void run() {
		try {
			while(true) {
				String msg = this.jmsTopic.receive();
				this.onMessage(msg);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}

	public void onMessage(javax.jms.Message msg) {
		try {
			this.onMessage(((javax.jms.TextMessage) msg).getText());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void onMessage(String msg) {
		String sql = this.messageToSQL(msg);
		this.cassandra.execute(sql);
	}
	
	private String messageToSQL(String msg) {
		String[] row = msg.split(";");
		return "INSERT INTO trafficlights (direction, color) VALUES ('" + row[0] + "', '" + row[1] + "')";
	}
	
	public void close() {
		try {
			this.cassandra.close();
			this.jmsTopic.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}