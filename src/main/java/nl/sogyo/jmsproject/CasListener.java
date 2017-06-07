package nl.sogyo.jmsproject;

import com.datastax.driver.core.*;

public class CasListener implements Runnable {
	private JMSTopic jmsTopic;
	private Cluster cluster;
	private com.datastax.driver.core.Session casSession;
	
	public CasListener() {
		try {
			// Connect to ActiveMQ Topic
			this.jmsTopic = new JMSTopic("localhost",61616,"admin","password","event");
			
			// Connect to the Cassandra cluster and keyspace "dev"
			this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			this.casSession = this.cluster.connect("dev");
		} catch (Exception e) {
			e.printStackTrace();
		}
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

	public void onMessage(String msg) {
		String query = this.messageToQuery(msg);
		this.casSession.execute(query);
	}
	
	public String messageToQuery(String msg) {
		String[] row = msg.split(";");
		return "INSERT INTO trafficlights (direction, color) VALUES ('" + row[0] + "', '" + row[1] + "')";
	}
	
	public void close() {
		try {
			this.casSession.close();
			this.cluster.close();
			this.jmsTopic.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}