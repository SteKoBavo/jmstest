package nl.sogyo.jmsproject;

public class CasListener implements Runnable {
	private JMSTopic jmsTopic;
	private CassandraConnection cassandra;
	
	public CasListener() {
		try {
			this.jmsTopic = new JMSTopic("localhost",61616,"admin","password","event");
			this.cassandra = new CassandraConnection("127.0.0.1","dev");
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
		this.cassandra.execute(query);
	}
	
	public String messageToQuery(String msg) {
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