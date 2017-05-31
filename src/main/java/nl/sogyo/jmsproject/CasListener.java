package nl.sogyo.jmsproject;

import com.datastax.driver.core.*;
import javax.jms.*;

public class CasListener {
	private JMSTopic jmsTopic;
	private MessageConsumer consumer;
	private Cluster cluster;
	private com.datastax.driver.core.Session casSession;
	
	public CasListener() throws JMSException {
		// Connect to ActiveMQ Topic
		this.jmsTopic = new JMSTopic("localhost",61616,"admin","password","event");
		this.consumer = jmsTopic.getConsumer();
		
		// Connect to the Cassandra cluster and keyspace "dev"
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		this.casSession = cluster.connect("dev");
	}
	
	public void listen() throws JMSException {
		while(true) {
			Message msg = this.consumer.receive();
			this.onMessage(msg);
		}		
	}

	public void onMessage(Message msg) throws JMSException {
		String body = ((TextMessage) msg).getText();
		String[] row = body.split(";");
		String query = "INSERT INTO trafficlights (direction, color) VALUES ('" + row[0] + "', '" + row[1] + "')";
		this.casSession.execute(query);
	}
	
	public void close() throws JMSException {
		this.casSession.close();
		this.cluster.close();
		this.jmsTopic.close();
	}
	
	public static void main (String[] args) {
		try {
			CasListener casListener = new CasListener();
			casListener.listen();
			casListener.close();
        } catch (Exception e) {
			// Just printStackTrace for all Exceptions for simplicity
            e.printStackTrace();
        }
	}
}