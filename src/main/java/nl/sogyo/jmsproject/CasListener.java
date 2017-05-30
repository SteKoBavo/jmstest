package nl.sogyo.jmsproject;

// Cassandra Java Driver
import com.datastax.driver.core.*;

// JMS
import javax.jms.*;

public class CasListener {
	public static void main (String[] args) {
		try {
			// Connect to ActiveMQ
			JMSTopic jmsTopic = new JMSTopic("localhost",61616,"admin","password","event");
			MessageConsumer consumer = jmsTopic.getConsumer();
			
			// Connect to the Cassandra cluster and keyspace "dev"
			Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			com.datastax.driver.core.Session casSession = cluster.connect("dev");
			
			// Listen to the topic and INSERT rows
			while(true) {
				Message msg = consumer.receive();
				String body = ((TextMessage) msg).getText();
				String[] row = body.split(";");
				if (row.length != 2) {
					break;
				}
				String query = "INSERT INTO people (firstname, lastname) VALUES ('" + row[0] + "', '" + row[1] + "')";
				casSession.execute(query);
			}
			
			// Close the connections
			casSession.close();
			cluster.close();
			jmsTopic.close();
        } catch (Exception e) {
			// Just printStackTrace for all Exceptions for simplicity
            e.printStackTrace();
        }
	}
}