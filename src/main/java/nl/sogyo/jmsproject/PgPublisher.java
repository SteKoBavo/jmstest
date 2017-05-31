package nl.sogyo.jmsproject;

import java.sql.*;
import javax.jms.*;

public class PgPublisher {
	JMSTopic jmsTopic;
	MessageProducer producer;
	java.sql.Connection pgConnection;
	org.postgresql.PGConnection listenConnection;
	
	public PgPublisher() throws Exception {
		// Connect to ActiveMQ
		this.jmsTopic = new JMSTopic("localhost",61616,"admin","password","event");
		this.producer = this.jmsTopic.getProducer();
		
		// Connect to PostGreSQL
		Class.forName("org.postgresql.Driver");
		this.pgConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "stefan", "password");
		
		// Listen to PostgreSQL channel
		this.listenConnection = (org.postgresql.PGConnection) this.pgConnection;
		Statement stmt = this.pgConnection.createStatement();
		stmt.execute("LISTEN mychannel");
		stmt.close();
	}
	
	public void publish() throws Exception {
		while (true) {
			Statement stmt = this.pgConnection.createStatement();
			ResultSet rst = stmt.executeQuery("SELECT 1");	//Issue a dummy query to contact the backend and receive any pending notifications.
			rst.close();
			stmt.close();

			org.postgresql.PGNotification notifications[] = this.listenConnection.getNotifications();
			if (notifications != null) {
				System.out.println("Got notification!");	//Note that we cannot (yet) delete records from the Cassandra DB this way, only add new records. Also, Cassandra automatically overwrites a record when a row is inserted with the same primary key.
				
				// Query the TABLE testschema.test
				Statement st = this.pgConnection.createStatement();
				ResultSet rs = st.executeQuery("SELECT * FROM testschema.trafficlights");
				
				// Publish all rows
				while (rs.next()) {
					TextMessage msg = this.jmsTopic.createTextMessage(rs.getString(1).trim() + ";" + rs.getString(2).trim());
					this.producer.send(msg);
				}
				
				rs.close();
				st.close();
			}
			if (false) {
				break;
			}
	
			Thread.sleep(100);
		}
	}
	
	public void close() throws Exception {
		this.pgConnection.close();
		this.jmsTopic.close();
	}
	
	public static void main (String[] args) {
		try {
			PgPublisher pgPublisher = new PgPublisher();
			pgPublisher.publish();
			pgPublisher.close();
        } catch (Exception e) {
			// Just printStackTrace for all Exceptions for simplicity
            e.printStackTrace();
        }
	}
}