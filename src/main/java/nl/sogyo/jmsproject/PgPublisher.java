package nl.sogyo.jmsproject;

import java.sql.*;
import javax.jms.*;

public class PgPublisher implements Runnable {
	JMSTopic jmsTopic;
	MessageProducer producer;
	java.sql.Connection pgConnection;
	org.postgresql.PGConnection listenConnection;
	
	public PgPublisher() {
		try {
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		try {
			while (true) {
				this.contactDB();
				Thread.sleep(100);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}
	
	private void contactDB() throws Exception {
		Statement stmt = this.pgConnection.createStatement();
		ResultSet rst = stmt.executeQuery("SELECT 1");	//Issue a dummy query to contact the backend and receive any pending notifications.
		rst.close();
		stmt.close();
		
		org.postgresql.PGNotification notifications[] = this.listenConnection.getNotifications();
		if (notifications != null) {			
			System.out.println("Got notification!");	//Note that we cannot (yet) delete records from the Cassandra DB this way, only add new records. Also, Cassandra automatically overwrites a record when a row is inserted with the same primary key.
			this.publishTable();
		}
	}
	
	private void publishTable() throws Exception {
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
	
	public void close() {
		try {
			this.pgConnection.close();
			this.jmsTopic.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}