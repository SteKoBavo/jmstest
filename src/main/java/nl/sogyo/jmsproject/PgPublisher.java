package nl.sogyo.jmsproject;

// JDBC
import java.sql.*;

// JMS
import javax.jms.*;

public class PgPublisher {
	public static void main (String[] args) {
		try {
			// Connect to ActiveMQ
			JMSTopic jmsTopic = new JMSTopic("localhost",61616,"admin","password","event");
			MessageProducer producer = jmsTopic.getProducer();
			
			// Connect to PostGreSQL
			Class.forName("org.postgresql.Driver");
			java.sql.Connection pgConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "stefan", "password");
			
			// Listen to PostgreSQL channel
			org.postgresql.PGConnection listenConnection = (org.postgresql.PGConnection) pgConnection;
			Statement stmt = pgConnection.createStatement();
			stmt.execute("LISTEN mychannel");
			stmt.close();
			
			while (true) {
				stmt = pgConnection.createStatement();
				ResultSet rst = stmt.executeQuery("SELECT 1");	//Issue a dummy query to contact the backend and receive any pending notifications.
				rst.close();
				stmt.close();

				org.postgresql.PGNotification notifications[] = listenConnection.getNotifications();
				if (notifications != null) {
					System.out.println("Got notification!");	//Note that we cannot (yet) delete records from the Cassandra DB this way, only add new records. Also, Cassandra automatically overwrites a record when a row is inserted with the same primary key.
					
					// Query the TABLE testschema.test
					Statement st = pgConnection.createStatement();
					ResultSet rs = st.executeQuery("SELECT * FROM testschema.test");
					
					// Publish all rows
					while (rs.next()) {
						TextMessage msg = jmsTopic.createTextMessage(rs.getString(1).trim() + ";" + rs.getString(2).trim());
						producer.send(msg);
					}
					
					rs.close();
					st.close();
				}
				if (false) {
					break;
				}
	
				Thread.sleep(500);
			}
			
			// Close the connections
			pgConnection.close();
			jmsTopic.close();
        } catch (Exception e) {
			// Just printStackTrace for all Exceptions for simplicity
            e.printStackTrace();
        }
	}
}