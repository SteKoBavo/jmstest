package nl.sogyo.jmsproject;

import nl.sogyo.jmsproject.postgresql.*;
import nl.sogyo.jmsproject.cassandra.*;

public class StartSystem {	
	public static void main (String[] args) {
		try {
			JMSTopic jmsTopic = new JMSTopic("localhost",61616,"admin","password","event");
			CassandraConnection cassandra = new CassandraConnection("127.0.0.1","dev");
			Thread cassandraThread = new Thread(new CassandraListener(jmsTopic,cassandra));
			cassandraThread.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
				
		Thread dataStreamThread = new Thread(new DataStreamer());
		dataStreamThread.start();
				
		Thread pgThread = new Thread(new PgPublisher());
		pgThread.start();
		
		PgUpdater pgUpdater = new PgUpdater();
		pgUpdater.run();
	}
}