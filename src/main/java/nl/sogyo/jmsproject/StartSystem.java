package nl.sogyo.jmsproject;

import nl.sogyo.jmsproject.postgresql.*;
import nl.sogyo.jmsproject.cassandra.*;

public class StartSystem {	
	public static void main (String[] args) {
		try {
			
			
			// Start CassandraListener
			JMSTopic jmsTopic = new JMSTopic("localhost",61616,"admin","password","event");
			CassandraConnection cassandra = new CassandraConnection("127.0.0.1","dev");
			Thread cassandraThread = new Thread(new CassandraListener(jmsTopic,cassandra));
			cassandraThread.start();
			
			
			// Start DataStreamer
			Thread dataStreamThread = new Thread(new DataStreamer());
			dataStreamThread.start();
			
			
			// Start PgPublisher
			JMSTopic jmsTopic2 = new JMSTopic("localhost",61616,"admin","password","event");
			Thread pgThread = new Thread(new PgPublisher(jmsTopic2));
			pgThread.start();
			
			
			// Start PgUpdater
			PgUpdater pgUpdater = new PgUpdater();
			pgUpdater.run();
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}