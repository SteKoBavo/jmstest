package nl.sogyo.jmsproject;

import nl.sogyo.jmsproject.postgresql.*;
import nl.sogyo.jmsproject.cassandra.*;

public class StartSystem {	
	public static void main (String[] args) {
		Thread cassandraThread = new Thread(new CassandraListener());
		cassandraThread.start();
		
		Thread pgThread = new Thread(new PgPublisher());
		pgThread.start();
		
		Thread dataStreamThread = new Thread(new DataStreamer());
		dataStreamThread.start();
		
		PgUpdater pgUpdater = new PgUpdater();
		pgUpdater.run();
	}
}