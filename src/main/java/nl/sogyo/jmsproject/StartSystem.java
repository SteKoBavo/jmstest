package nl.sogyo.jmsproject;

import nl.sogyo.jmsproject.postgresql.*;

public class StartSystem {	
	public static void main (String[] args) {
		Thread casThread = new Thread(new CasListener());
		casThread.start();
		
		Thread pgThread = new Thread(new PgPublisher());
		pgThread.start();
		
		Thread dataStreamThread = new Thread(new DataStreamer());
		dataStreamThread.start();
		
		PgUpdater pgUpdater = new PgUpdater();
		pgUpdater.run();
	}
}