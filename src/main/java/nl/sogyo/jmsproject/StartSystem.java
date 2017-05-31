package nl.sogyo.jmsproject;

public class StartSystem {	
	public static void main (String[] args) {
		Thread casThread = new Thread(new CasListener());
		casThread.start();
		
		Thread pgThread = new Thread(new PgPublisher());
		pgThread.start();
		
		PgUpdater pgUpdater = new PgUpdater();
		pgUpdater.run();
	}
}