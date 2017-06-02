package nl.sogyo.jmsproject;

import java.io.*;
import java.net.*;
import java.util.Random;

public class DataStreamer implements Runnable {
	public void run() {
		try {
			ServerSocket serverSock = new ServerSocket(5083);
			while (true) {
				Socket sock = serverSock.accept();	// Waits here until a connection is made!
				
				PrintWriter writer = new PrintWriter(sock.getOutputStream());
				this.streamRandomNumbers(writer);
				writer.close();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	private void streamRandomNumbers(PrintWriter writer) {
		try {
			Random rand = new Random();
			while (true) {
				int randomNumber = rand.nextInt(4);
				writer.println("Random number:" + randomNumber);
				Thread.sleep(100);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}