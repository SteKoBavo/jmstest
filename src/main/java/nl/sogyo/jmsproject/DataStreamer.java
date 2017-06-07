package nl.sogyo.jmsproject;

import java.io.*;
import java.net.*;
import java.util.Random;

public class DataStreamer implements Runnable {
	ServerSocket serverSock;
	Random rand = new Random();
	
	public void run() {
		try {
			this.serverSock = new ServerSocket(5083);
			while (true) {
				this.connectStream();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	private void connectStream() throws IOException {
		Socket sock = this.serverSock.accept();	// Waits here until a connection is made!
		PrintWriter writer = new PrintWriter(sock.getOutputStream());
		this.streamRandomNumbers(writer);
		writer.close();
	}
	
	private void streamRandomNumbers(PrintWriter writer) {
		try {
			while (true) {
				int randomNumber = this.rand.nextInt(4);
				writer.println(randomNumber + "\n");
				Thread.sleep(100);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}