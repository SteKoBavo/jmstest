package nl.sogyo.jmsproject;

import javax.jms.MessageListener;

public class JMSTopicMock implements JMSTopic {
	public void publish(String str) {
	}
	
	public String receive() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return "North;Red";
	}

	public void subscribe(MessageListener messageListener) {
	}
	
	public void close() {
	}
}