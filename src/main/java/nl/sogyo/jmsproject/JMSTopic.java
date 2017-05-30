package nl.sogyo.jmsproject;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

// Represents a specific Topic (Destination) for a specific Session on a specific Connection.
public class JMSTopic {
	private Connection connection;
	private Session session;
	private Destination dest;
	
	public JMSTopic(String host, int port, String user, String password, String destination) throws JMSException {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
		this.connection = factory.createConnection(user, password);
		this.connection.start();
		this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		this.dest = new ActiveMQTopic(destination);
	}
	
	public MessageConsumer getConsumer() throws JMSException {
		return this.session.createConsumer(this.dest);
	}
	
	public MessageProducer getProducer() throws JMSException {
		MessageProducer producer = this.session.createProducer(this.dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		return producer;
	}

	public TextMessage createTextMessage(String str) throws JMSException {
		return this.session.createTextMessage(str);
	}
	
	public void close() throws JMSException {
		connection.close();
	}
}