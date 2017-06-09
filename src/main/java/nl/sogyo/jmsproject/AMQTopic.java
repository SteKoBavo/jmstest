package nl.sogyo.jmsproject;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

// Represents a specific Topic (Destination) for a specific Session on a specific Connection.
public class AMQTopic implements JMSTopic {
	private Connection connection;
	private Session session;
	private Destination dest;
	private MessageProducer producer;
	private MessageConsumer consumer;
	
	public AMQTopic(String host, int port, String user, String password, String destination) throws JMSException {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
		this.connection = factory.createConnection(user, password);
		this.connection.start();
		this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		this.dest = new ActiveMQTopic(destination);
		this.producer = this.session.createProducer(this.dest);
		this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		this.consumer = this.session.createConsumer(this.dest);
	}
	
	public void publish(String str) throws JMSException {
		TextMessage msg = this.session.createTextMessage(str);
		this.producer.send(msg);
	}
	
	public String receive() throws JMSException {
		Message msg = this.consumer.receive();
		return ((TextMessage) msg).getText();
	}
	
	public void close() throws JMSException {
		connection.close();
	}
}