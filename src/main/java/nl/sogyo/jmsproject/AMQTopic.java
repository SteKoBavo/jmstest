package nl.sogyo.jmsproject;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

public class AMQTopic implements JMSTopic {
	private TopicConnection connection;
	private TopicSession session;
	private Topic dest;
	private MessageProducer producer;
	private MessageConsumer consumer;
	private TopicSubscriber subscriber;
	
	public AMQTopic(String host, int port, String user, String password, String destination) throws JMSException {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
		this.connection = factory.createTopicConnection(user, password);
		this.connection.start();
		this.session = this.connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
		this.dest = new ActiveMQTopic(destination);
		this.producer = this.session.createProducer(this.dest);
		this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		this.consumer = this.session.createConsumer(this.dest);
		this.subscriber = this.session.createSubscriber(this.dest);
	}
	
	public void publish(String str) throws JMSException {
		TextMessage msg = this.session.createTextMessage(str);
		this.producer.send(msg);
	}
	
	public String receive() throws JMSException {
		Message msg = this.consumer.receive();
		return ((TextMessage) msg).getText();
	}

	public void subscribe(MessageListener messageListener) throws JMSException {
		this.subscriber.setMessageListener(messageListener);
	}
	
	public void close() throws JMSException {
		this.connection.close();
	}
}