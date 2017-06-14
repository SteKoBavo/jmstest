package nl.sogyo.jmsproject;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

public class AMQTopic implements JMSTopic {
	private TopicConnection connection;
	private TopicSession session;
	private Topic topic;
	private MessageProducer producer;
	private MessageConsumer consumer;
	private TopicSubscriber subscriber;
	
	public AMQTopic(String host, int port, String user, String password, String topicName) throws JMSException {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
		this.connection = factory.createTopicConnection(user, password);
		this.connection.start();
		this.session = this.connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
		this.topic = new ActiveMQTopic(topicName);

		this.producer = this.session.createProducer(this.topic);
		this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		this.consumer = this.session.createConsumer(this.topic);
		this.subscriber = this.session.createSubscriber(this.topic);
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