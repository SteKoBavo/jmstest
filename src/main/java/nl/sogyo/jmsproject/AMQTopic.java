package nl.sogyo.jmsproject;

import javax.jms.*;
import javax.naming.*;
import java.util.Properties;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

public class AMQTopic implements JMSTopic {
	private Connection connection;
	private Session session;
	private Destination dest;
	private MessageProducer producer;
	private MessageConsumer consumer;

	private TopicConnection subscribeConnection;
	private TopicSubscriber subscriber;
	
	public AMQTopic(String host, int port, String user, String password, String destination) throws JMSException {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
		this.connection = factory.createConnection(user, password);
		this.connection.start();
		this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		this.dest = new ActiveMQTopic(destination);
		this.producer = this.session.createProducer(this.dest);
		this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		this.consumer = this.session.createConsumer(this.dest);
		this.setupSubscriber(host,port,user,password,destination);
	}
	
	public void publish(String str) throws JMSException {
		TextMessage msg = this.session.createTextMessage(str);
		this.producer.send(msg);
	}
	
	public String receive() throws JMSException {
		Message msg = this.consumer.receive();
		return ((TextMessage) msg).getText();
	}

	private void setupSubscriber(String host, int port, String user, String password, String destination) {
		try {
			// Obtain a JNDI connection
			Properties env = new Properties();
			// ... specify the JNDI properties specific to the vendor
			env.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
			env.setProperty(Context.PROVIDER_URL, "tcp://" + host + ":" + port);
			env.setProperty("topic." + destination, destination);

			InitialContext jndi = new InitialContext(env);

			// Look up a JMS connection factory
			TopicConnectionFactory conFactory =
					(TopicConnectionFactory) jndi.lookup("TopicConnectionFactory");

			// Create a JMS connection
			this.subscribeConnection =
					conFactory.createTopicConnection(user, password);

			// Create JMS session object
			TopicSession subSession =
					this.subscribeConnection.createTopicSession(false,
							Session.AUTO_ACKNOWLEDGE);

			// Look up a JMS topic
			Topic chatTopic = (Topic) jndi.lookup(destination);

			this.subscriber = subSession.createSubscriber(chatTopic);
			this.subscribeConnection.start();

		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void subscribe(MessageListener messageListener) throws JMSException {
		this.subscriber.setMessageListener(messageListener);
	}
	
	public void close() throws JMSException {
		this.connection.close();
		this.subscribeConnection.close();
	}
}