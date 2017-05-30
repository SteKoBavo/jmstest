// JMS
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;
import javax.jms.*;

public class JMSConnection {
	// Parameters
	private String user = "admin";
	private String password = "password";
	private String host = "localhost";
	private int port = 61616;
	private String destination = "event";
	
	private Connection connection;
	private Session session;
	private Destination dest;
	
	public JMSConnection() throws JMSException {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + this.host + ":" + this.port);
		this.connection = factory.createConnection(this.user, this.password);
		this.connection.start();
		this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		this.dest = new ActiveMQTopic(this.destination);
	}
	
	public MessageConsumer getConsumer() throws JMSException {
		return this.session.createConsumer(this.dest);
	}
	
	public MessageProducer getProducer() throws JMSException {
		MessageProducer producer = this.session.createProducer(this.dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		return producer;
	}

	public Session getSession() {
		return this.session;
	}
	
	public void close() throws JMSException {
		connection.close();
	}
}