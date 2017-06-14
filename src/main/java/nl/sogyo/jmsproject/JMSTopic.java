package nl.sogyo.jmsproject;

import javax.jms.JMSException;
import javax.jms.MessageListener;

public interface JMSTopic {
	public void publish(String str) throws JMSException;
	public String receive() throws JMSException;
	public void subscribe(MessageListener messageListener) throws JMSException;
	public void close() throws JMSException;
}