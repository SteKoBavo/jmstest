package nl.sogyo.jmsproject;

import javax.jms.JMSException;

public interface JMSTopic {
	public void publish(String str) throws JMSException;
	public String receive() throws JMSException;
	public void close() throws JMSException;
}