package com.javacodegeeks.jms;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

/**
 * s
 * @author Yazeed
 *
 */
public class MsgProducer {

	private static String url = "tcp://localhost:61616";
	public static javax.jms.ConnectionFactory connFactory;
	public static javax.jms.Connection connection;
	public static javax.jms.Session mqSession;
	public static javax.jms.Topic topic;
	public static javax.jms.MessageProducer producer;

	public static void main(String[] args) throws URISyntaxException, Exception {
		BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:61616)"));
		broker.start();
		connFactory = new ActiveMQConnectionFactory(url);
		connection = connFactory.createConnection("system", "manager");
		connection.start();
		mqSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		topic = mqSession.createTopic("RealTimeData");
		producer = mqSession.createProducer(topic);
		producer.setTimeToLive(30000);

		TextMessage message = mqSession.createTextMessage();

		int seq_id = 1;

		while (seq_id <= 5) {
			message.setText("Hello world | " + "seq_id #" + seq_id);
			producer.send(message);
			seq_id++;

			System.out.println("sent_msg =>> " + message.getText());
			// if(seq_id>100000) break;

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		connection.close();
		mqSession.close();
	}

}
