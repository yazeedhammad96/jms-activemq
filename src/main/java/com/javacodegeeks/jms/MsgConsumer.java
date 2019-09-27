package com.javacodegeeks.jms;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

/**
 * 
 * @author Yazeed
 *
 */
public class MsgConsumer {

	private static String url = "tcp://localhost:61616";
	public static javax.jms.ConnectionFactory connFactory;
	public static javax.jms.Connection connection;
	public static javax.jms.Session mqSession;
	public static javax.jms.Topic topic;
	public static javax.jms.MessageConsumer consumer;

	public static void main(String[] args) throws URISyntaxException, Exception {
		BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:61616)"));
		broker.start();
		connFactory = new ActiveMQConnectionFactory(url);
		connection = connFactory.createConnection("system", "manager");
		connection.setClientID("0002");
		// connection.start();
		mqSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
		topic = mqSession.createTopic("RealTimeData");
		consumer = mqSession.createDurableSubscriber(topic, "SUBS01");
		connection.start();

		MessageListener listner = new MessageListener() {
			public void onMessage(Message message) {
				try {
					if (message instanceof TextMessage) {
						TextMessage txtmsg = (TextMessage) message;
						Calendar cal = Calendar.getInstance();
						// cal.getTime();
						SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
						String time = sdf.format(cal.getTime());

						String msg = "received_message =>> " + txtmsg.getText() + " | received_at :: " + time;
						System.out.println(msg);

						// consumer.sendData(msg);
					}

				} catch (JMSException e) {
					System.out.println("Caught:" + e);
					e.printStackTrace();
				}
			}
		};

		consumer.setMessageListener(listner);

	}

}