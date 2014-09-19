package cs3213.jms.chat;

import javax.jms.*;
import javax.naming.*;
import java.io.*;
import java.io.InputStreamReader;
import java.util.Properties;

public class SimpleChat implements javax.jms.MessageListener {

    private TopicSession pubSession;
    private TopicSession subSession;
    private TopicPublisher publisher;
    private TopicConnection connection;
    private String username;
    
    public SimpleChat(String topicName, String username, String password)
            throws Exception {
                
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
        props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
        props.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
        
        InitialContext jndi = new InitialContext(props);

        // Look up a JMS connection factory
        TopicConnectionFactory conFactory
                = (TopicConnectionFactory) jndi.lookup("SimpleConnectionFactory");

        // Create a JMS connection
        TopicConnection connection
                = conFactory.createTopicConnection(username, password);

        // Create two JMS sessions
        TopicSession pubSession
                = connection.createTopicSession(false,
                        Session.AUTO_ACKNOWLEDGE);
        TopicSession subSession
                = connection.createTopicSession(false,
                        Session.AUTO_ACKNOWLEDGE);

        // Look up a JMS topic
        Topic chatTopic = (Topic) jndi.lookup(topicName);

        // Create a JMS publisher and subscriber
        TopicPublisher publisher
                = pubSession.createPublisher(chatTopic);
        TopicSubscriber subscriber
                = subSession.createSubscriber(chatTopic);

        // Set a JMS message listener
        subscriber.setMessageListener(this);

        // Intialize the Chat application
        set(connection, pubSession, subSession, publisher, username);

        // Start the JMS connection; allows messages to be delivered
        connection.start();
        
        System.out.println(username + " logged in.");
    }
    
    public void set(TopicConnection con, TopicSession pubSess,
            TopicSession subSess, TopicPublisher pub,
            String username) {
        this.connection = con;
        this.pubSession = pubSess;
        this.subSession = subSess;
        this.publisher = pub;
        this.username = username;
    }
    
    @Override
    public void onMessage(Message message) {
        try {
            TextMessage textMessage = (TextMessage) message;
            String text = textMessage.getText();
            System.out.println(text);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
    
    protected void writeMessage(String text) throws JMSException {
        TextMessage message = pubSession.createTextMessage();
        message.setText(username + " : " + text);
        publisher.publish(message);
    }
    
    public void close() throws JMSException {
        connection.close();
    }
    
    public static void main(String[] args) {        
        if (args.length < 3) {
            System.out.println("Usage: chat <topic> <username> <password>");
            System.out.println("<topic> must be defined in the JMS provider.");
        } else {
            try {
                String topic = args[0];
                String username = args[1];
                String password = args[2];            
                SimpleChat chat = new SimpleChat(topic, username, password);

                BufferedReader commandLine = new java.io.BufferedReader(new InputStreamReader(System.in));
                while (true) {
                    String s = commandLine.readLine();
                    if (s.equalsIgnoreCase("exit")) {
                        chat.close(); 
                        break;
                    } else {
                        chat.writeMessage(s);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
