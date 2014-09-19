package cs3213.jms.order;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * Matric 1:
 * Name   1:
 * 
 * Matric 2:
 * Name   2:
 *
 * This file implements a pipe that transfer messages using JMS.
 */

public class JmsPipe implements IPipe, MessageListener {
    private QueueConnectionFactory _qconFactory;
    private QueueConnection _qcon;
    private QueueSession _qsession;
    private QueueSender _qsender;
    private TextMessage _senderTextMsg;
    private QueueReceiver _qreceiver;
    private Queue _queue;

    private String _factoryName;
    private String _queueName;

    private String _receiverMsgLine;

    public JmsPipe(String factoryName, String queueName) {
        _factoryName = factoryName;
        _queueName = queueName;
        try {
            init(getInitialContext());
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Order s) {
        try {
            _senderTextMsg.setText(s.toString() + "\n");
            _qsender.send(_senderTextMsg);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Order read() {
        if (_receiverMsgLine == null) {
            return null;
        }
        Order o = Order.fromString(_receiverMsgLine);
        _receiverMsgLine = null;
        return o;
    }

    @Override
    public void onMessage(Message msg) {
        try {
            if (msg instanceof TextMessage) {
                _receiverMsgLine = ((TextMessage) msg).getText();
            } else {
                _receiverMsgLine = msg.toString();
            }
        } catch (JMSException jmse) {
            System.err.println("An exception occurred: " + jmse.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            _qsender.close();
            _qreceiver.close();
            _qsession.close();
            _qcon.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private InitialContext getInitialContext()
            throws NamingException {
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
        props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
        props.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
        return new InitialContext(props);
    }

    public void init(Context ctx)
            throws NamingException, JMSException {
        _qconFactory = (QueueConnectionFactory) ctx.lookup(this._factoryName);
        _qcon = _qconFactory.createQueueConnection();
        _qsession = _qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        _queue = (Queue) ctx.lookup(_queueName);
        _qreceiver = _qsession.createReceiver(_queue);
        _qreceiver.setMessageListener(this);

        _qsender = _qsession.createSender(_queue);
        _senderTextMsg = _qsession.createTextMessage();
        _qcon.start();
    }
    
}
