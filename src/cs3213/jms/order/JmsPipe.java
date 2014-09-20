package cs3213.jms.order;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Matric 1: A0105750N
 * Name   1: Gu Junchao
 * 
 * Matric 2:
 * Name   2:
 *
 * This file implements a pipe that transfer messages using JMS.
 */

public class JmsPipe implements IPipe, MessageListener {
    private QueueConnectionFactory _qconFactory;
    private String _factoryName;
    private String _queueName;

    private QueueConnection _senderQcon;
    private QueueSession _senderQsession;
    private QueueSender _qsender;
    private Queue _senderQueue;
    private TextMessage _senderTextMsg;

    private QueueConnection _receiverQcon;
    private QueueSession _receiverQsession;
    private QueueReceiver _qreceiver;
    private Queue _receiverQueue;

    private List<String> _receiverMsgs;

    private boolean _isSenderInitialized;
    private boolean _isReceiverInitialized;

    public JmsPipe(String factoryName, String queueName) {
        _factoryName = factoryName;
        _queueName = queueName;
        _receiverMsgs = new LinkedList<String>();
        _isReceiverInitialized = false;
        _isSenderInitialized = false;
    }

    @Override
    public void write(Order s) {
        try {
            if (!_isSenderInitialized) {
                try {
                    initSender(getInitialContext());
                } catch (NamingException nme) {
                    nme.printStackTrace();
                }
            }
            _senderTextMsg.setText(s.toString() + "\n");
            _qsender.send(_senderTextMsg);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Order read() {
        if (!_isReceiverInitialized) {
            try {
                initReceiver(getInitialContext());
            } catch (NamingException nme) {
                nme.printStackTrace();
            } catch (JMSException jmse) {
                jmse.printStackTrace();
            }
        }
        if (_receiverMsgs.isEmpty()) {
            return null;
        }
        String str = _receiverMsgs.remove(_receiverMsgs.size() - 1);
        Order o = Order.fromString(str);
        return o;
    }

    @Override
    public void onMessage(Message msg) {
        try {
            if (msg instanceof TextMessage) {
                _receiverMsgs.add(((TextMessage) msg).getText());
            } else {
                _receiverMsgs.add(msg.toString());
            }
        } catch (JMSException jmse) {
            System.err.println("An exception occurred: " + jmse.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            if (_isSenderInitialized) {
                _qsender.close();
                _senderQsession.close();
                _senderQcon.close();
            } else if (_isReceiverInitialized) {
                _qreceiver.close();
                _receiverQsession.close();
                _receiverQcon.close();
            }
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

    public void initSender(Context ctx) throws NamingException, JMSException {
        _qconFactory = (QueueConnectionFactory) ctx.lookup(this._factoryName);
        _senderQcon = _qconFactory.createQueueConnection();
        _senderQsession = _senderQcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        _senderQueue = (Queue) ctx.lookup(_queueName);
        _qsender = _senderQsession.createSender(_senderQueue);
        _senderTextMsg = _senderQsession.createTextMessage();
        _senderQcon.start();
        _isSenderInitialized = true;
    }

    public void initReceiver(Context ctx) throws JMSException, NamingException {
        _qconFactory = (QueueConnectionFactory) ctx.lookup(this._factoryName);
        _receiverQcon = _qconFactory.createQueueConnection();
        _receiverQsession = _receiverQcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        _receiverQueue = (Queue) ctx.lookup(_queueName);
        _qreceiver = _receiverQsession.createReceiver(_receiverQueue);
        _qreceiver.setMessageListener(this);
        _receiverQcon.start();
        _isReceiverInitialized = true;
    }
    
}
