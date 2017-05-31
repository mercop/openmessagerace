package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;

public class DefaultMessageFactory implements MessageFactory {

    private DefaultBytesMessage defaultBytesMessage;

    public DefaultMessageFactory(){
        this.defaultBytesMessage = new DefaultBytesMessage();
    }

    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        defaultBytesMessage.clear();
        defaultBytesMessage.setBody(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        defaultBytesMessage.putProperties(MessageHeader.TOPIC, topic);
        return defaultBytesMessage;
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        defaultBytesMessage.clear();
        defaultBytesMessage.setBody(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
        defaultBytesMessage.putProperties(MessageHeader.QUEUE, queue);
        return defaultBytesMessage;
    }
}
