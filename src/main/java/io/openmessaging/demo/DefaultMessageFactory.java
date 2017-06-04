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
//        defaultBytesMessage.putHeaders(MessageHeader.MESSAGE_ID, "b1369toto6c1");
//        defaultBytesMessage.putProperties("PRO_OFFSET", "PRODUCER0_5");
//        defaultBytesMessage.putProperties("h0qev", "wc5ta");
//        defaultBytesMessage.putProperties("d8rtv", "sk68a");
        return defaultBytesMessage;
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        defaultBytesMessage.clear();
        defaultBytesMessage.setBody(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
//        defaultBytesMessage.putHeaders(MessageHeader.MESSAGE_ID, "b1369toto6c1");
//        defaultBytesMessage.putProperties("PRO_OFFSET", "PRODUCER0_5");
//        defaultBytesMessage.putProperties("h0qev", "wc5ta");
//        defaultBytesMessage.putProperties("d8rtv", "sk68a");
        return defaultBytesMessage;
    }
}
