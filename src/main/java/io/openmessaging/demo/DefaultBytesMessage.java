package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage {

    private DefaultKeyValue headers;
    private DefaultKeyValue properties;
    private byte[] body;


    public DefaultBytesMessage() {
        this.headers = new DefaultKeyValue();
        this.properties = new DefaultKeyValue();
        this.body = null;
    }

    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override
    public void clear() {
        headers.clear();
        properties.clear();
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        properties.put(key, value);
        return this;
    }
}
