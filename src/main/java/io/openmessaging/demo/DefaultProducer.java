package io.openmessaging.demo;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;
import io.openmessaging.store.ProducerCache;

public class DefaultProducer  implements Producer {

    private static int cacheSize = 30;

    private static int cachePagePoolSize = 120;

    private static int pageSize = 512 * 1024;

    private int num;

    private MessageFactory messageFactory;
    private WriteMessageStore writeMessageStore;
    private KeyValue properties;

    //第一次发送消息的时候才会被真正初始化
    private Boolean hasBeenRegistered;
    private ProducerCache producerCache;
    private long producerId;

    public DefaultProducer(KeyValue properties) {
        this.messageFactory = new DefaultMessageFactory();
        this.writeMessageStore = WriteMessageStore.getInstance();
        this.properties = properties;
        this.hasBeenRegistered = false;
        this.num = 200;
    }



    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {
        //根据实际场景，一个线程只能有一个messageProcuder，因为使用线程名称作为messageProducer的id
        if (!this.hasBeenRegistered){
            this.producerId = Thread.currentThread().getId();
            this.producerCache =  new ProducerCache(cacheSize, pageSize, cachePagePoolSize, producerId);
            this.writeMessageStore.registerProducer(producerId, producerCache);
            this.hasBeenRegistered = true;
        }

        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        
        producerCache.cacheMessage(topic != null ? topic : queue, (DefaultBytesMessage) message);
    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void flush() {
        //先flush cache
        producerCache.flush();
        //在从writemessagestore中删除
        writeMessageStore.deleteProcuder(producerId);
        this.hasBeenRegistered = false;
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
}
