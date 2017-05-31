package io.openmessaging.demo;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;
import io.openmessaging.store.CachePage;

import java.util.concurrent.ArrayBlockingQueue;

public class DefaultProducer  implements Producer {

    private static String MESSAGE_DESTINATION = "D";

    private static int cachePagesSize = 5;
    private static int cachePageCapacity = 10 * 1024 * 1024;

    private MessageFactory messageFactory;
    private MessageStore messageStore;

    private ArrayBlockingQueue<CachePage> cachePages;
    private ArrayBlockingQueue<CachePage> cachePagesPool;
    private CachePage cachePageOfCurrent;

    private KeyValue properties;
    private Boolean hasBeenInitialized;

    private long producerID;

    public DefaultProducer(KeyValue properties) {
        this.messageFactory = new DefaultMessageFactory();
        this.messageStore = MessageStore.getInstance();
        this.cachePages = new ArrayBlockingQueue<>(cachePagesSize);
        this.cachePagesPool = new ArrayBlockingQueue<>(cachePagesSize);
        this.cachePageOfCurrent = null;

        this.properties = properties;
        this.hasBeenInitialized = false;
    }

    private void init(){
        this.producerID = Thread.currentThread().getId();

        for (int i=0; i<cachePagesSize; i++){
            cachePagesPool.add(new CachePage(cachePageCapacity, producerID));
        }
        cachePageOfCurrent = cachePagesPool.poll();
        this.messageStore.registerProducer(producerID, cachePages, cachePagesPool);
        this.hasBeenInitialized = true;
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
        if (!this.hasBeenInitialized){
            init();
        }

        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }

        String queueOtTopic;
        if (topic != null){
            queueOtTopic = topic;
        }else {
            queueOtTopic = queue;
        }
        DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage)message;
        defaultBytesMessage.putHeaders(MESSAGE_DESTINATION, queueOtTopic);

        boolean result = cachePageOfCurrent.putMessage( defaultBytesMessage);
        if (!result){
            try {
                //下面这两个操作都可能会阻塞
                cachePages.put(cachePageOfCurrent);
//                System.out.print("any pages: " + cachePages.size() + "_" + "pool : "+ cachePagesPool.size());
                cachePageOfCurrent = cachePagesPool.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("fail to apply for cache page ");
            }
            result = cachePageOfCurrent.putMessage(defaultBytesMessage);
            if (!result){
                System.out.println("fatal error : fail to store message into page cache");
            }
        }
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
        long producerID = Thread.currentThread().getId();
        try {
            cachePages.put(cachePageOfCurrent);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("fatal error : cachePages.put(cachePageOfCurrent) operation has been interrupted");
        }
        messageStore.deleteProducer(producerID);
        hasBeenInitialized = false;
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
