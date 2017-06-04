package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.store.ReadMappedFile;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultPullConsumer implements PullConsumer {

    private KeyValue properties;
    private Queue<String> queue;

    private List<DefaultBytesMessage> list;
    private int index;
    private String currentTopic;

    private ReadMappedFile mappedFile;

    private AtomicBoolean readOver;
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message poll() {

//        System.out.println("begin to get message list --- 0");
//        if (list == null) {
//            System.out.println("list is null");
//        } else {
//            System.out.println("list size is : "+list.size());
//        }

        if (index == list.size() || list == null) {
//            System.out.println("begin to get message list --- 1 ");
            if (readOver.get()) {
                currentTopic = queue.poll();
                if (currentTopic == null) {
                    return null;
                }
                mappedFile = new ReadMappedFile(currentTopic);
                readOver.set(false);
            }
            if (list != null) {
                list.clear();
            }
//            System.out.println("begin to get message list --- 2 ");
            list = mappedFile.getMessageList(1000, readOver);
//            System.out.println("size of list : " + list.size());
            if (list.size() == 0) {
                currentTopic = queue.poll();
                if (currentTopic == null) {
                    return null;
                }
                mappedFile = new ReadMappedFile(currentTopic);
                readOver.set(false);
                list = mappedFile.getMessageList(1000, readOver);
                if (list.size() == 0) {
                    return null;
                }
            }
            index = 0;
        }
        return list.get(index ++);
    }


    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        this.queue = new LinkedList<>();
        queue.add(queueName);
        for (String topic : topics){
            queue.add(topic);
        }
        this.list = new ArrayList<>();
        this.index = 0;
        this.currentTopic = null;
        this.mappedFile = null;
        this.readOver = new AtomicBoolean(true);
    }

}
