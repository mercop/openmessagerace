package io.openmessaging.store;

import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.TransferUtil;

import java.nio.ByteBuffer;

/**
 * Created by sang on 2017/5/25.
 */
public class CachePage {

    private static int INT_BYTE_LENGTH = 4;

    private String queueOrTopic;
    private int size;
    private long producerId;
    private byte[] bytes;
    private ByteBuffer byteBuffer;

    private TransferUtil transferUtil;

    public CachePage(long producerId){
        this.producerId = producerId;
    }

    public CachePage(byte[] bytes){
        this.bytes = bytes;
        this.byteBuffer = ByteBuffer.wrap(bytes);
        this.producerId = byteBuffer.getLong();
        this.transferUtil = new TransferUtil();
    }

    public CachePage(int pageSize, long producerId) {
        this.size = pageSize;
        this.producerId = producerId;
        this.bytes = new byte[pageSize];
        this.byteBuffer = ByteBuffer.wrap(bytes);
        this.transferUtil = new TransferUtil();
    }

    public boolean putMessage(DefaultBytesMessage message){
        byte[] attributeBytes = transferUtil.transMessageAttributeToBytes(message);
        int length_1 = attributeBytes.length;
        int length_2 = message.getBody().length;
        int totalLength = length_1 + length_2;
        if ( totalLength  + INT_BYTE_LENGTH > size - byteBuffer.position()){
            return false;
        }
        byteBuffer.putInt(totalLength);
        byteBuffer.put(attributeBytes);
        byteBuffer.put(message.getBody());
        return true;
    }

    //返回null，表示该cachePage的数据已经解析完毕了
    public DefaultBytesMessage getNextMessage(){
        if (byteBuffer.position() >= byteBuffer.limit()){
            return null;
        }

        int totalLength = byteBuffer.getInt();
        byte[] bytes = new byte[totalLength];
        byteBuffer.get(bytes, 0, totalLength);

        return transferUtil.transBytesToMessage(bytes);
    }

    public void clear(){
        queueOrTopic = null;
        byteBuffer.position(0);
    }

    public int getAvaiableLength(){
        return byteBuffer.position();
    }

    public byte[] toBytes(){
        return this.byteBuffer.array();
    }

    public long getProducerId() {
        return producerId;
    }

    public String getQueueOrTopic() {
        return queueOrTopic;
    }

    public void setQueueOrTopic(String queueOrTopic) {
        this.queueOrTopic = queueOrTopic;
    }
}
