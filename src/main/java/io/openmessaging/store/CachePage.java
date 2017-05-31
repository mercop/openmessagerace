package io.openmessaging.store;

import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.TransferMessageAttributeToBytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sang on 2017/5/25.
 */
public class CachePage {

    private static int INT_BYTE_LENGTH = 4;

    private int size;
    private long producerId;
    private byte[] bytes;
    private ByteBuffer byteBuffer;

    private TransferMessageAttributeToBytes transferUtil;


    public CachePage(int pageSize, long producerId) {
        this.size = pageSize;
        this.producerId = producerId;
        this.bytes = new byte[pageSize];
        this.byteBuffer = ByteBuffer.wrap(bytes);
        this.transferUtil = new TransferMessageAttributeToBytes();
        byteBuffer.putLong(producerId);
    }

    public boolean putMessage(DefaultBytesMessage message){
        byte[] attributeBytes = transferUtil.transMessageAttributeToBytes(message);
        int length_1 = attributeBytes.length;
        int length_2 = message.getBody().length;
        int totalLength = length_1 + length_2 + INT_BYTE_LENGTH * 3;
        if ( totalLength > size - byteBuffer.position()){
            return false;
        }
        byteBuffer.putInt(totalLength);
        byteBuffer.putInt(length_1);
        byteBuffer.put(attributeBytes);
        byteBuffer.putInt(length_2);
        byteBuffer.put(message.getBody());
        return true;
    }

    public void clear(){
        byteBuffer.position(0);
        byteBuffer.putLong(producerId);
    }
    public int getAvaiableLength(){
        return byteBuffer.position();
    }
    public byte[] toBytes(){
        return this.byteBuffer.array();
    }

}
