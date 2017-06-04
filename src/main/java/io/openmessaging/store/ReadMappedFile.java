package io.openmessaging.store;

import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.TransferUtil;
import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by sang on 2017/6/4.
 */
public class ReadMappedFile {

    private static String fileDir = Constants.STORE_MESSAGE_PATH;

    public final static int MAPPED_BUFFER_SIZE_PER_TIME = 16 * 1024 * 1024;

    public final static int THRESH =  MAPPED_BUFFER_SIZE_PER_TIME - 512 * 1024;

    private TransferUtil transferUtil;

    protected FileChannel fileChannel;

    private String filePath;
    private MappedByteBuffer mappedByteBuffer;

    private long totalLength;

    private int mappedTimes;
    private String queueOrTopic;
    private String TYPE;

    public ReadMappedFile(String queueOrTopic){

        this.queueOrTopic = queueOrTopic;
        this.transferUtil = new TransferUtil();
        if(queueOrTopic.startsWith("Q"))
            TYPE = MessageHeader.QUEUE;
        else
            TYPE = MessageHeader.TOPIC;

        this.filePath = fileDir + queueOrTopic + ".txt";
        File dir = new File(fileDir);
        try{
            File file = new File(filePath);
            if (!file.exists()){
                System.out.println("fatal error : fail to find the data file");
            }
            this.fileChannel = new RandomAccessFile(this.filePath, "rw").getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, MAPPED_BUFFER_SIZE_PER_TIME);
            this.mappedTimes = 1;
            this.totalLength = 0;
            System.out.println("mapped times " + mappedTimes);
        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + this.filePath + " Failed. ");
        } catch (IOException e) {
            System.out.println("map file " + this.filePath + " Failed. ");
        }
    }

    public List<DefaultBytesMessage> getMessageList(int number, AtomicBoolean flag){
        List<DefaultBytesMessage> list = new ArrayList<>();
        int length = 0;

        while (true){
            length = mappedByteBuffer.getInt();
            if (length <= 0){
                flag.set(true);
                break;
            }
            byte[] bytes_1 = new byte[length];
            if (mappedByteBuffer.position() + length > mappedByteBuffer.limit()){
                System.out.println("fatal error : " + mappedByteBuffer.position() + "   " + length + "   " + mappedByteBuffer.limit());
                System.out.println("list size " + list.size());
                break;
            }
            mappedByteBuffer.get(bytes_1, 0, length);
//            System.out.println(queueOrTopic +  "begin to parse message");
            DefaultBytesMessage message = transferUtil.transBytesToMessage(bytes_1);
            message.headers().put(TYPE, this.queueOrTopic);

            list.add(message);
//            System.out.print( queueOrTopic + "_" + list.size() +"_");
            if (list.size() == number){
                break;
            }
            if (mappedByteBuffer.position() > THRESH) {

                this.totalLength += mappedByteBuffer.position();
                try {
                    Method getCleanerMethod = mappedByteBuffer.getClass().getMethod("cleaner", new Class[0]);
                    getCleanerMethod.setAccessible(true);
                    sun.misc.Cleaner cleaner = (sun.misc.Cleaner)
                            getCleanerMethod.invoke(mappedByteBuffer, new Object[0]);
                    cleaner.clean();

                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                            totalLength, MAPPED_BUFFER_SIZE_PER_TIME);
                    mappedTimes ++;
                    System.out.println("mapped times " + mappedTimes);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (list.size() == 0) {
            flag.set(true);
        }
        return list;
    }

}
