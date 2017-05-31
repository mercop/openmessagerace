package io.openmessaging.demo;

import io.openmessaging.store.CachePage;
import io.openmessaging.tester.Constants;
import io.openmessaging.utils.ByteConvertUtil;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageStore {

//    private static final MessageStore INSTANCE = new MessageStore();
    private static volatile MessageStore INSTANCE ;

    private static int numOfCachePagesWrittenPerTime = 3;

    private static long mappedBufferSizePerTime = 128 * 1024 * 1024;

    private static int mappedTimes = 0;

    private static ConcurrentHashMap<Long, Object> registeredMap = new ConcurrentHashMap<>(256);

    private static ConcurrentHashMap<Long, Object> deletedMap = new ConcurrentHashMap<>(256);

    private static ConcurrentHashMap<Long, ArrayBlockingQueue<CachePage>> producerCachePagesMap =
            new ConcurrentHashMap<>(256);

    private static ConcurrentHashMap<Long, ArrayBlockingQueue<CachePage>> producerCachePoolMap =
            new ConcurrentHashMap<>(256);

    private MappedByteBuffer mappedByteBuffer;

    private FileChannel fileChannel;

    private AtomicBoolean allProducerHaveBeenDeleted;

    private AtomicBoolean writeServiceHasBeenStarted;

    private Thread writeServiceThread;

    public synchronized void registerProducer(long producerId,  ArrayBlockingQueue<CachePage> cachePages,
                                 ArrayBlockingQueue<CachePage> cachePool){
        this.producerCachePagesMap.put(producerId, cachePages);
        this.producerCachePoolMap.put(producerId, cachePool);
        this.registeredMap.put(producerId, new Object());

        //注册的生产者数量到达一定个数后， 启动写消息线程
        if (registeredMap.size() > 5 && (!writeServiceHasBeenStarted.get())){
            writeServiceThread.start();
            writeServiceHasBeenStarted.set(true);
        }
    }

    public void deleteProducer(long producerId){
        //清理生产者的cache
        this.deletedMap.put(producerId, new Object());
        if (deletedMap.size() == registeredMap.size()){
            allProducerHaveBeenDeleted.set(true);
        }
    }

    public static MessageStore getInstance() {
        if (INSTANCE == null) {
            synchronized (MessageStore.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MessageStore();
                }
            }
        }
        return INSTANCE;
    }

    private MessageStore() {
        allProducerHaveBeenDeleted = new AtomicBoolean(false);
        writeServiceHasBeenStarted = new AtomicBoolean(false);
        writeServiceThread = new Thread(new WriteFileService());
        writeServiceThread.setName("writeServiceThread");
        File dir = new File(Constants.STORE_MESSAGE_PATH);
        if (!dir.exists()){
            dir.mkdirs();
        }
        File dataFile = new File(Constants.STORE_MESSAGE_PATH + "data.txt");
        if (!dataFile.exists()){
            try {
                dataFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("fatal error : fail to create data file");
            }
        }
        try {
            fileChannel = new RandomAccessFile(dataFile, "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("fatal error : fail to get file channel");
        }
        try {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                    mappedTimes * mappedBufferSizePerTime, mappedBufferSizePerTime);
            mappedTimes ++;
            //System.out.println("mapped times " + mappedTimes);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("fatal error : fail to get mappedbytebuffer");
        }
    }

    class WriteFileService implements Runnable{

        @Override
        public void run() {
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ArrayBlockingQueue<CachePage> cachePages = null;
            CachePage cachePage = null;
            while (true){
                //对生产者缓存队列进行遍历
                for (long producerId : producerCachePagesMap.keySet()){
                    cachePages = producerCachePagesMap.get(producerId);

                    if (cachePages != null){
                        //写一定个数的缓存也到文件中
                        //System.out.print(" not null " + cachePages.size());
                        for (int i=0; i<numOfCachePagesWrittenPerTime; i++){
                            cachePage = cachePages.poll();
                            if (cachePage == null){
                                break;
                            }
                            putData(cachePage);
                            cachePage.clear();
                            try {
                                producerCachePoolMap.get(producerId).put(cachePage);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                System.out.println("fatal error : producerCachePoolMap.get(producerId).put(cachePage) " +
                                        "operation has been interrupted ");
                            }
                        }

                    }else {
                        System.out.println(" ** null **");
                    }
                }
                if (allProducerHaveBeenDeleted.get()){
                    break;
                }
            }

            for (long producerId : producerCachePagesMap.keySet()) {
                cachePages = producerCachePagesMap.get(producerId);
                while (true){
                    cachePage = cachePages.poll();
                    if (cachePage == null){
                        break;
                    }
                    putData(cachePage);
                }
            }
            try {
               fileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("fatal error : fail to close BufferedOutputStream");
            }
            System.out.println("write service is over");
        }
    }

    private void putData(CachePage cachePage){
        int length = cachePage.getAvaiableLength();
        if (length > mappedBufferSizePerTime - mappedByteBuffer.position()){
            try {
                ByteBuffer byteBuffer = mappedByteBuffer;
                Method getCleanerMethod = byteBuffer.getClass().getMethod("cleaner", new Class[0]);
                getCleanerMethod.setAccessible(true);
                sun.misc.Cleaner cleaner = (sun.misc.Cleaner)
                        getCleanerMethod.invoke(byteBuffer, new Object[0]);
                cleaner.clean();
                this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                        mappedTimes * mappedBufferSizePerTime, mappedBufferSizePerTime);
                mappedTimes ++;
                //System.out.println( "mapped times " + mappedTimes);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//        System.out.println("length " + length + " rest "+ ( mappedBufferSizePerTime - mappedByteBuffer.position()));
        mappedByteBuffer.put(ByteConvertUtil.IntToBytes(length));
        mappedByteBuffer.put(cachePage.toBytes(), 0, length);
    }
}
