package io.openmessaging.demo;

import io.openmessaging.store.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class WriteMessageStore {

    private static WriteMessageStore INSTANCE = new WriteMessageStore();

    private static int INITIAL_PRODUCER_SIZE = 32;

    private static int INITIAL_QUEUEANDTOPIC_SIZE = 128;

    private static int NUM_PAGECACHE_PER_DISPATCH = 3;

    private static int NUM_CACHEPAGE_PER_WRTIE = 4;

    private static int WRITE_CACHE_QUEUE_SIZE = 16;

    private static ConcurrentHashMap<Long, Object> registeredMap = new ConcurrentHashMap<>(INITIAL_PRODUCER_SIZE);

    private static ConcurrentHashMap<Long, Object> deletedMap = new ConcurrentHashMap<>(INITIAL_PRODUCER_SIZE);

    private static ConcurrentHashMap<Long, ArrayBlockingQueue<CachePage>> producerCachePagesMap
            = new ConcurrentHashMap<>(INITIAL_PRODUCER_SIZE);

    private static ConcurrentHashMap<Long, ArrayBlockingQueue<CachePage>> producerCachePoolsMap
            = new ConcurrentHashMap<>(INITIAL_PRODUCER_SIZE);


    //对应于每个queue或者topic的map结构
    private static ConcurrentHashMap<String, ArrayBlockingQueue<CachePage>> cachePages
            = new ConcurrentHashMap<>(INITIAL_QUEUEANDTOPIC_SIZE);

    private static ConcurrentHashMap<String, WriteMappedFile> fileQueueMap
            = new ConcurrentHashMap<>(INITIAL_QUEUEANDTOPIC_SIZE);


    //标志生产者是否全部终止
    private static volatile boolean isAllTerminated = false;

    private Thread dispatchService;

    private Thread writeFileService;

    public synchronized void registerProducer(long producerId, ProducerCache producerCache){
        this.producerCachePagesMap.put(producerId, producerCache.getCacheQueue());
        this.producerCachePoolsMap.put(producerId, producerCache.getCachePagePool());
        this.registeredMap.put(producerId, new Object());
        if (registeredMap.size() == 10){
            dispatchService = new Thread(new DispatchService());
            dispatchService.setName("DispatchService_Thread");
            dispatchService.start();

            writeFileService = new Thread(new WriteFileService());
            writeFileService.setName("WriteFileService_Thread");
            writeFileService.start();
        }
    }

    public synchronized void deleteProcuder(long procuderId){
        this.deletedMap.put(procuderId, new Object());

        //当所有的生产者都终止后，（终止之前都会对相应的producercache进行flush操作），
        //设置标志位表示，所有生产者已经被终止了
        if (deletedMap.size() == registeredMap.size()){
            isAllTerminated = true;
        }
    }

    //将不同的cachepage分发到对应的队列中
    class DispatchService implements Runnable{

        @Override
        public void run() {
            ArrayBlockingQueue<CachePage> queue = null;
            CachePage cachePage = null;
            ArrayBlockingQueue<CachePage> queueOfSpecifiedQueue = null;
            String queueOrTopic = null;
            while (true){
                for (long producerId : producerCachePagesMap.keySet()){
                    queue = producerCachePagesMap.get(producerId);
                    for (int i=0; i<NUM_PAGECACHE_PER_DISPATCH; i++){
                        cachePage = queue.poll();
                        if (cachePage == null){
                            break;
                        }
                        try {
                            queueOrTopic = cachePage.getQueueOrTopic();
                            queueOfSpecifiedQueue = cachePages.get(queueOrTopic);
                            if (queueOfSpecifiedQueue == null){
                                queueOfSpecifiedQueue = new ArrayBlockingQueue<>(WRITE_CACHE_QUEUE_SIZE);
                                cachePages.put(queueOrTopic, queueOfSpecifiedQueue);
                                fileQueueMap.put(queueOrTopic, new WriteMappedFile(queueOrTopic));
                            }
                            queueOfSpecifiedQueue.put(cachePage);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.out.println("fatal error : fail to put cache page to cachePage queue");
                        }
                    }
                }
                //当所有的生产都被终止，则退出循环
                if (isAllTerminated){
                    System.out.println("all producers have been terminated");
                    break;
                }
            }
            //分发剩余所有的producercache
            for (long producerId : producerCachePagesMap.keySet()){
                queue = producerCachePagesMap.get(producerId);
                while (true){
                    cachePage = queue.poll();
                    if (cachePage == null){
                        break;
                    }
                    queueOrTopic = cachePage.getQueueOrTopic();
                    queueOfSpecifiedQueue = cachePages.get(queueOrTopic);
                    try {
                        queueOfSpecifiedQueue.put(cachePage);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        System.out.println("fatal error : fail to put cache page to cachePage queue");
                    }
                }
            }
            System.out.println("dispatch service has been terminated");
        }
    }

    //写数据到mappedfile
    class WriteFileService implements Runnable{

        @Override
        public void run() {
            ArrayBlockingQueue<CachePage> cachePageQueue = null;
            ArrayBlockingQueue<CachePage> cachePagePoolQueue = null;
            WriteMappedFile mappedFile = null;
            CachePage cachePage = null;
            while (true){
                boolean flag = true;
                for (String queueOrTopic : cachePages.keySet()){
                    cachePageQueue = cachePages.get(queueOrTopic);
                    mappedFile = fileQueueMap.get(queueOrTopic);
                    if (mappedFile == null) {
                        fileQueueMap.put(queueOrTopic, new WriteMappedFile(queueOrTopic));
                        mappedFile = fileQueueMap.get(queueOrTopic);
                    }

                    if (cachePageQueue != null){
                        for(int i=0; i<NUM_CACHEPAGE_PER_WRTIE; i++){
                            cachePage = cachePageQueue.poll();
                            if (cachePage == null){
                                break;
                            }
                            flag = false;
                            mappedFile.writeCachePage(cachePage);
                            try {
                                cachePage.clear();
                                producerCachePoolsMap.get(cachePage.getProducerId()).put(cachePage);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                System.out.println("fatal error : fail to put cache page into cache page pool");
                            }
                        }
                    }
                }
                if (isAllTerminated && flag){
                    System.out.println("write file service has been terminated");
                    break;
                }
            }
            for (String queueOrTopic : fileQueueMap.keySet()){
                WriteMappedFile writeMappedFile = fileQueueMap.get(queueOrTopic);
                writeMappedFile.putInt(-1);
//                writeMappedFile.close();
            }
        }
    }
    private WriteMessageStore() {
    }

    public static WriteMessageStore getInstance() {
        if (INSTANCE == null) {
            synchronized (WriteMessageStore.class) {
                if (INSTANCE == null) {
                    INSTANCE = new WriteMessageStore();
                }
            }
        }
        return INSTANCE;
    }
}
