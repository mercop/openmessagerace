package io.openmessaging.store;

import io.openmessaging.demo.DefaultBytesMessage;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by sang on 2017/5/27.
 */
public class ProducerCache {

    private int cacheSize;

    private int pageSize;

    private long producerId;

    private ArrayBlockingQueue<CachePage> cacheQueue;

    private ArrayBlockingQueue<CachePage> cachePagePool;

    private HashMap<String, CachePage> cacheMap;

    public ProducerCache(int cacheSize, int pageSize, int cachePagePoolSize, long producerId){
        this.cacheSize = cacheSize;
        this.pageSize = pageSize;
        this.producerId = producerId;
        this.cachePagePool = new ArrayBlockingQueue<>(cachePagePoolSize);
        this.cacheQueue = new ArrayBlockingQueue<>(cacheSize);
        this.cacheMap = new HashMap<>(150);
    }

    public void cacheMessage(String queueOrTopic, DefaultBytesMessage defaultBytesMessage){
        CachePage cachePage = cacheMap.get(queueOrTopic);
        if (cachePage == null){
            cachePage = new CachePage(pageSize, producerId);
            cachePage.setQueueOrTopic(queueOrTopic);
            cacheMap.put(queueOrTopic, cachePage);
        }
        boolean result = cachePage.putMessage(defaultBytesMessage);
        if (!result){
            try {
                //下面这两行代码是可能会出现阻塞的，阻塞时，生产者线程进入waiting状态
                cacheQueue.put(cachePage);
                cachePage = cachePagePool.take();
                cachePage.setQueueOrTopic(queueOrTopic);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("fatal error : fail to apply for a page cache");
            }
            result = cachePage.putMessage(defaultBytesMessage);
            if (!result){
                System.out.println("fatal error : fail to store message into cache page");
            }
        }
    }

    public void flush(){
        for (String queueOrTopic : cacheMap.keySet()){
            try {

                cacheQueue.put(cacheMap.get(queueOrTopic));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        cacheMap.clear();
    }

    public ArrayBlockingQueue<CachePage> getCacheQueue(){
        return cacheQueue;
    }

    public ArrayBlockingQueue<CachePage> getCachePagePool(){
        return cachePagePool;
    }
}
