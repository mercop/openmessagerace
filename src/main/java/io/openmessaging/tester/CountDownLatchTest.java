package io.openmessaging.tester;

import java.util.concurrent.CountDownLatch;

/**
 * Created by sang on 2017/5/22.
 */
public class CountDownLatchTest {
    static CountDownLatch countDownLatch = new CountDownLatch(3);

    public static void main(String[] args){
        new Thread(new ThreadTest1()).start();
        new Thread(new ThreadTest2()).start();
        new Thread(new ThreadTest3()).start();

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("over");

    }


    static class ThreadTest1 implements Runnable{

        @Override
        public void run() {
            for(int i=0; i<100; i++){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            countDownLatch.countDown();
        }
    }

    static class ThreadTest2 implements Runnable{

        @Override
        public void run() {
            for(int i=0; i<100; i++){
                try {
                    Thread.sleep(80);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            countDownLatch.countDown();
        }
    }

    static class ThreadTest3 implements Runnable{

        @Override
        public void run() {
            for(int i=0; i<100; i++){
                try {
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            countDownLatch.countDown();
        }
    }
}
