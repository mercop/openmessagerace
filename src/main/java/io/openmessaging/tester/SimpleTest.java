package io.openmessaging.tester;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by sang on 2017/5/25.
 */
public class SimpleTest {

    static ArrayList<Integer> list = new ArrayList<>();

    public static void main(String[] args){

        Random random = new Random();
        int sum =0;
        for (int i=0; i<100000; i++){
            int b = random.nextInt() % 100;
            list.add(b);
            sum += b;
        }
        System.out.println(sum);
        int num = 10;

        Thread[] threads = new Thread[num];
        for (int i=0; i<num; i++){
            threads[i] = new Thread(new calculate());
        }
        for (int i=0; i<num; i++){
            threads[i].start();
        }

        for (int i=0; i<num; i++){
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int a=0;

    }

    static class calculate implements Runnable{

        @Override
        public void run() {
            int index = 0;
            int size = list.size();
            int sum = 0;
            for (int i=0; i<size; i++){
                sum += list.get(i);
            }
            System.out.println(Thread.currentThread().getId() + " : " + sum);
        }
    }
}
