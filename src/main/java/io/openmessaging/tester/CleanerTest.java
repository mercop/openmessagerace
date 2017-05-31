package io.openmessaging.tester;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by sang on 2017/5/23.
 */
public class CleanerTest {

    public static void main(String[] args){
        String filePath = "d:/tmp/test.txt";
        File file = new File(filePath);
        if (!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileChannel fileChannel;
        MappedByteBuffer mappedByteBuffer = null;
        while(true){
            try{
                fileChannel = new RandomAccessFile(filePath, "rw").getChannel();
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 50 * 1024*1024);
            }catch (FileNotFoundException e){
                System.out.println("file not found");
            }catch (IOException e){
                System.out.println("IO Exception");
            }
            int a=0;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            clearMappedByteBuffer(mappedByteBuffer);
            int b=0;
        }

    }

    public static void clearMappedByteBuffer(MappedByteBuffer byteBuffer){
        try {
            Method getCleanerMethod = byteBuffer.getClass().getMethod("cleaner", new Class[0]);
            getCleanerMethod.setAccessible(true);
            sun.misc.Cleaner cleaner = (sun.misc.Cleaner)
                    getCleanerMethod.invoke(byteBuffer, new Object[0]);
            cleaner.clean();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
