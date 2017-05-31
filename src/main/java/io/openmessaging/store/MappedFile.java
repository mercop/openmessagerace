package io.openmessaging.store;

import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.utils.ByteConvertUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MappedFile {
    public final static int defaultFileSize = 1024 * 1024;

    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    protected int fileSize;
    protected FileChannel fileChannel;
    private String filePath;
	private MappedByteBuffer mappedByteBuffer;

	public MappedFile(final String fileDir, String fileName, int mappedFileSize) {
		filePath = fileDir + fileName;
		fileSize = mappedFileSize;
		if (mappedFileSize <= 0){
		    fileSize = defaultFileSize;
        }

	    File dir = new File(fileDir);
	    try{
//	        long start = System.currentTimeMillis();
            if (!dir.exists() && (!dir.isDirectory())){
                dir.mkdirs();
            }
            File file = new File(filePath);
            if (!file.exists()){
                file.createNewFile();
            }
//            long end = System.currentTimeMillis();
//            System.out.println(end - start);
            this.fileChannel = new RandomAccessFile(this.filePath, "rw").getChannel();
//            long start = System.currentTimeMillis();
            this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
//            long end = System.currentTimeMillis();
//            System.out.println(end - start);

        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + this.filePath + " Failed. ");
        } catch (IOException e) {
            System.out.println("map file " + this.filePath + " Failed. ");
        }

	}

	public int getRestSpace(){
	    return this.fileSize - this.wrotePosition.get();
    }

    public int getPosition(){
	    return this.mappedByteBuffer.position();
    }

    public boolean appendMessage(final ByteBuffer data) {
	    if (this.mappedByteBuffer == null){
            try {
                //long start = System.currentTimeMillis();
                this.fileChannel = new RandomAccessFile(this.filePath, "rw").getChannel();
                this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                //long end = System.currentTimeMillis();
                //System.out.println("map "+(end - start));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.position()) <= this.fileSize) {
            int length = data.position();
            data.position(0);
            try {
                this.mappedByteBuffer.put(data);
            } catch (Throwable e) {
                System.out.println("Error occurred when append message to mappedFile.");
            }
            this.wrotePosition.addAndGet(data.limit());
            return true;
        }

        return false;
    }

    public int getFileSize(){
        return this.fileSize;
    }

    public void setInitialPosition(int newPosition){
        if (this.mappedByteBuffer == null){
            try {
                this.fileChannel = new RandomAccessFile(this.filePath, "rw").getChannel();
                this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.mappedByteBuffer.position(newPosition);
    }

    public DefaultBytesMessage getNextMessage(int offset, int size) {
        if (this.mappedByteBuffer == null){
            try {
                this.fileChannel = new RandomAccessFile(this.filePath, "rw").getChannel();
                this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        byte[] bytes = new byte[size];
        this.mappedByteBuffer.position((int)offset);
        this.mappedByteBuffer.get(bytes);
        DefaultBytesMessage message = ByteConvertUtil.transBytesToMessage(bytes);
        return message;
    }

    public void flushAndRelease(){
//        long start = System.currentTimeMillis();
//        this.mappedByteBuffer.force();
//        long end = System.currentTimeMillis();
//        System.out.println("flush " + (end - start));
//        try {
//            Thread.sleep(3);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

//        long start_1 = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer;
        try {
            Method getCleanerMethod = byteBuffer.getClass().getMethod("cleaner", new Class[0]);
            getCleanerMethod.setAccessible(true);
            sun.misc.Cleaner cleaner = (sun.misc.Cleaner)
                    getCleanerMethod.invoke(byteBuffer, new Object[0]);
            cleaner.clean();
            this.mappedByteBuffer = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
//        long end_1 = System.currentTimeMillis();
//        System.out.println("clean " + (end_1 - start_1));

    }

}
