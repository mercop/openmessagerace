package io.openmessaging.store;

import io.openmessaging.tester.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class WriteMappedFile {

    private static String fileDir = Constants.STORE_MESSAGE_PATH;

    public final static int MAPPED_BUFFER_SIZE_PER_TIME = 4 * 1024 * 1024;

    protected FileChannel fileChannel;

    private String filePath;
	private MappedByteBuffer mappedByteBuffer;

	//记录当前已用的文件长度，当map的时候进行更新
	private long totalLength;

	public WriteMappedFile(String queueOrTopic){

        this.filePath = fileDir + queueOrTopic + ".txt";
        File dir = new File(fileDir);
        try{
            if (!dir.exists() && (!dir.isDirectory())){
                dir.mkdirs();
            }
            File file = new File(filePath);
            if (!file.exists()){
                file.createNewFile();
            }
            this.fileChannel = new RandomAccessFile(this.filePath, "rw").getChannel();
            this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, MAPPED_BUFFER_SIZE_PER_TIME);
            this.totalLength = 0;
        } catch (FileNotFoundException e) {
            System.out.println("create file channel " + this.filePath + " Failed. ");
        } catch (IOException e) {
            System.out.println("map file " + this.filePath + " Failed. ");
        }
    }

    public void writeCachePage(CachePage cachePage){
        byte[] bytes = cachePage.toBytes();
        int length = cachePage.getAvaiableLength();

        //判断是否有mappedbytebuffer中是否有足够的剩余空间
        if (length > mappedByteBuffer.limit() - mappedByteBuffer.position()){
            //更新已用文件长度
            totalLength += mappedByteBuffer.position();
//            mappedByteBuffer.force();
            //清理内存
            ByteBuffer byteBuffer = this.mappedByteBuffer;
            try {
                Method getCleanerMethod = byteBuffer.getClass().getMethod("cleaner", new Class[0]);
                getCleanerMethod.setAccessible(true);
                sun.misc.Cleaner cleaner = (sun.misc.Cleaner)
                        getCleanerMethod.invoke(byteBuffer, new Object[0]);
                cleaner.clean();

            } catch (Exception e) {
                e.printStackTrace();
            }

            //以length为起点进行再map
            try {
                mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, totalLength,
                        MAPPED_BUFFER_SIZE_PER_TIME);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("fatal error : fail to map file");
            }
        }
        //存储cachepage的数据
        mappedByteBuffer.put(bytes, 0, length);
    }

    public void putInt(int value){
        this.mappedByteBuffer.putInt(value);
    }

    public void close(){
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
