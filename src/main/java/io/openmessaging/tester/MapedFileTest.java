package io.openmessaging.tester;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
/**
 * 内存映射文件
 * @author yunzhu.ydd
 *
 */
public class MapedFileTest {
    private   final String FILE_NAME = "D://mapedFile.txt";
    private   final long fileSize = 1024 * 1024;// 1M
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private File file;
    public  MapedFileTest() throws IOException {
        this.file = new File(FILE_NAME);
        this.fileChannel = new RandomAccessFile(this.FILE_NAME, "rw")
                .getChannel();
        //产生一定大小的文件，byte默认都为0。也就是说文件的byte都被初始化为0了
        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0,
                fileSize);
    }
    /**
     * 写
     * @param str
     */
    public void write(String str)
    {
        for (int i=0; i<10000; i++){
            mappedByteBuffer.put(str.getBytes());
        }
    }
    /**
     * 读文件
     */
    public void getAll()
    {
        System.out.println("capacity:"+mappedByteBuffer.capacity());
        for(int i=0;i<mappedByteBuffer.capacity();i++)
        {
            char c=(char)mappedByteBuffer.get(i);
            if(c!=' '&&c!=0)
                System.out.print(c);
        }
        System.out.println();
    }
    /**
     * 通过MappedByteBuffer操作文件
     * @throws IOException
     */
    public void test_写和读() throws IOException
    {
        System.out.println("capacity:"+mappedByteBuffer.capacity());
        write("hello world");
        getAll();
        fileChannel.close();
    }
    /**
     * MappedByteBuffer.slice()生成的是DirectByteBuffer,对该buffer写操作会被直接写到文件里面
     * @param str
     * @throws IOException
     */
    public void test_slic_写和读(String str) throws IOException
    {
        ByteBuffer buffer=mappedByteBuffer.slice();
        buffer.put(str.getBytes());
        System.out.println("mappedByteBuffer.clice产生的buffer类型为:"+buffer.getClass());
        getAll();
        fileChannel.close();
    }
    public static void main(String[]a) throws IOException
    {
        MapedFileTest mappedFile=new MapedFileTest();
        mappedFile. test_写和读();
        //mappedFile.test_slic_写和读("hahahahahah");
    }

}