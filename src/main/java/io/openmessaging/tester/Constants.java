package io.openmessaging.tester;

public class Constants {

    public final static String STORE_PATH = System.getProperty("store.path", "D:\\tmp");
    public final static int PRO_NUM = Integer.valueOf(System.getProperty("pro.num", "10"));
    public final static int CON_NUM = Integer.valueOf(System.getProperty("con.num", "10"));
    public final static String PRO_PRE = System.getProperty("pro.pre","PRODUCER");
    public final static int PRO_MAX = Integer.valueOf(System.getProperty("pro.max","4000000"));
    public final static String CON_PRE = System.getProperty("con.pre", "CONSUMER_");
    public final static String TOPIC_PRE = System.getProperty("topic.pre", "TOPIC_");
    public final static String QUEUE_PRE = System.getProperty("topic.pre", "QUEUE_");
    
    public final static int MAPPEDFILE_SIZE = 4 * 1024 * 1024;
    
    public final static int MI_MAPPEDFILE_SIZE = 1024 * 1024;
    
    public final static int MESSAGEINDEX_SIZE = 16;

    public final static String PRO_OFFSET = "PRO_OFFSET";

    public final static String MESSAGEQUEUE_NAME = "MESSAGE";
    
    public final static String STORE_MESSAGE_PATH = STORE_PATH +"\\ms\\";
    
    public final static String STORE_INDEX_PATH = STORE_PATH +"\\index\\";

}
