package io.openmessaging.demo;

/**
 * Created by sang on 2017/5/28.
 */
public class TransferMessageAttributeToBytes {

/*    private static String leftToken = "{{";
    private static String rightToken = "}}";*/
    private static String keySplit = ":";
    private static String entrySplit = "|";

    private static String propSplit = "^";

    private StringBuffer stringBuffer;

    public TransferMessageAttributeToBytes() {
        stringBuffer = new StringBuffer();
    }

    public byte[] transMessageAttributeToBytes(DefaultBytesMessage message) {
        if (message == null){
            System.out.println("transMessageAttributeToBytes message is null");
            return null;
        }
        stringBuffer.setLength(0);
        //stringBuffer.append(leftToken);
        for (String key : message.headers().keySet()) {
            stringBuffer.append(key).append(keySplit).append(message.headers().getString(key)).append(entrySplit);
        }
        stringBuffer.deleteCharAt(stringBuffer.length() - 1);
        //stringBuffer.append(rightToken);

        stringBuffer.append(propSplit);

        //stringBuffer.append(leftToken);
        for (String key : message.properties().keySet()) {
            stringBuffer.append(key).append(keySplit).append(message.properties().getString(key)).append(entrySplit);
        }
        stringBuffer.deleteCharAt(stringBuffer.length() - 1);
        //stringBuffer.append(rightToken);

        return stringBuffer.toString().getBytes();
    }

}
