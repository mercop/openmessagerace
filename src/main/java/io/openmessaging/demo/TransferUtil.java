package io.openmessaging.demo;

import io.openmessaging.MessageHeader;
import io.openmessaging.tester.Constants;

import java.util.StringTokenizer;

/**
 * Created by sang on 2017/5/28.
 */
public class TransferUtil {

	private static String keySplit = ":";
    private static String entrySplit = "|";

    private static String propSplit = "^";

    private StringBuffer stringBuffer;
   
    
    private StringTokenizer stringTokenizer;

    public TransferUtil() {
        stringBuffer = new StringBuffer();
    }
    //基本消息格式：MessageId|TOPIC_ID^[PRO_OFFSET|randomKey:radomValue^body
    //header中默认key为[Topic,MessageId],properties中默认key为[PRO_OFFSET]
    //Demo:b1369toto6c|Q7^0_5|h0qev:wc5ta|d8rtv:sk68a^
    public byte[] transMessageAttributeToBytes(DefaultBytesMessage message) {
        if (message == null){
            System.out.println("transMessageAttributeToBytes message is null");
            return null;
        }
        
        stringBuffer.setLength(0);
        
        String messageId = message.headers().getString(MessageHeader.MESSAGE_ID);
        stringBuffer.append(messageId);
        
        stringBuffer.append(propSplit);
        String proOffset = message.properties().getString(Constants.PRO_OFFSET);

        if(proOffset != null){
        	stringBuffer.append(message.properties().getString(Constants.PRO_OFFSET).substring(8));
            stringBuffer.append(entrySplit);
        }
        
        for (String key : message.properties().keySet()) {
        	if(!key.equals(Constants.PRO_OFFSET))
        		stringBuffer.append(key).append(keySplit).append(message.properties().getString(key)).append(entrySplit);
        }
        stringBuffer.deleteCharAt(stringBuffer.length() - 1);
        stringBuffer.append(propSplit);
        return stringBuffer.toString().getBytes();
    }
    
    public DefaultBytesMessage transBytesToMessage(byte[] bytes){
    	
//    	System.out.println(new String(bytes));
    	if (bytes == null || bytes.length == 0){
            System.out.println("transferByteToMessage bytes is null");
            return null;
        }
    	
    	String mapString = new String(bytes);
    	DefaultBytesMessage message = new DefaultBytesMessage();
    	stringTokenizer = new StringTokenizer(mapString,propSplit);  	
    	if(stringTokenizer.countTokens() < 3)
			System.out.println("Error occurred in the transfer process");
    	
    	//Message header decode
    	if(stringTokenizer.hasMoreTokens()){
    		String header = stringTokenizer.nextToken();
    		message.headers().put("MessageId", header);
    	}
    	if(stringTokenizer.hasMoreTokens()){
			String propertiesStr = stringTokenizer.nextToken();
			
			String[] properties = propertiesStr.split("\\"+entrySplit);
			if(properties.length < 1)
				System.out.println("Error occurred in the transfer properties process");
			message.properties().put(Constants.PRO_OFFSET, Constants.PRO_PRE + properties[0]);
			StringTokenizer items;
			for(int i = 1; i < properties.length; i ++){
				items = new StringTokenizer(properties[i],keySplit);
				message.properties().put(items.nextToken(), items.hasMoreTokens() ? ((String) (items.nextToken())) : null);
			}
    	}
    	if(stringTokenizer.hasMoreTokens()){
    		message.setBody(stringTokenizer.nextToken().getBytes());
    	}

		return message;
    }

}
