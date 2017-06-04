package io.openmessaging.utils;

import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.DefaultKeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class ByteConvertUtil {

	static String leftToken = "{{";
	static String rightToken = "}}";
	static String keySplit = ":";
	static String entrySplit = "|";
	// static String mapSplit = "@@@";

	static String propSplit = "^";



	/**
	 * 方法名称:transMapToString 传入参数:Key-Value 返回值:String 形如
	 * {{key1:value1|key2:value2}}
	 */
	public static byte[] transMapToBytes(DefaultKeyValue dkv) {
		StringBuffer sb = new StringBuffer();
		sb.append(leftToken);
		for (Map.Entry<String, Object> entry : dkv.getKvs().entrySet()) {
			sb.append(entry.getKey().toString()).append(keySplit).append(entry.getValue().toString())
					.append(entrySplit);
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(rightToken);
		return sb.toString().getBytes();
	}

	public static String transMapToString(DefaultKeyValue dkv) {
		StringBuffer sb = new StringBuffer();
		sb.append(leftToken);
		for (Map.Entry<String, Object> entry : dkv.getKvs().entrySet()) {
			sb.append(entry.getKey().toString()).append(keySplit).append(entry.getValue().toString())
					.append(entrySplit);
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(rightToken);
		return sb.toString();
	}

	/**
	 * 方法名称:transStringToMap 传入参数:mapString 形如 {{key1:value1|key2:value2}}
	 * 返回值:Map
	 */
	public static Map<String, Object> transBytesToMap(byte[] bytes) {
		Map<String, Object> dkv = new HashMap<>();
		// validation

		String mapString = new String(bytes);
		String strMap = mapString.substring(2, mapString.length() - 2);

		StringTokenizer items;
		for (StringTokenizer entrys = new StringTokenizer(strMap, entrySplit); entrys.hasMoreTokens(); dkv
				.put(items.nextToken(), items.hasMoreTokens() ? ((Object) (items.nextToken())) : null))
			items = new StringTokenizer(entrys.nextToken(), keySplit);
		return dkv;
	}

	/**
	 * Message 转为 byte[]
	 * 
	 * @param message
	 * @return
	 */
	public static byte[] transMessageToBytes(DefaultBytesMessage message) {
		if (message == null){
			System.out.println("transMessageToBytes message is null");
			return null;
		}
		StringBuilder sb = new StringBuilder();
		sb.append(leftToken);
		for (String key : message.headers().keySet()) {
			sb.append(key).append(keySplit).append(message.headers().getString(key)).append(entrySplit);
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(rightToken);

		sb.append(propSplit);

		sb.append(leftToken);
		for (String key : message.properties().keySet()) {
			sb.append(key).append(keySplit).append(message.properties().getString(key)).append(entrySplit);
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(rightToken);

		sb.append(propSplit);
		byte[] bytes = sb.toString().getBytes();
		return byteMerger(bytes, message.getBody());

	}

	public static byte[] byteMerger(byte[] byte1, byte[] byte2) {
		byte[] byte3 = new byte[byte1.length + byte2.length];
		System.arraycopy(byte1, 0, byte3, 0, byte1.length);
		System.arraycopy(byte2, 0, byte3, byte1.length, byte2.length);
		return byte3;
	}
	
	public static byte[] byteMerger(byte[] byte1, byte[] byte2,byte[] byte3) {
		byte[] byte4 = new byte[byte1.length + byte2.length + byte3.length];
		System.arraycopy(byte1, 0, byte4, 0, byte1.length);
		System.arraycopy(byte2, 0, byte4, byte1.length, byte2.length);
		System.arraycopy(byte3, 0, byte4, byte1.length + byte2.length, byte3.length);
		return byte4;
	}

	public static DefaultBytesMessage transBytesToMessage(byte[] bytes) {

		String mapString = new String(bytes);
		String[] strItems = mapString.split("\\" + propSplit);
		if(strItems.length < 3){
			System.out.println("213");
		}
		DefaultBytesMessage message = new DefaultBytesMessage();
		message.setBody(strItems[2].getBytes());
		for (Map.Entry<String, Object> entry : transBytesToMap(strItems[0].getBytes()).entrySet()) {
			message.putHeaders(entry.getKey(), entry.getValue().toString());
		}

		for (Map.Entry<String, Object> entry : transBytesToMap(strItems[1].getBytes()).entrySet()) {
			message.putProperties(entry.getKey(), entry.getValue().toString());
		}
		return message;
	}
	
	/**
	 * int 转 Bytes
	 * @param values
	 * @return
	 */
	public static byte[] IntToBytes(int values) {
		byte[] buffer = new byte[4];
		for (int i = 0; i < 4; i++) {
			int offset = 32 - (i + 1) * 8;
			buffer[i] = (byte) ((values >> offset) & 0xff);
		}
		return buffer;
	}
	
	/**
	 * bytes 转 int
	 * @param buffer
	 * @return
	 */
	public static int BytesToInt(byte[] buffer,int offset) {
		int values = 0;
		for (int i = 0; i < 4; i++) {
			values <<= 8;
			values |= (buffer[offset + i] & 0xff);
		}
		return values;
	}
	
	public static int BytesToInt(byte[] buffer) {
		return BytesToInt(buffer,0);
	}
	
	/**
	 * long 转 bytes
	 * @param values
	 * @return
	 */
	public static byte[] LongToBytes(long values) {
		byte[] buffer = new byte[8];
		for (int i = 0; i < 8; i++) {
			int offset = 64 - (i + 1) * 8;
			buffer[i] = (byte) ((values >> offset) & 0xff);
		}
		return buffer;
	}
	
	/**
	 * bytes 转 long
	 * @param buffer
	 * @return
	 */
	public static long BytesToLong(byte[] buffer, int offset) {
		long values = 0;
		for (int i = 0; i < 8; i++) {
			values <<= 8;
			values |= (buffer[i + offset] & 0xff);
		}
		return values;
		
	}
	
	public static long BytesToLong(byte[] buffer) {
		return BytesToLong(buffer,0);
	}
//
//	public static void main(String[] args) {
//		DefaultKeyValue dkv = new DefaultKeyValue();
//		dkv.getKvs().put("key1", "value1");
//		dkv.getKvs().put("key2", "value2");
//		dkv.getKvs().put("key3", "value3");
//		byte[] bytes = transMapToBytes(dkv);
//		System.out.println("--------------------");
//		for (byte b : bytes)
//			System.out.print(b);
//		System.out.println();
//		System.out.println("--------------------");
//		Map<String, Object> dkv2 = new HashMap<>();
//		dkv2 = transBytesToMap(bytes);
//
//		for (Map.Entry<String, Object> entry : dkv2.entrySet())
//			System.out.println(entry.getKey() + "-------" + entry.getValue());
//
//		byte[] bodys = { 1, 2, 3, 4, 5, 6, 7 };
//		DefaultBytesMessage message = new DefaultBytesMessage(bodys);
//		message.headers().put("headKey11", "value11");
//		message.headers().put("headKey12", "value12");
//		message.properties().put("propKey21", "value21");
//		message.properties().put("propKey22", "value22");
//		byte[] bytes1 = transMessageToBytes(message);
//		System.out.println("--------------------");
//		for (byte b : bytes1)
//			System.out.print(b);
//		System.out.println();
//
//		DefaultBytesMessage message2 = transBytesToMessage(bytes1);
//		System.out.println("--------------------");
//
//		for (String key : message2.headers().keySet())
//			System.out.println(key + "-------" + message2.headers().getString(key));
//
//		for (String key : message2.properties().keySet())
//			System.out.println(key + "-------" + message2.properties().getString(key));
//
//		System.out.println("---------Int - Bytes-----------");
//		int x = 100;
//		byte[] bytes3 = IntToBytes(x);
//		System.out.println(bytes3);
//
//		System.out.println(BytesToInt(bytes3));
//
//		System.out.println("---------long - Bytes-----------");
//		long x2 = 100000000000000000L;
//		byte[] bytes4 = LongToBytes(x2);
//		System.out.println(bytes4);
//
//		System.out.println(BytesToLong(bytes4));
//
//		System.out.println("---------MessageIndex - Bytes-----------");
//		MessageIndex mi = new MessageIndex(100101000123213L, 10);
//		byte[] bytes5 = transMessageIndexToBytes(mi);
//		Print.print(bytes5);
//		MessageIndex mi2 = transBytesToMessageIndex(bytes5);
//		Print.print(mi2);
//	}
}
