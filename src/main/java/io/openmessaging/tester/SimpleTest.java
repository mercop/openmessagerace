package io.openmessaging.tester;

import java.util.HashMap;

/**
 * Created by sang on 2017/5/25.
 */
public class SimpleTest {
    public static void main(String[] args){
        HashMap<String, String> hashMap = new HashMap<>();
        String number =  hashMap.get("jack");
        if (number == null){
            hashMap.put("jack", "123");
        }
        int a=0;
    }
}
