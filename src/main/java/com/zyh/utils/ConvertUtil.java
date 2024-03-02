package com.zyh.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;


public class ConvertUtil {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConvertUtil.class);

    public static final Charset CHARSET = StandardCharsets.UTF_8;

    public static byte[] convertObjectToBytes(Object data){
        byte[] bytes = JSONObject.toJSONString(data).getBytes(CHARSET);
        return bytes;
    }

    public static <T> T readObjectFromBytes(byte[] data,Class<T> klass){
        String indexDataStr = new String(data, StandardCharsets.UTF_8);
//        LoggerUtil.info(LOGGER,"[ConvertUtil][readObjectFromBytes][class]: {}",klass.getName());
//        JSONObject.parseObject(indexDataStr,new TypeReference<TreeMap<String, Position>>(){})
        JSONObject object = JSONObject.parseObject(indexDataStr);
        T res = JSONObject.toJavaObject(object, klass);
        return res;
    }

    public static Object readMapObjectFromBytes(byte[] data,TypeReference typeReference){
        String indexDataStr = new String(data, StandardCharsets.UTF_8);
        Object o = JSONObject.parseObject(indexDataStr, typeReference);
        return o;
    }
}
