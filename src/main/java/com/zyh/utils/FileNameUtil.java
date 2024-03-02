package com.zyh.utils;

import java.util.UUID;

public class FileNameUtil {

    public static String getFileName(){
        return UUID.randomUUID().toString().replace("-","").toLowerCase();
    }
}
