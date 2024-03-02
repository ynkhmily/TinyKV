package com.zyh.service;

public interface KV extends Cloneable{

    void set(String key,String values);

    String get(String key);

    void rm(String key);
}
