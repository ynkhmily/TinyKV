package com.zyh.model.constant;

public class Constant {

    // bloomfilter的初始大小
    public static final int BLOOMFILTER_INIT_SIZE = 100;

    // bloomfilter的错误率
    public static final double BLOOMFILTER_WRONG_RATE = 0.001;

    // 稀疏索引构建的大小
    public static final long SPARSE_INDEX_PART_SIZE = 10;

    public static final String RW = "rw";

    // L0 层SSTable文件的最大值（MB）
    public static final int MAX_FILE_SIZE = 128;

    // 每一层最大的文件数量 = MAX_FILE_NUM * max(level * ratio,1)
    public static final int MAX_FILE_NUM = 2;

    // 每一个文件存储的记录数 = MAX_ITEM_NUM * max(level * ratio,1)
    public static final int MAX_ITEM_NUM = 100;

    // 放大因子
    public static final int RATIO = 2;

    public static final long VERSION = 0;

    // 内存中只读表的最大数量
    public static final int MAX_IMMUTABLE_NUM = 10;
}
