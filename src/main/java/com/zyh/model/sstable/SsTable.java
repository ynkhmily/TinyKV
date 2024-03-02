package com.zyh.model.sstable;

import com.alibaba.fastjson.TypeReference;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.zyh.model.constant.Constant;
import com.zyh.utils.ConvertUtil;
import com.zyh.utils.LoggerUtil;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Data
public class SsTable implements Closeable {
    private final Logger LOGGER = LoggerFactory.getLogger(SsTable.class);

    public RandomAccessFile tableFile;

    public TreeMap<String,Position> index;

    public BloomFilter<String>  bloomFilter;

    public TableMetaInfo tableMetaInfo;

    public String filePath;

    /**
     * 指定文件路径和分区大小
     * @param filePath 表文件路径
     */
    private SsTable(String filePath) {
        try {
            this.filePath = filePath;
            this.tableMetaInfo = new TableMetaInfo(Constant.VERSION);
            this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Constant.BLOOMFILTER_INIT_SIZE,Constant.BLOOMFILTER_WRONG_RATE);
            this.tableFile = new RandomAccessFile(filePath, Constant.RW);
            tableFile.seek(0);
            index = new TreeMap<String, Position>();
        } catch (IOException e) {
            throw new RuntimeException("SsTable 初始化失败");
        }
    }

    public SsTable(String filePath, RandomAccessFile tableFile, TreeMap<String,Position> index, TableMetaInfo tableMetaInfo){
        this.filePath = filePath;
        this.tableFile = tableFile;
        this.index = index;
        this.tableMetaInfo = tableMetaInfo;
        this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Constant.BLOOMFILTER_INIT_SIZE,Constant.BLOOMFILTER_WRONG_RATE);
        initBloomFilter();
    }

    /**
     * 从内存表中构建ssTable
     * @param filePath
     * @param map
     * @return
     */
    public static SsTable createFromMemory(String filePath, TreeMap<String, Element> map) {
        SsTable ssTable = new SsTable(filePath);
        ssTable.initFromMemory(map);
        ssTable.initBloomFilter();
        return ssTable;
    }

    /**
     * 从文件中构建ssTable
     * @param filePath
     * @return
     */
    public static SsTable createFromFile(String filePath) {
        SsTable ssTable = new SsTable(filePath);
        ssTable.restoreFromFile();
        return ssTable;
    }

    /**
     * 从ssTable中查询数据
     * @param key
     * @return
     */
    public String query(String key) {
        if(!bloomFilter.mightContain(key) || !index.containsKey(key)){
            return null;
        }

        Position position = index.get(key);
        if(position.getDeleted()){
            return null;
        }

        long start = position.getStart();
        long len = position.getLen();
        byte[] data = new byte[(int) len];
        try {
            tableFile.seek(start);
            tableFile.read(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String res = new String(data, ConvertUtil.CHARSET);

        return res;
    }

    /**
     * 从文件中恢复ssTable到内存
     */
    private void restoreFromFile() {
        try {
            TableMetaInfo tableMetaInfo = TableMetaInfo.readFromFile(tableFile);
            LoggerUtil.info(LOGGER,"[SsTable][restoreFromFile][TableMetaInfo]: {}",tableMetaInfo);

            long indexStart = tableMetaInfo.getIndexStart();
            long indexLen = tableMetaInfo.getIndexLen();
            tableFile.seek(indexStart);
            byte[] indexData = new byte[(int)indexLen];
            tableFile.read(indexData);
            String indexDataStr = new String(indexData, StandardCharsets.UTF_8);
            LoggerUtil.info(LOGGER,"[SsTable][restoreFromFile][indexString]: {}",indexDataStr);


            this.index = (TreeMap<String, Position>) ConvertUtil.readMapObjectFromBytes(indexData,
                    new TypeReference<TreeMap<String, Position>>() {});
//            this.index = ConvertUtil.readObjectFromBytes(indexData, this.index.getClass());
            LoggerUtil.info(LOGGER,"[SsTable][restoreFromFile][sparseIndex]: {}",this.index);

            // 根据index构建bloomfilter
            initBloomFilter();

            this.tableMetaInfo = tableMetaInfo;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 初始化布隆过滤器
     */
    private void initBloomFilter() {
        index.keySet().stream().forEach(k -> this.bloomFilter.put(k));
    }

    /**
     * 从内存表转化为ssTable
     * @param memData
     */
    private void initFromMemory(TreeMap<String, Element> memData) {
        try {
            // 当前的记录数
            ArrayList<Element> data = new ArrayList<>();
            // 当前的filePointer是0
            tableMetaInfo.setDataStart(tableFile.getFilePointer());
            for (Element element : memData.values()) {
                data.add(element);
                if(data.size() >= Constant.SPARSE_INDEX_PART_SIZE){
                    writeDataPart(data);
                }
            }

            // 将剩余的写入文件
            if(data.size() > 0){
                writeDataPart(data);
            }

            long dataLen = tableFile.getFilePointer() - tableMetaInfo.getDataStart();
            tableMetaInfo.setDataLen(dataLen);
            // 写入索引
            byte[] indexBytes = ConvertUtil.convertObjectToBytes(index);

            tableMetaInfo.setIndexLen((long) indexBytes.length);
            tableMetaInfo.setIndexStart(tableFile.getFilePointer());
            tableFile.write(indexBytes);
            LoggerUtil.info(LOGGER,"[SsTable][initFromIndex][index]: {}",index);

            // 写入元数据
            tableMetaInfo.writeToFile(tableFile);
            LoggerUtil.info(LOGGER, "[SsTable][initFromIndex]: {},{}", filePath, tableMetaInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将数据分区写入文件
     * @param partData
     * @throws IOException
     */
    private void writeDataPart(ArrayList<Element> partData) throws IOException {
        long dataStart = tableFile.getFilePointer();
        StringBuilder partDataStr = new StringBuilder();
        for (Element element : partData) {
            String value = element.getValue();
            partDataStr.append(value);
            long len = value.getBytes(StandardCharsets.UTF_8).length;
            index.put(element.getKey(), new Position(dataStart, len, element.getDeleted()));
            dataStart += len;
        }

        byte[] partDataBytes = partDataStr.toString().getBytes(StandardCharsets.UTF_8);
        tableFile.write(partDataBytes);
        partData.clear();
    }


    public String getMinKey(){
        return index.firstKey();
    }

    public String getMaxKey(){
        return index.lastKey();
    }

    public void close() throws IOException {
        tableFile.close();
    }

    public void deleteFile(){
        try {
            close();
            File file = new File(filePath);
            if(file.delete()){
                LOGGER.info("[SsTable][deleteFile] 文件删除成功: {}",filePath);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
