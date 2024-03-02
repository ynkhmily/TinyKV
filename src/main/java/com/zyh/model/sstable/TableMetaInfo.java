package com.zyh.model.sstable;

import com.zyh.model.constant.Constant;
import lombok.Data;

import java.io.IOException;
import java.io.RandomAccessFile;

@Data
public class TableMetaInfo {
    public Long version;

    public Long dataStart;

    public Long dataLen;

    public Long indexStart;

    public Long indexLen;

    public TableMetaInfo(Long version) {
        this.version = version;
    }

    public void writeToFile(RandomAccessFile file){
        try {
            file.writeLong(version);
            file.writeLong(dataStart);
            file.writeLong(dataLen);
            file.writeLong(indexStart);
            file.writeLong(indexLen);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static TableMetaInfo readFromFile(RandomAccessFile file){
        try {
            TableMetaInfo tableMetaInfo = new TableMetaInfo(Constant.VERSION);
            long length = file.length();

            file.seek(length - 8);
            tableMetaInfo.setIndexLen(file.readLong());

            file.seek(length - 8 * 2);
            tableMetaInfo.setIndexStart(file.readLong());

            file.seek(length - 8 * 3);
            tableMetaInfo.setDataLen(file.readLong());

            file.seek(length - 8 * 4);
            tableMetaInfo.setDataStart(file.readLong());

            file.seek(length - 8 * 5);
            tableMetaInfo.setVersion(file.readLong());

            return tableMetaInfo;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
