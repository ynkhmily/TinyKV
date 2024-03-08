package com.zyh.model.log;

import com.zyh.model.constant.Constant;
import com.zyh.model.sstable.Element;
import com.zyh.utils.ConvertUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.plaf.IconUIResource;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;


@Data
@AllArgsConstructor
public class CommitLog implements Closeable {

    public static final Logger LOGGER = LoggerFactory.getLogger(CommitLog.class);

    public static final String WAL = "wal";

    public static final String WAL_TMP = "walTmp";

    public static final String SUFFIX = ".log";

    private ConcurrentLinkedDeque<Integer> tmpNumList;

    private RandomAccessFile wal;

    private File walFile;

    private String dataDir;

    public CommitLog(String dataDir){
        try {
            this.dataDir = dataDir;
            this.tmpNumList = new ConcurrentLinkedDeque<>();
            this.walFile = new File(dataDir + File.separator + WAL + SUFFIX);
            if (!walFile.exists()){
                walFile.createNewFile();
            }
            this.wal = new RandomAccessFile(walFile, Constant.RW);
            this.wal.seek(0);
            detectTmpFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(Element element){
        try {
            byte[] bytes = ConvertUtil.convertObjectToBytes(element);
            int len = bytes.length;
            wal.seek(wal.length());
            wal.writeInt(len);
            wal.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public TreeMap<String, Element> recoverTableFromLog(){
        TreeMap<String, Element> data = new TreeMap<>();
        long lastPoint = 0;
        try {
            wal.seek(0);
            while(wal.getFilePointer() != wal.length()){
                int len = wal.readInt();
                byte[] bytes = new byte[len];
                wal.read(bytes);
                Element element = (Element) ConvertUtil.readObjectFromBytes(bytes, Element.class);
                data.put(element.getKey(),element);
                lastPoint += len + 4;
            }
            return data;
        } catch (IOException e) {
            try {
                // 指向最后完整的数据
                wal.seek(lastPoint);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return data;
    }

    public static boolean containWAL(String dataDir){
        File file = new File(dataDir);
        File[] files = file.listFiles();
        for (File currentfile : files) {
            if(currentfile.getName().equals(WAL + SUFFIX)){
                return true;
            }
        }
        return false;
    }

    /**
     * 切换日志并返回当前的临时日志编号
     * @return
     */
    public void switchLog(){
        try {
            wal.close();
            Integer tmpNum = getMaxTmpNum();
            if(Objects.isNull(tmpNum)){
                tmpNum = 0;
            } else {
                tmpNum ++;
            }
            File tmpfile = new File(formatFilePath(dataDir, WAL_TMP + tmpNum + SUFFIX));
            if(tmpfile.exists()){
                if(!tmpfile.delete()){
                    throw new RuntimeException("删除文件失败：" + WAL_TMP + SUFFIX);
                }
            }
            if(!walFile.renameTo(tmpfile)){
                throw new RuntimeException("重命名失败：" + WAL_TMP + tmpNum + SUFFIX);
            }
            walFile = new File(formatFilePath(dataDir,WAL + SUFFIX));
            wal = new RandomAccessFile(walFile,Constant.RW);
            tmpNumList.add(tmpNum);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }

    public void deleteTmpLog() {
        Integer tmpNum = tmpNumList.pollFirst();
        String fileName = WAL_TMP + tmpNum +SUFFIX;
        String filePath = formatFilePath(dataDir, fileName);
        File file = new File(filePath);
        if (file.delete()) {
            LOGGER.info("[CommitLog][deleteTmpLog] {}文件删除成功",fileName);
        } else {
            throw new RuntimeException("[CommitLog][deleteTmpLog] 文件删除失败： " + fileName);
        }
    }

    public Integer getMaxTmpNum(){
        return tmpNumList.peekLast();
    }

    public void detectTmpFile(){
        File file = new File(dataDir);
        File[] files = file.listFiles();
        ArrayList<Integer> numList = new ArrayList<>();
        for (File currentFile : files) {
            String name = currentFile.getName();
            if (name.startsWith(WAL_TMP) && name.endsWith(SUFFIX)) {
                int len = WAL_TMP.length();
                String tmpNumStr = name.substring(len);
                tmpNumStr = tmpNumStr.substring(0, tmpNumStr.length() - SUFFIX.length());
                numList.add(Integer.valueOf(tmpNumStr));
            }
        }
        Collections.sort(numList);
        for (Integer num : numList) {
            tmpNumList.add(num);
        }
    }

    public ConcurrentLinkedDeque<Integer> getTmpNumLsit(){
        return tmpNumList;
    }

    public List<TreeMap<String, Element>> recoverTableFromTmpLog(){
        List<TreeMap<String, Element>>  data = new ArrayList<>();
        for (int i = 0; i < tmpNumList.size(); i++) {
            int tmpNum = tmpNumList.peekFirst();
            TreeMap<String, Element> current = new TreeMap<>();
            RandomAccessFile file = null;
            String filePath = formatFilePath(dataDir, WAL_TMP + tmpNum + SUFFIX);
            try {
                file = new RandomAccessFile(filePath,Constant.RW);
                file.seek(0);
                while(file.getFilePointer() != file.length()){
                    int len = file.readInt();
                    byte[] bytes = new byte[len];
                    file.read(bytes);
                    Element element = (Element) ConvertUtil.readObjectFromBytes(bytes, Element.class);
                    current.put(element.getKey(),element);
                }
            } catch (IOException e) {
                LOGGER.info("[recoverTableFromTmpLog]临时文件损坏: {}", filePath);
            } finally {
                try {
                    file.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            data.add(current);
        }

        return data;
    }

    private String formatFilePath(String ...args){
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            String s = args[i];
            if(i != args.length - 1){
                s += File.separator;
            }
            res.append(s);
        }
        return res.toString();
    }

    public TreeMap<String, Element> readFromFile(Integer num){
        TreeMap<String, Element> data = new TreeMap<>();
        try {
            RandomAccessFile file = new RandomAccessFile(formatFilePath(dataDir, WAL_TMP + num + SUFFIX), Constant.RW);
            file.seek(0);
            while(file.getFilePointer() != file.length()){
                int len = file.readInt();
                byte[] bytes = new byte[len];
                file.read(bytes);
                Element element = (Element) ConvertUtil.readObjectFromBytes(bytes, Element.class);
                data.put(element.getKey(),element);
            }
            return data;
        } catch (IOException e) {
            throw new RuntimeException("[CommitLog][readFromFile] 读取失败！",e);
        }
    }

    public String query(String key) {
        ArrayList<Integer> tmpList = new ArrayList<>();
        for (Integer currentNum : tmpNumList) {
            tmpList.add(currentNum);
        }
        // 查询时是逆序的
        for (int i = tmpList.size() - 1; i >= 0; i--) {
            TreeMap<String, Element> data = readFromFile(tmpList.get(i));
            if(data.containsKey(key)){
                return data.get(key).getValue();
            }
        }

        return null;
    }
}
