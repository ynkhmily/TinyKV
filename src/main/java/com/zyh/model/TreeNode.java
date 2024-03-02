package com.zyh.model;

import com.zyh.model.constant.Constant;
import com.zyh.model.log.ManiFest;
import com.zyh.model.worker.Cleaner;
import com.zyh.model.worker.DefaultThreadFactory;
import com.zyh.model.sstable.Element;
import com.zyh.model.sstable.Position;
import com.zyh.model.sstable.SsTable;
import com.zyh.model.sstable.TableMetaInfo;
import com.zyh.utils.ConvertUtil;
import com.zyh.utils.FileNameUtil;
import com.zyh.utils.LoggerUtil;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Data
public class TreeNode {

    public static final Logger LOGGER = LoggerFactory.getLogger(TreeNode.class);

    private int level;

    private volatile CopyOnWriteArrayList<SsTable> tableList;

    private List<String>  sparseIndex;

    private TreeNode nextLevel;

    private String dataDir;

    private ReadWriteLock lock;

    public static final String SUFFIX = ".table";

    private ThreadPoolExecutor workerPool;


    private ManiFest maniFest;

    public TreeNode(String dataDir,int level, ManiFest maniFest){
        this.dataDir = dataDir;
        this.level = level;
        this.tableList = new CopyOnWriteArrayList<>();
        this.sparseIndex = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
        this.maniFest = maniFest;
        // 整颗树只有一个compact线程，因此只有第0层包含
        // core 1; max 1; keepAliveTime 60; 这里必须是单线程
//        this.workerPool = new ThreadPoolExecutor(1, 1, 60,
//                TimeUnit.SECONDS, new LinkedBlockingDeque<>(10), new DefaultThreadFactory("TreeNode"));
        this.workerPool = (ThreadPoolExecutor) Executors.newCachedThreadPool(new DefaultThreadFactory("TreeNode"));
        if(level != 0){
            buildSparseIndex();
        }
    }

    public void initTreeNode(List<String> fileNameList) {
        for (String fileName : fileNameList) {
            String filePath = dataDir + File.separator + fileName;
            this.tableList.add(SsTable.createFromFile(filePath));
        }
    }

    public void setNextLevel(TreeNode next){
        this.nextLevel = next;
    }

    public void buildSparseIndex() {
        for (SsTable ssTable : tableList) {
            sparseIndex.add(ssTable.getMinKey());
        }
    }

    public String get(String key){
        String value = null;
        try {
            lock.readLock().lock();
            if(level != 0){
                int idx = Collections.binarySearch(this.sparseIndex, key);
                if(idx < 0){
                    idx = -1 * idx;
                    idx -= 2;
                }

                if (idx >= 0) {
                    SsTable ssTable = this.tableList.get(idx);
                    value = ssTable.query(key);
                }
            } else {
                for (int i = tableList.size() - 1; i >= 0; i--) {
                    value = tableList.get(i).query(key);
                    if(!Objects.isNull(value)){
                        break;
                    }
                }
            }
            if (Objects.isNull(value) && !Objects.isNull(nextLevel)) {
                value = nextLevel.get(key);
            }
        } catch (Exception e){
            throw new RuntimeException("[TreeNode][query] 查询失败",e);
        } finally {
            lock.readLock().unlock();
            return value;
        }
    }

    /**
     * 添加持久化数据文件, 该方法被工作线程调用
     * 对当前层进行压缩, L0 层采用size-tiered compaction
     * @param data
     */
    public void insertSstable(TreeMap<String, Element> data){
        try{
            lock.writeLock().lock();
            int maxFileNum = getMaxFileNum();
            // 触发压缩
            if(tableList.size() > maxFileNum){
                // TODO 异步
                this.workerPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        sizeTiredCompact();
                    }
                });
            }
            String fileName = FileNameUtil.getFileName() + SUFFIX;
            String filePath = getFilePath(fileName);
            SsTable ssTable = SsTable.createFromMemory(filePath, data);
            // 记录日志
            maniFest.insertFile(level, fileName);
            tableList.add(ssTable);
        } catch (Exception e){
            throw new RuntimeException("insertSstable failed! current level :" + level);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private String getFilePath(String fileName){
        String filePath = dataDir + File.separator + fileName;

        return filePath;
    }

    private int getMaxFileNum(){

        return Constant.MAX_FILE_NUM * Integer.max(1, level * Constant.RATIO);
    }

    /**
     * 将多个SsTable合并成一个SsTable
     */
    private void sizeTiredCompact() {
        CopyOnWriteArrayList<SsTable> tableListSnapShot = null;
        try {
            lock.readLock().lock();

            if (this.tableList.size() <= getMaxFileNum()) {
                return;
            }
            // 留最后一个进行压缩
            tableListSnapShot = new CopyOnWriteArrayList<>();
            for (int i = 0; i < this.tableList.size() - 1; i++) {
                tableListSnapShot.add(tableList.get(i));
            }
        } finally {
            lock.readLock().unlock();
        }

        LOGGER.info("[TreeNode][sizeTiredCompact] 开始对level{}进行压缩", level);
        TreeMap<String, Position> index = new TreeMap<>();
        HashMap<String, RandomAccessFile> key2file = mergeIndex(index,tableListSnapShot);

        // 避免将所有value读入内存中
        String fileName = FileNameUtil.getFileName() + SUFFIX;
        String targetFilePath = getFilePath(fileName);
        try {
            List<String> snapShotfilePath = tableListSnapShot.stream().map(e -> e.getFilePath()).collect(Collectors.toList());
            SsTable targetSsTable = generateTargetFile(targetFilePath, index, key2file);

            // 将生成的文件插入到下一层
            if(Objects.isNull(nextLevel)){
                this.nextLevel = new TreeNode(dataDir,level + 1, maniFest);
            }
            this.nextLevel.nextLevelInsertSstable(targetSsTable);
            maniFest.delete(level, snapShotfilePath);
            // 丢弃原有的SsTable
            lock.writeLock().lock();
            LOGGER.info("[TreeNode][sizeTiredCompact] 开始删除无效文件，预计删除文件数量：{}",snapShotfilePath.size());
            int deletedSize = 0;
            for (SsTable ssTable : tableList) {
                if(snapShotfilePath.contains(ssTable.getFilePath())){
                    ssTable.deleteFile();
                    tableList.remove(ssTable);
                    deletedSize ++;
                }
            }
            LOGGER.info("[TreeNode][sizeTiredCompact] 删除无效文件结束，实际删除文件数量：{}",deletedSize);
        } catch (Exception e) {
            LOGGER.error("[sizeTiredCompact] error, targetFilePath : {}, {}",targetFilePath,e);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }

    }

    private SsTable generateTargetFile(String targetFilePath, TreeMap<String, Position> index, HashMap<String, RandomAccessFile> key2file) throws IOException {
        RandomAccessFile targetFile = new RandomAccessFile(targetFilePath, Constant.RW);
        TreeMap<String, Position> targetIndex = new TreeMap<>();
        targetFile.seek(0);
        for (Map.Entry<String, Position> enrty : index.entrySet()) {
            String key = enrty.getKey();
            Position position = enrty.getValue();
            RandomAccessFile file = key2file.get(key);
            long len = position.getLen();
            byte[] bytes = new byte[(int) len];
            // TODO 零拷贝
            file.seek(position.start);
            file.read(bytes);

            // 数据写入到目标文件
            long start = targetFile.getFilePointer();
            targetFile.write(bytes);
            targetIndex.put(key,new Position(start,len,position.getDeleted()));
        }

        TableMetaInfo tableMetaInfo = new TableMetaInfo(Constant.VERSION);
        tableMetaInfo.setDataStart(0L);
        long dataLen = targetFile.getFilePointer() - tableMetaInfo.getDataStart();
        tableMetaInfo.setDataLen(dataLen);
        // 写入索引
        byte[] targetIndexBytes = ConvertUtil.convertObjectToBytes(targetIndex);

        tableMetaInfo.setIndexLen((long) targetIndexBytes.length);
        tableMetaInfo.setIndexStart(targetFile.getFilePointer());
        targetFile.write(targetIndexBytes);
        LoggerUtil.info(LOGGER,"[TreeNode][generateTargetFile][targetIndex]: {}",targetIndex);

        // 写入元数据
        tableMetaInfo.writeToFile(targetFile);
        LoggerUtil.info(LOGGER, "[TreeNode][generateTargetFile]: {},{}", targetFilePath, tableMetaInfo);

        return new SsTable(targetFilePath,targetFile,targetIndex,tableMetaInfo);
    }


    /**
     * level >= 1时数据文件插入，执行当前方法的线程为上一层的workPool
     * @param ssTable
     */
    public void nextLevelInsertSstable(SsTable ssTable) throws IOException {
        TreeMap<String, Position> index = new TreeMap<>();
        CopyOnWriteArrayList<SsTable> nextLevelTableList = null;
        try {
            lock.readLock().lock();
            nextLevelTableList = this.getTableList();
        } finally {
            lock.readLock().unlock();
        }
        HashMap<String, RandomAccessFile> key2file = mergeIndex(index, nextLevelTableList);

        TreeMap<String, Position> insertIndex = ssTable.getIndex();
        RandomAccessFile insertFile = ssTable.getTableFile();
        for (Map.Entry<String, Position> entry : insertIndex.entrySet()) {
            String key = entry.getKey();
            index.put(key,entry.getValue());
            key2file.put(key, insertFile);
        }

        List<String> fileNameList = new ArrayList<>();
        List<TreeMap<String, Position>> indexList = splitIndex(index);
        CopyOnWriteArrayList<SsTable> tableListTmp = new CopyOnWriteArrayList<>();
        for (TreeMap<String, Position> currentIndex : indexList) {
            String fileName = FileNameUtil.getFileName() + SUFFIX;
            String filePath = getFilePath(fileName);
            fileNameList.add(fileName);
            SsTable currentSsTable = generateTargetFile(filePath, currentIndex, key2file);
            tableListTmp.add(currentSsTable);
        }
        maniFest.updateLevelFileNames(level, fileNameList);

        // 切换引用
        try {
            lock.writeLock().lock();
            LOGGER.info("[TreeNode][nextLevelInsertSstable] 开始删除无效文件，删除文件数量：{}", tableList.size() + 1);
            ssTable.deleteFile();
            for (SsTable table : tableList) {
                table.deleteFile();
            }
            this.tableList = tableListTmp;
            LOGGER.info("[TreeNode][nextLevelInsertSstable] 删除无效文件结束");
            buildSparseIndex();
        } finally {
            lock.writeLock().unlock();
        }

        // 检查当前层是否需要压缩
        int maxFileNum = getMaxFileNum();
        if(tableList.size() > maxFileNum){
            SsTable targetSsTable = this.tableList.get(0);
            // 将生成的文件插入到下一层
            if(Objects.isNull(nextLevel)){
                this.nextLevel = new TreeNode(dataDir,level + 1, maniFest);
            }
            // TODO 异步
            this.workerPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        nextLevel.nextLevelInsertSstable(targetSsTable);
                    } catch (IOException e) {
                        throw new RuntimeException("[TreeNode][nextLevelInsertSstable]leveledCompact 失败");
                    }
                }
            });
        }
    }

    private List<TreeMap<String, Position>> splitIndex(TreeMap<String, Position> index) {
        List<TreeMap<String, Position>> indexList = new ArrayList<>();
        Iterator<Map.Entry<String, Position>> iterator = index.entrySet().iterator();
        int maxSize = getMaxItem();
        TreeMap<String, Position> tmpIndex = new TreeMap<>();
        while(iterator.hasNext()){
            Map.Entry<String, Position> next = iterator.next();
            tmpIndex.put(next.getKey(),next.getValue());
            if(tmpIndex.size() >= maxSize){
                indexList.add(tmpIndex);
                tmpIndex = new TreeMap<>();
            }
        }
        if(tmpIndex.size() > 0){
            indexList.add(tmpIndex);
        }
        return indexList;
    }

    /**
     *
     * @param index 存储合并后的索引
     * @param currentTableList
     * @return key和对应的文件句柄
     */
    public HashMap<String, RandomAccessFile> mergeIndex( TreeMap<String, Position> index,List<SsTable> currentTableList){
        HashMap<String, RandomAccessFile> key2file = new HashMap<>();
        for (int i = 0; i < currentTableList.size(); i++) {
            SsTable ssTable = currentTableList.get(i);
            TreeMap<String, Position> currentIndex = ssTable.getIndex();
            RandomAccessFile tableFile = ssTable.getTableFile();
            for (Map.Entry<String, Position> entry : currentIndex.entrySet()) {
                String key = entry.getKey();
                index.put(key, entry.getValue());
                key2file.put(key,tableFile);
            }
        }
        return key2file;
    }

    public int getMaxItem(){
        return Constant.MAX_ITEM_NUM * Integer.max(1, level * Constant.RATIO);
    }

}
