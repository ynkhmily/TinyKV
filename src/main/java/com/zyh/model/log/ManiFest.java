package com.zyh.model.log;

import com.alibaba.fastjson.TypeReference;
import com.zyh.model.TreeNode;
import com.zyh.model.constant.Constant;
import com.zyh.utils.ConvertUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ManiFest implements Closeable {
    public static final String MANIFEST = "manifest";
    private String dataDir;

    private RandomAccessFile log;

    private File file;

    private volatile ConcurrentHashMap<Integer,List<String>> fileNameTree;

    private ConcurrentLinkedDeque<String> expiredFileName;

    private Set<String> pastFilePaths;

    public ManiFest(String dataDir) throws IOException {
        this.dataDir = dataDir;
        this.file = new File(dataDir + File.separator + MANIFEST);
        this.expiredFileName = new ConcurrentLinkedDeque();
        this.pastFilePaths = new HashSet<>();
        if(!file.exists()){
            file.createNewFile();
        }
        log = new RandomAccessFile(this.file, Constant.RW);
    }

    public TreeNode rebuildTreeFromFile(){
        long lastPoint = 0;
        try {
            log.seek(0);
            while(log.getFilePointer() != log.length()){
                int len = log.readInt();
                byte[] bytes = new byte[len];
                log.read(bytes);
                fileNameTree = (ConcurrentHashMap<Integer, List<String>>) ConvertUtil.readMapObjectFromBytes(bytes, new TypeReference<ConcurrentHashMap<Integer, List<String>>>(){});
                insertIntoPastFileList();
                lastPoint += len + 4;
            }
        } catch (IOException e) {
            try {
                // 指向最后完整的数据
                log.seek(lastPoint);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return rebuildTree();
    }

    private void insertIntoPastFileList() {
        if(Objects.isNull(fileNameTree)){
            return;
        }
        for (Map.Entry<Integer, List<String>> entry : fileNameTree.entrySet()) {
            List<String> fileNames = entry.getValue();
            for (String fileName : fileNames) {
                this.pastFilePaths.add(fileName);
            }
        }
    }

    private TreeNode rebuildTree() {
        if(Objects.isNull(fileNameTree))    return new TreeNode(dataDir, 0, this);

        TreeNode root = null, last = null;
        for (Map.Entry<Integer, List<String>> entry : fileNameTree.entrySet()) {
            int level = entry.getKey();
            List<String> fileNameList = entry.getValue();
            TreeNode treeNode = new TreeNode(dataDir, level, this);
            treeNode.initTreeNode(fileNameList);
            if(level == 0){
                root = treeNode;
            } else if(Objects.nonNull(last)){
                treeNode.buildSparseIndex();
                last.setNextLevel(treeNode);
            }
            last = treeNode;
        }

        return root;
    }

    public void appendToLog(){
        try {
            byte[] bytes = ConvertUtil.convertObjectToBytes(fileNameTree);
            int len = bytes.length;
            log.writeInt(len);
            log.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void insertFile(Integer level,String filName){
        if(Objects.isNull(fileNameTree)){
            fileNameTree = new ConcurrentHashMap<>();
        }
        List<String> currentLevelFileNames = fileNameTree.get(level);
        if(Objects.isNull(currentLevelFileNames)){
            currentLevelFileNames = new ArrayList<>();
        }
        currentLevelFileNames.add(filName);
        fileNameTree.put(level,currentLevelFileNames);
        appendToLog();
    }

    public synchronized void updateLevelFileNames(Integer level, List<String> fileNameList){
        fileNameTree.put(level,fileNameList);
        appendToLog();
    }

    public synchronized void delete(int level,List<String> deletedFilePaths){
        List<String> fileNames = fileNameTree.get(level);
        if(fileNames.size() < deletedFilePaths.size()){
            throw new RuntimeException("[Manifest][delete] 删除失败");
        }
        Iterator<String> iterator = fileNames.iterator();
        while(iterator.hasNext()){
            String fileName = iterator.next();
            String filePath = dataDir + File.separator + fileName;
            if(deletedFilePaths.contains(filePath)){
                iterator.remove();
            }
        }
        appendToLog();
    }

    @Override
    public void close() throws IOException {
        log.close();
    }

    public ConcurrentLinkedDeque<String> getExpiredFileNames(){
        return expiredFileName;
    }

    public Boolean needDeleted(String name){
        return pastFilePaths.contains(name);
    }
}
