package com.zyh.service;

import com.zyh.model.constant.Constant;
import com.zyh.model.log.CommitLog;
import com.zyh.model.log.ManiFest;
import com.zyh.model.worker.Cleaner;
import com.zyh.model.worker.DefaultThreadFactory;
import com.zyh.model.sstable.Element;
import com.zyh.model.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.StyledEditorKit;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TinyKV implements KV{

    public static final Logger LOGGER = LoggerFactory.getLogger(TinyKV.class);

    private TreeMap<String, Element>   memtable;

    private ConcurrentLinkedDeque<TreeMap<String, Element>> immutable;

    private String dataDir;

    private CommitLog log;

    private TreeNode tableTree;

    private ReadWriteLock lock;

    private int storeThreshold;

    private ThreadPoolExecutor workerPool;

    private Cleaner cleaner;

    public static void main(String[] args) throws InterruptedException {
        String filePath = "E:\\zyh\\java\\TinyKV\\data";
        int maxNum = 100;
        TinyKV tinyKV = new TinyKV(filePath, maxNum);
        CountDownLatch countDownLatch = new CountDownLatch(2);

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 500; i++) {
                    tinyKV.set(String.valueOf(i), "thread1 : " + i);
                }
                countDownLatch.countDown();

            }
        },"thread1").start();
//
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 300; i++) {
//                    tinyKV.set(String.valueOf(i), "thread2 : " + i);
                    tinyKV.rm(String.valueOf(i));
                }

                countDownLatch.countDown();
            }
        },"thread2").start();


        countDownLatch.countDown();
        countDownLatch.await();

//        for (int i = 0; i < 300; i++) {
//            tinyKV.rm(String.valueOf(i));
//        }

        for (int i = 0; i < 500; i++) {
            System.out.println(i + " : " + tinyKV.get(String.valueOf(i)));
        }
        System.out.println("finish");
    }

    public TinyKV(String dataDir, int storeThreshold){
        this.dataDir = dataDir;
        this.log = new CommitLog(dataDir);
        this.storeThreshold = storeThreshold;
        // core 1; max 1; keepAliveTime 60; 这里必须是单线程
        this.workerPool = new ThreadPoolExecutor(1,1, 5,
                TimeUnit.SECONDS,new LinkedBlockingDeque<>(10),new DefaultThreadFactory("TinKV"));

        this.immutable = new ConcurrentLinkedDeque<>();
        this.lock = new ReentrantReadWriteLock();
        ManiFest maniFest = null;
        try {
            maniFest = new ManiFest(dataDir);
            this.tableTree = maniFest.rebuildTreeFromFile();
            cleaner = new Cleaner(maniFest, dataDir);
            cleaner.start();
            if(CommitLog.containWAL(dataDir)){
                this.memtable = this.log.recoverTableFromLog();
                recoverTmpFileWrite();
            } else {
                this.memtable = new TreeMap<>();
            }
        } catch (IOException e) {
            throw new RuntimeException("TinyKV启动失败");
        }

    }

    private void recoverTmpFileWrite() {
        List<TreeMap<String, Element>> dataList = this.log.recoverTableFromTmpLog();
        ConcurrentLinkedDeque<Integer> tmpNumList = this.log.getTmpNumList();
        LOGGER.info("[TinyKV][recoverTmpFileWrite] 临时日志文件恢复写入，文件数量: {}",tmpNumList.size());
        if(tmpNumList.size() != dataList.size()){
            throw new RuntimeException("[TinyKV][recoverTmpFileWrite]临时日志文件数量不匹配");
        }

        for (int i = 0; i < dataList.size(); i++) {
            this.tableTree.insertSstable(dataList.get(i));
            log.deleteTmpLog();
        }
    }


    public void set0(String key, String value, boolean deleted) {
        try {
            lock.writeLock().lock();

            if (memtable.size() >= storeThreshold) {
                switchTable();
                TreeMap<String, Element> currentImmutable = immutable.peekFirst();
                // TODO 并发
                workerPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        tableTree.insertSstable(currentImmutable);
                        log.deleteTmpLog();
                        try {
                            // 修改只读内存表
                            lock.writeLock().lock();
                            ConcurrentLinkedDeque<Integer> tmpNumList = log.getTmpNumLsit();
                            if (tmpNumList.size() < immutable.size()) {
                                immutable.pollLast();
                            }
                        } catch (Exception e){
                            throw new RuntimeException("[TinyKV][WorkerPool] 失败",e);
                        } finally {
                          lock.writeLock().unlock();
                        }
                    }
                });
            }

            Element element = new Element(key, value, deleted);
            this.log.append(element);
            memtable.put(key, element);

        } finally {
            lock.writeLock().unlock();
        }

    }

    private void switchTable() {
        TreeMap<String, Element> tmp = memtable;
        memtable = new TreeMap<>();
        this.log.switchLog();
        // 队头为最新的数据
        this.immutable.addFirst(tmp);
        if(this.immutable.size() >= Constant.MAX_IMMUTABLE_NUM) {
            this.immutable.pollLast();
        }
    }

    public String get(String key) {
        String value = null;
        try {
            lock.readLock().lock();

            if (memtable.containsKey(key)) {
                Element element = memtable.get(key);
                if(!element.getDeleted()) {
                    value = element.getValue();
                }
            }
            if(Objects.isNull(value)){
                for (TreeMap<String, Element> data : immutable) {
                    if(data.containsKey(key)){
                        value = data.get(key).getValue();
                        break;
                    }
                }
                if(Objects.isNull(value)) {
                    value = log.query(key);
                }
            }

            if(Objects.isNull(value)) {
                value = this.tableTree.get(key);
            }

        } finally {
            lock.readLock().unlock();
            return value;
        }
    }

    public void rm(String key) {
        set0(key,"",true);
    }

    public void set(String key, String value){
        set0(key,value,false);
    }

}
