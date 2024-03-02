package com.zyh.model.worker;

import com.zyh.model.log.CommitLog;
import com.zyh.model.log.ManiFest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.File;
import java.util.concurrent.*;

public class Cleaner {
    public static final Logger LOGGER = LoggerFactory.getLogger(Cleaner.class);

    private ManiFest maniFest;

    private ScheduledExecutorService workerPool;

    private File file;

    private String dataDir;

    public Cleaner(ManiFest maniFest, String dataDir){
        this.maniFest = maniFest;
        this.dataDir = dataDir;
        this.file = new File(dataDir);
        this.workerPool = Executors.newScheduledThreadPool(1,new DefaultThreadFactory("Cleaner"));
    }


    public void start(){
        workerPool.schedule(new Runnable() {
            @Override
            public void run() {
                File[] files = file.listFiles();
                LOGGER.info("[cleaner] 开始定期清理");
                for (File currentFile : files) {
                    if(maniFest.needDeleted(currentFile.getName())){
                        LOGGER.info("[cleaner] 定期清理，清理文件： {}",currentFile.getName());
                        if(!file.delete()){
                            throw  new RuntimeException("[Cleaner] 文件清理失败: " + currentFile.getName());
                        }
                    }
                }
            }
        },1,TimeUnit.MINUTES);
    }
}
