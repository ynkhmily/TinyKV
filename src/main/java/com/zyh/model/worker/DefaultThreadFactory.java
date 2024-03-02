package com.zyh.model.worker;

import lombok.Data;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class DefaultThreadFactory implements ThreadFactory {

    private ThreadGroup group;

    private String prefix;

    private AtomicInteger num = new AtomicInteger(0);

    public DefaultThreadFactory(String prefix){
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        if (Objects.isNull(prefix) || prefix.isEmpty()) {
            prefix = "default";
        }

        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(group, r, getName());
        return thread;
    }

    public String getName(){
        return "[" + prefix + "_thread" + num.get() + "]";
    }
}
