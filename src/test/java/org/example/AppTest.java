package org.example;

import com.zyh.model.sstable.Element;
import com.zyh.service.TinyKV;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Unit test for simple App.
 */
public class AppTest
        extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() throws InterruptedException {
        String filePath = "E:\\zyh\\java\\TinyKV\\data";
        int maxNum = 100;
        TinyKV tinyKV = new TinyKV(filePath, maxNum);
        CountDownLatch countDownLatch = new CountDownLatch(2);

//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for (int i = 0; i < 500; i++) {
//                    tinyKV.set(String.valueOf(i), "thread1 : " + i);
//                }
//                countDownLatch.countDown();
//
//            }
//        },"thread1").start();
////
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for (int i = 0; i < 300; i++) {
////                    tinyKV.set(String.valueOf(i), "thread2 : " + i);
//                    tinyKV.rm(String.valueOf(i));
//                }
//
//                countDownLatch.countDown();
//            }
//        },"thread2").start();
//
//
//        countDownLatch.countDown();
//        countDownLatch.await();

        for (int i = 0; i < 300; i++) {
            tinyKV.rm(String.valueOf(i));
        }

        for (int i = 0; i < 500; i++) {
            System.out.println(i + " : " + tinyKV.get(String.valueOf(i)));
        }
//        for(int i = 0;i < 100;i ++){
//            System.out.println(tinyKV.get(String.valueOf(i)));
//        }

//        for(int i = 0;i < 200;i ++){
//            System.out.println(tinyKV.get(String.valueOf(i)));
//        }

        CountDownLatch stop = new CountDownLatch(1);
        stop.await();

    }
}
