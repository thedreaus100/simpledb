package com.simpledb;

import org.junit.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;

public class SimpleDBTest {

    private final ExecutorService executorService;

    public SimpleDBTest(){
        executorService = Executors.newFixedThreadPool(1);
    }

    @Test
    public void testSimpleDBLoad() {


        final List<Runnable> runnables = new ArrayList<Runnable>();
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        int maxTimeoutSeconds = 60 * 5;
        String message = "INFO: ";

        try (final PipedInputStream pis = new PipedInputStream();
             final PipedOutputStream pos = new PipedOutputStream();){

            //System.setIn(pis);
            pos.connect(pis);

            runnables.add(SimpleDBTest.fillSimpleDB(pis, pos));
            runnables.add(new DefaultProcessor(pis));

            int numThreads = runnables.size();
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            final CountDownLatch allDone = new CountDownLatch(numThreads);

            for (final Runnable submittedTestRunnable : runnables) {
                threadPool.submit(new Runnable() {
                    public void run() {
                        allExecutorThreadsReady.countDown();
                        try {
                            afterInitBlocker.await();
                            submittedTestRunnable.run();
                        } catch (final Throwable e) {
                            exceptions.add(e);
                        } finally {
                            allDone.countDown();
                        }
                    }
                });
            }

            assertTrue("Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent", allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));
            afterInitBlocker.countDown();
            assertTrue(message +" timeout! More than" + maxTimeoutSeconds + "seconds", allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS));

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally{
            System.out.println("Shutting down pool");
            threadPool.shutdownNow();
        }

        assertTrue(message + "failed with exception(s)" + exceptions, exceptions.isEmpty());
    }

    public static Thread fillSimpleDB(final PipedInputStream pis, final PipedOutputStream pos){

        return new Thread(() -> {
            boolean flag = true;
            int count = 0;
            int key = 0;
            try {
                Thread.sleep(0);
                while (flag && count < 100) {
                    String entry = String.format("SET: %s, %s\n", UUID.randomUUID().toString(), getRandomString(64));
                    System.out.print(entry);
                    pos.write(entry.getBytes());
                    count++;
                    Thread.sleep(0);
                }
            } catch (InterruptedException e) {
                System.out.println("INTERUPPTED");
                flag = false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static String getRandomString(int size){

        SecureRandom random = new SecureRandom();
        char[] chararray = new char[size];
        for(int i  =0; i < chararray.length; i++){
            chararray[i] = (char) (random.nextInt(25) + 65);
        }

        return new String(chararray);
    }
}
