package com.simpledb;

import com.simpledb.memtable.Memtable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleDB {

    private static boolean test = true;
    private static ExecutorService service;
    private static Logger logger = LogManager.getRootLogger();

    public static void main(String[] args) {

        service = Executors.newFixedThreadPool(10);
        Runtime.getRuntime().addShutdownHook(SimpleDB.shutdown());
        DefaultProcessor processor = new DefaultProcessor();
        Thread processorThread = new Thread(processor);
        service.submit(processorThread);

        ExecutorContext.getInstance().register(service);
    }

    public static Thread shutdown(){

        return new Thread(new Runnable() {

            @Override
            public void run() {
                try{
                    ExecutorContext.getInstance().shutdown();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }){};
    }
}
