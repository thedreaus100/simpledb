package com.simpledb;

import com.simpledb.memtable.Memtable;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleDB {

    private static Memtable<String> memTable;
    private static boolean test = true;
    private static ExecutorService service;


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
                    for(ExecutorService service:ExecutorContext.getInstance().getExecutorServices()){
                        System.out.println("Shutting Down: " + service);
                        service.shutdownNow();
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }){};
    }
}
