package com.simpledb.memtable;

import com.simpledb.KeyValuePair;
import com.simpledb.writer.LogWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultMemtable implements Memtable<String> {

    private TreeMap<String, Serializable> cacheMap;
    private int maxSize;
    private AtomicInteger size;
    private final LogWriter<String> writer;
    protected  ReentrantReadWriteLock.WriteLock writeLock;
    protected boolean dumped = false;

    //Log
    private Logger logger = LogManager.getRootLogger();

    public DefaultMemtable(ReentrantReadWriteLock.WriteLock writeLock, LogWriter<String> writer){

        this.writeLock = writeLock;
        this.writer = writer;
        size = new AtomicInteger(0);
        cacheMap = new TreeMap<String, Serializable>();
        maxSize = 1024;
    }

    @Override
    public void dumped(){
        this.dumped = true;
    }

    @Override
    public void insert(KeyValuePair<String> keyValuePair) {

       if(!dumped){
           //blocks as long as nothing else is concurrently writing... and there are no ongoing reads
           logger.debug("Attempting to obtain write lock: " + writeLock);
           writeLock.lock();
           logger.debug(String.format("Writing key %s\t", keyValuePair.getKey()));
           try{
               cacheMap.put(keyValuePair.getKey(), keyValuePair.getValue());
           }finally{
               //don't want to block while calculating used space
               size.addAndGet(writer.calculateSpace(keyValuePair));
               writeLock.unlock();
           }
       }
    }

    @Override
    public int getSize() {
        return size.get();
    }

    @Override
    public synchronized boolean isFull() {
        return size.get() >= maxSize;
    }

    @Override
    public TreeMap<String, Serializable> getMap() {

        return cacheMap;
    }
}
