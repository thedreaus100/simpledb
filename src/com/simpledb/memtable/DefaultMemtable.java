package com.simpledb.memtable;

import com.simpledb.KeyValuePair;
import com.simpledb.writer.LogWriter;

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
    protected  ReadWriteLock readWriteLock;
    protected boolean dumped = false;

    public DefaultMemtable(ReadWriteLock readWriteLock, LogWriter<String> writer){

        this.readWriteLock = readWriteLock;
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
           Lock lock = readWriteLock.writeLock();
           lock.lock();
           try{
               cacheMap.put(keyValuePair.getKey(), keyValuePair.getValue());
           }finally{
               //don't want to block while calculating used space
               size.addAndGet(writer.calculateSpace(keyValuePair));
               lock.unlock();
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
