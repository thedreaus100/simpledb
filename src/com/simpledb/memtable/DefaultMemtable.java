package com.simpledb.memtable;

import com.simpledb.KeyValuePair;
import com.simpledb.writer.LogWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultMemtable implements Memtable<String> {

    private TreeMap<String, Serializable> cacheMap;
    protected long maxSize;
    protected long maxBlockSize;
    private AtomicLong size;
    private final LogWriter<String> writer;
    protected  ReentrantReadWriteLock.WriteLock writeLock;
    protected boolean dumped = false;

    //Log
    private Logger logger = LogManager.getRootLogger();

    public DefaultMemtable(ReentrantReadWriteLock.WriteLock writeLock, LogWriter<String> writer){

        this.writeLock = writeLock;
        this.writer = writer;
        size = new AtomicLong(0);
        cacheMap = new TreeMap<String, Serializable>();
        maxSize = 1024 * 10;
        maxBlockSize = 1024;
    }

    @Override
    public void dumped(){
        this.dumped = true;
    }

    @Override
    public void insert(KeyValuePair<String> keyValuePair) throws MemtableException {

       //blocks as long as nothing else is concurrently writing... and there are no ongoing reads
       logger.debug("Attempting to obtain write lock: " + writeLock);
       writeLock.lock();
       logger.debug(String.format("Writing key %s\t", keyValuePair.getKey()));
       try{
           if(this.isFull()){
               throw new MemtableFullException();
           }else if(this.dumped){
               throw new MemtableDumpedException();
           }
           cacheMap.put(keyValuePair.getKey(), keyValuePair.getValue());
       }finally{
           //don't want to block while calculating used space
           writeLock.unlock();
           size.addAndGet(writer.calculateSpace(keyValuePair));
       }
    }

    @Override
    public long getSize() {
        return size.get();
    }

    @Override
    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public long getMaxBlockSize() {
        return maxBlockSize;
    }

    public void setMaxBlockSize(long maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
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
