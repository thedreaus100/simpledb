package com.simpledb.memtable;

import com.simpledb.KeyValuePair;
import com.simpledb.writer.LogWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultMemtable extends Memtable<String, String> {

    //Log
    private Logger logger = LogManager.getRootLogger();
    protected final TreeMap<String, String> cacheMap;

    //Lock
    protected ReentrantReadWriteLock.ReadLock readLock;
    protected ReentrantReadWriteLock.WriteLock writeLock;
    protected ReentrantReadWriteLock readWriteLock;

    public DefaultMemtable(LogWriter<String, String> writer){

        super(writer);
        this.readWriteLock = new ReentrantReadWriteLock(true);
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        this.cacheMap = new TreeMap<String, String>();
    }

    @Override
    public void insert(KeyValuePair<String, String> keyValuePair) throws MemtableException {

       //blocks as long as nothing else is concurrently writing... and there are no ongoing reads
       logger.debug("Attempting to obtain write lock: " + writeLock);
       writeLock.lock();
       try{
           if(this.isFull()){
               throw new MemtableFullException();
           }else if(this.dumped.get()){
               throw new MemtableDumpedException();
           }
           logger.debug(String.format("Writing key %s\t", keyValuePair.getKey()));
           cache().put(keyValuePair.getKey(), keyValuePair.getValue());
       }finally{
           //don't want to block while calculating used space
           writeLock.unlock();
           size.addAndGet(writer.calculateSpace(keyValuePair));
       }
    }

    @Override
    public NavigableSet<String> getKeys() {
        return cache().navigableKeySet();
    }

    @Override
    public String getValue(String key) {
        return cache().get(key);
    }

    @Override
    public TreeMap<String, String> cache() {
        return cacheMap;
    }

    @Override
    public void lock() {

        logger.debug(String.format("Attempting to obtain read lock: %s", readWriteLock));
        this.readLock.lock();
        logger.debug(String.format("Lock Status %s", readWriteLock));
    }

    @Override
    public void unlock() {
        logger.debug(String.format("unlocking: %s", readWriteLock));
        this.readLock.unlock();
        logger.debug(String.format("Lock Status %s", readWriteLock));
    }
}
