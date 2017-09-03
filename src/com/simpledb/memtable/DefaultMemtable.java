package com.simpledb.memtable;

import com.simpledb.KeyValuePair;
import com.simpledb.writer.LogWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultMemtable extends Memtable<String, String> {

    //Log
    private Logger logger = LogManager.getRootLogger();
    protected final TreeMap<String, String> cacheMap;

    public DefaultMemtable(ReentrantReadWriteLock.WriteLock writeLock, LogWriter<String, String> writer){

        super(writer);
        this.cacheMap = new TreeMap<String, String>();
        this.writeLock = writeLock;
    }

    @Override
    public void insert(KeyValuePair<String, String> keyValuePair) throws MemtableException {

       //blocks as long as nothing else is concurrently writing... and there are no ongoing reads
       logger.debug("Attempting to obtain write lock: " + writeLock);
       writeLock.lock();
       logger.debug(String.format("Writing key %s\t", keyValuePair.getKey()));
       try{
           if(this.isFull()){
               throw new MemtableFullException();
           }else if(this.dumped.get()){
               throw new MemtableDumpedException();
           }
           cache().put(keyValuePair.getKey(), keyValuePair.getValue());
       }finally{
           //don't want to block while calculating used space
           writeLock.unlock();
           size.addAndGet(writer.calculateSpace(keyValuePair));
       }
    }

    @Override
    public Set<String> getKeys() {
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
}
