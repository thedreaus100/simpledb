package com.simpledb.memtable;

import com.simpledb.KeyValuePair;
import com.simpledb.writer.LogWriter;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DefaultMemtable implements Memtable<String> {

    private ConcurrentSkipListMap<String, Serializable> cacheMap;
    private int maxSize;
    private int size = 0;
    private final LogWriter<String> writer;

    public DefaultMemtable(LogWriter<String> writer){

        this.writer = writer;
        cacheMap = new ConcurrentSkipListMap<String, Serializable>();
        maxSize = 1024;
    }

    @Override
    public void insert(KeyValuePair<String> keyValuePair) {
        cacheMap.put(keyValuePair.getKey(), keyValuePair.getValue());

        synchronized (this){
            size += writer.calculateSpace(keyValuePair);
        }
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public boolean isFull() {
        return size >= maxSize;
    }

    @Override
    public ConcurrentSkipListMap<String, Serializable> getMap() {

        return cacheMap;
    }
}
