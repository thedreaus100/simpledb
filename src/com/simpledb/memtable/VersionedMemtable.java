package com.simpledb.memtable;

import com.simpledb.KeyValuePair;
import com.simpledb.writer.LogWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class VersionedMemtable<K,T> extends Memtable<K, T> {

    //Log
    private Logger logger = LogManager.getRootLogger();

    protected final TreeMap<String, ConcurrentLinkedDeque<String>> cacheMap;

    public VersionedMemtable(LogWriter<K, T> writer){

        super(writer);
        this.writeLock = writeLock;
        this.cacheMap = new TreeMap<String, ConcurrentLinkedDeque<String>>();
    }

    @Override
    public void insert(KeyValuePair<K, T> keyValuePair) throws MemtableException {

        //TODO Implement this
    }

    @Override
    public Set<K> getKeys() {
        return null;
    }

    @Override
    public T getValue(K key) {
        return null;
    }

    @Override
    public Map<K, T> cache() {
        return null;
    }
}
