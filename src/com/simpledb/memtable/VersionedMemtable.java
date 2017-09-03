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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class VersionedMemtable<K, T> extends Memtable<K, T> {

    //Log
    private Logger logger = LogManager.getRootLogger();

    protected final TreeMap<K, ConcurrentLinkedDeque<T>> cacheMap;

    public VersionedMemtable(LogWriter<K, T> writer){

        super(writer);
        this.writeLock = writeLock;
        this.cacheMap = new TreeMap<K, ConcurrentLinkedDeque<T>>();
    }

    @Override
    public void insert(KeyValuePair<K, T> keyValuePair) throws MemtableException {

        try{
            if(this.isFull()){
                throw new MemtableFullException();
            }else if(this.dumped.get()){
                throw new MemtableDumpedException();
            }

            ConcurrentLinkedDeque<T> stack = cacheMap.get(keyValuePair.getKey());
            stack.addLast(keyValuePair.getValue());
        }finally{
            //don't want to block while calculating used space
            writeLock.unlock();
            size.addAndGet(writer.calculateSpace(keyValuePair));
        }
    }

    @Override
    public NavigableSet<K> getKeys() {
        return cacheMap.navigableKeySet();
    }

    @Override
    public T getValue(K key) {
        return cacheMap.get(key).peekLast();
    }


    @Override
    public TreeMap<K, ConcurrentLinkedDeque<T>> cache() {
        return cacheMap;
    }
}
