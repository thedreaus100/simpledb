package com.simpledb.memtable;

import com.simpledb.KeyValuePair;
import com.simpledb.writer.LogWriter;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Memtable<K, T> {

    protected final LogWriter<K, T> writer;

    protected long maxSize;
    protected long maxBlockSize;
    protected AtomicLong size;
    protected AtomicBoolean dumped;

    public Memtable(LogWriter<K, T> writer){

        this.writer = writer;
        this.size = new AtomicLong(0);
        this.maxSize = 1024 * 50;
        this.maxBlockSize = 1024;
        this.dumped = new AtomicBoolean(false);
    }

    public void dumped(){
        this.dumped.getAndSet(true);
    }

    public long getSize() {
        return size.get();
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public long getMaxBlockSize() {
        return maxBlockSize;
    }

    public void setMaxBlockSize(long maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
    }

    public boolean isFull() {
        return size.get() >= maxSize;
    }

    public abstract void lock();

    public abstract void unlock();

    public abstract Map<K, ? extends Object> cache();

    public abstract void insert(KeyValuePair<K, T> keyValuePair) throws MemtableException;

    public abstract Set<K> getKeys();

    public abstract T getValue(K key);
}
