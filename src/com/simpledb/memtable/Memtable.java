package com.simpledb.memtable;

import com.simpledb.KeyValuePair;

import java.io.Serializable;
import java.util.TreeMap;

public interface Memtable<K> {

    public void insert(KeyValuePair<K> keyValuePair) throws MemtableException;
    public long getSize();
    public long getMaxSize();
    public long getMaxBlockSize();
    public void dumped();
    public boolean isFull();
    public TreeMap<K, Serializable> getMap();
}
