package com.simpledb.memtable;

import com.simpledb.KeyValuePair;

import java.io.Serializable;
import java.util.TreeMap;

public class VersionedMemtable<K> implements Memtable<K> {



    @Override
    public void insert(KeyValuePair<K> keyValuePair) throws MemtableException {

    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public long getMaxSize() {
        return 0;
    }

    @Override
    public long getMaxBlockSize() {
        return 0;
    }

    @Override
    public void dumped() {

    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public TreeMap<K, Serializable> getMap() {
        return null;
    }
}
