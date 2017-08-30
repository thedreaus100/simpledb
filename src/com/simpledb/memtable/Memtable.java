package com.simpledb.memtable;

import com.simpledb.KeyValuePair;

import java.io.Serializable;
import java.util.TreeMap;

public interface Memtable<K> {

    public void insert(KeyValuePair<K> keyValuePair);
    public int getSize();
    public void dumped();
    public boolean isFull();
    public TreeMap<K, Serializable> getMap();
}
