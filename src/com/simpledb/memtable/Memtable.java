package com.simpledb.memtable;

import com.simpledb.KeyValuePair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public interface Memtable<K> {

    public void insert(KeyValuePair<K> keyValuePair);
    public int getSize();
    public void dumped();
    public boolean isFull();
    public TreeMap<K, Serializable> getMap();
}
