package com.simpledb.index;

public interface LookupIndex<K> {

    public void insertKey(K key, long offset);
}
