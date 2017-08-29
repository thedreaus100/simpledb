package com.simpledb;

import com.simpledb.memtable.Memtable;

public abstract class Processor<K> implements Runnable {

    public abstract Memtable<K> getMemTable();
}
