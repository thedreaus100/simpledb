package com.simpledb;

import com.simpledb.memtable.Memtable;

public abstract class Processor<K, T> implements Runnable {

    public abstract Memtable<K, T> getMemTable();
    public abstract void wakeUpMemtableManagerThread();
}
