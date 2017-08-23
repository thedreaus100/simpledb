package main.com.simpledb.index.src.main.com.simpledb.memtable;

import main.com.simpledb.index.src.main.com.simpledb.KeyValuePair;

public interface Memtable<K> {

    public void insert(KeyValuePair<K> keyValuePair);
    public int getSize();
    public boolean isFull();
}
