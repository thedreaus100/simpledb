package main.com.simpledb.index.src.main.com.simpledb.memtable;

import main.com.simpledb.index.src.main.com.simpledb.KeyValuePair;
import main.com.simpledb.index.src.main.com.simpledb.writer.LogWriter;

import java.util.TreeMap;

public class DefaultMemtable implements Memtable<String> {

    private TreeMap<String, Object> cacheMap;
    private int maxSize;
    private int size = 0;
    private final LogWriter<String> writer;

    public DefaultMemtable(LogWriter<String> writer){

        this.writer = writer;
        cacheMap = new TreeMap<String, Object>();
        maxSize = 1024;
    }

    @Override
    public void insert(KeyValuePair<String> keyValuePair) {
        cacheMap.put(keyValuePair.getKey(), keyValuePair.getValue());
        size += writer.calculateSpace(keyValuePair);
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public boolean isFull() {
        return size >= maxSize;
    }
}
