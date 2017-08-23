package main.com.simpledb.index.src.main.com.simpledb.writer;

import main.com.simpledb.index.src.main.com.simpledb.KeyValuePair;
import main.com.simpledb.index.src.main.com.simpledb.index.LookupIndex;
import main.com.simpledb.index.src.main.com.simpledb.memtable.Memtable;

public interface LogWriter<K> {

    public int calculateSpace(KeyValuePair<K> keyValuePair);
    public LookupIndex dump(Memtable<K> memtable);
}
