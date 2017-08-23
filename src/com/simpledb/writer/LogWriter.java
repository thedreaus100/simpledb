package com.simpledb.writer;

import com.simpledb.KeyValuePair;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;

import java.io.IOException;

public interface LogWriter<K> {

    public int calculateSpace(KeyValuePair<K> keyValuePair);
    public LookupIndex dump(final Memtable<K> memtable) throws IOException;
}
