package com.simpledb.writer;

import com.simpledb.KeyValuePair;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;

import java.io.IOException;

public interface LogWriter<K, T> {

    public int calculateSpace(KeyValuePair<K, T> keyValuePair);
    public LookupIndex dump(final Memtable<K, T> memtable, boolean shouldLock) throws IOException;
}
