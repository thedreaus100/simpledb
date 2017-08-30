package com.simpledb.index;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public class DefaultLookUpIndex implements  LookupIndex<String> {

    protected Map<String, Long> offsetMap;
    protected File file;

    public DefaultLookUpIndex(File file){

        offsetMap = new LinkedHashMap<String, Long>();
        this.file = file;
    }

    public void insertKey(String key, long offset){

        offsetMap.put(key, offset);
    }

    @Override
    public String toString(){
        return String.format("Look Up Index - file:  %s, index:  %s", file.getName(), offsetMap);
    }
}
