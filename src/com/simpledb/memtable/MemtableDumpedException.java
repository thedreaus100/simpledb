package com.simpledb.memtable;

public class MemtableDumpedException extends MemtableException {

    public MemtableDumpedException(){
        super("Error: Attempted write to dumped memtable");
    }
}
