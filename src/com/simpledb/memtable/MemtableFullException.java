package com.simpledb.memtable;

public class MemtableFullException extends MemtableException {

    public MemtableFullException(){
        super("Memtable full");
    }
}
