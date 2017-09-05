package com.simpledb.memtable;

public class MemtableException extends Exception {

    protected String message;

    public MemtableException(String message){
        this.message = message;
    }

    @Override
    public String getMessage(){
        return this.message;
    }
}
