package com.simpledb.action;

import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.Tokenizer;
import org.omg.CORBA.DynAnyPackage.Invalid;

import java.io.*;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;

public abstract class Action<K> {

    protected Tokenizer<K> tokenizer;
    protected OutputStream out;

    public Action(Tokenizer<K> tokenizer, OutputStream out){

        this.tokenizer = tokenizer;
        this.out = out;
    }

    public Callable<Result> execute(Memtable<K> memtable, K input) throws InvalidInputException {

        if(this.tokenizer == null || this.tokenizer.getValidator().validate(input)){
           return  _execute(memtable, input);
        }else{
            throw new InvalidInputException("Invalid Input");
        }
    }

    protected void outputResult(Result result){

        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this.out));
            writer.write(result.toString());
            writer.flush();

        } catch (IOException e) {
            e.printStackTrace();
            //TODO log this
        }
    }

    protected abstract Callable<Result> _execute(Memtable<K> memtable, K input);
}
