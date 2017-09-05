package com.simpledb.action;

import com.simpledb.Processor;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.Tokenizer;

import java.io.*;
import java.util.concurrent.Callable;

public abstract class Action<K, T> {

    protected Tokenizer<K, T> tokenizer;
    protected OutputStream out;
    protected final Processor processor;

    public Action(Processor processor, Tokenizer<K,T> tokenizer, OutputStream out){

        this.tokenizer = tokenizer;
        this.out = out;
        this.processor = processor;
    }

    public Callable<Result> execute(K input) throws InvalidInputException {

        if(this.tokenizer == null || this.tokenizer.getValidator().validate(input)){
           return  _execute(input);
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

    protected abstract Callable<Result> _execute(K input);
}
