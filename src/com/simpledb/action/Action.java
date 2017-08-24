package com.simpledb.action;

import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.Tokenizer;
import org.omg.CORBA.DynAnyPackage.Invalid;

import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;

public abstract class Action<K> {

    protected Tokenizer<K> tokenizer;
    public Action(Tokenizer<K> tokenizer){
        this.tokenizer = tokenizer;
    }

    public Callable<Result> execute(Memtable<K> memtable, ConcurrentLinkedDeque<LookupIndex> indexStack, K input) throws InvalidInputException {

        if(this.tokenizer == null || this.tokenizer.getValidator().validate(input)){
           return  _execute(memtable, indexStack, input);
        }else{
            throw new InvalidInputException("Invalid Input");
        }
    }

    protected abstract Callable<Result> _execute(Memtable<K> memtable,  ConcurrentLinkedDeque<LookupIndex> indexStack, K input);
}
