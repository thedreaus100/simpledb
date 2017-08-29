package com.simpledb.action;

import com.simpledb.KeyValuePair;
import com.simpledb.Processor;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.ActionSETTokenizer;
import com.simpledb.writer.LogWriter;

import java.io.OutputStream;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;

public class ActionSET extends Action<String> {

    public ActionSET(Processor processor, OutputStream out){

        super(processor, new ActionSETTokenizer(), out);
    }

    @Override
    protected Callable<Result> _execute(String input) {

        return new Callable<Result>(){

            //Want to make sure that if we have a full memtable no other processes are trying to write to a full table
            @Override
            public Result call() throws Exception {

                KeyValuePair<String> keyValuePair = tokenizer.tokenize(input);
                Memtable<String> memtable = null;
                Result result = null;

                //block until it has access to non-full Memtable.
                //should replace with Reeantract Lock
                synchronized (this){
                    while((memtable = processor.getMemTable()) == null || memtable.isFull()){
                        try{
                            wait();
                        }catch(InterruptedException e){}
                        finally{
                            memtable = processor.getMemTable();
                        }
                    }
                }

                memtable.insert(keyValuePair);
                result = new Result(String.format("INSERTED:\t%s", keyValuePair));
                outputResult(result);
                return result;
            }
        };
    }
}
