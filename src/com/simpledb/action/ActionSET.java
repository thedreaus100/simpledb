package com.simpledb.action;

import com.simpledb.KeyValuePair;
import com.simpledb.Processor;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.memtable.MemtableException;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.ActionSETTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.OutputStream;
import java.util.concurrent.Callable;

public class ActionSET extends Action<String, String> {

    //Log
    private Logger logger = LogManager.getRootLogger();
    private final ActionSET self;

    public ActionSET(Processor processor, OutputStream out){

        super(processor, new ActionSETTokenizer(), out);
        self = this;
    }

    @Override
    protected Callable<Result> _execute(String input) {

        return new Callable<Result>(){

            @Override
            public Result call() throws Exception {

                //Want to make sure that if we have a full memtable no other processes are trying to write to a full table
                KeyValuePair<String, String> keyValuePair = tokenizer.tokenize(input);
                Result result = null;
                boolean complete = false;
                do{
                    try{
                        //Grab available Memtable which can change because of the ManageMemtable Thread.
                        Memtable<String, String> memtable = processor.waitForNextMemtable();
                        memtable.insert(keyValuePair);
                        complete = true;
                        result = new Result(String.format("INSERTED:\t%s", keyValuePair));
                        outputResult(result);
                    }catch(MemtableException e){}
                }while(!complete);
                return result;
            }
        };
    }
}
