package com.simpledb.action;

import com.simpledb.KeyValuePair;
import com.simpledb.Processor;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.ActionSETTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.OutputStream;
import java.util.concurrent.Callable;

public class ActionSET extends Action<String> {

    //Log
    private Logger logger = LogManager.getRootLogger();
    private final ActionSET self;

    public ActionSET(Processor processor, OutputStream out){

        super(processor, new ActionSETTokenizer(), out);
        self = this;
    }

    @Override
    protected Callable<Result> _execute(String input) {

        return ()->{

            //Want to make sure that if we have a full memtable no other processes are trying to write to a full table
            KeyValuePair<String> keyValuePair = tokenizer.tokenize(input);
            Memtable<String> memtable = null;
            Result result = null;

            //block until it has access to non-full Memtable.
            //once full interrupt Memtable manager Thread so that it can dump the memtable.
            synchronized (self){

                while((memtable = processor.getMemTable()) == null || memtable.isFull()){
                    logger.debug(String.format("MEMTABLE FULL - Size: \t%s", memtable.getSize()));
                    processor.wakeUpMemtableManagerThread();

                    try{
                        //Wait for Managememtable to dump memtable
                        wait(1000);
                    }catch(InterruptedException e){};
                    logger.debug("attempting to resume SET");
                }
            }

            memtable.insert(keyValuePair);
            result = new Result(String.format("INSERTED:\t%s", keyValuePair));
            outputResult(result);
            return result;
        };
    }
}
