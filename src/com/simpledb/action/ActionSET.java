package com.simpledb.action;

import com.simpledb.KeyValuePair;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.ActionSETTokenizer;
import com.simpledb.writer.LogWriter;

import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;

public class ActionSET extends Action<String> {

    public ActionSET(){

        super(new ActionSETTokenizer());
    }

    @Override
    protected Callable<Result> _execute(Memtable<String> memtable, String input) {

        return new Callable<Result>(){

            //Want to make sure that if we have a full memtable no other processes are trying to write to a full table
            @Override
            public Result call() throws Exception {

                KeyValuePair<String> keyValuePair = tokenizer.tokenize(input);
                memtable.insert(keyValuePair);
                return new Result(String.format("INSERTED:\t%s", keyValuePair));
            }
        };
    }
}
