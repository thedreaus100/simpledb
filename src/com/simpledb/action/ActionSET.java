package com.simpledb.action;

import com.simpledb.KeyValuePair;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.ActionSETTokenizer;

import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ActionSET extends Action<String> {

    public ActionSET(){
        super(new ActionSETTokenizer());
    }

    @Override
    protected Result _execute(Memtable<String> memtable, ConcurrentLinkedDeque<LookupIndex> indexStack, String input) {

        KeyValuePair<String> keyValuePair = tokenizer.tokenize(input);
        memtable.insert(keyValuePair);
        return new Result(String.format("INSERTED:\t%s", keyValuePair));
    }
}
