package com.simpledb.action;

import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;

import java.io.OutputStream;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ActionGET extends  Action<String>{

    private final ConcurrentLinkedDeque<LookupIndex> indexStack;
    public ActionGET(ConcurrentLinkedDeque<LookupIndex> indexStack, OutputStream out){

        super(null, out);
        this.indexStack = indexStack;
    }

    @Override
    protected Callable<Result> _execute(Memtable<String> memtable, String input) {

       return new Callable<Result>(){

           @Override
           public Result call() throws Exception {
               Object value = memtable.getMap().get(input.trim());
               Result result = null;
               if(value != null){

                   result = new Result(value.toString());
               }else{

                   result = new Result("the data should be in the LookupIndex Stack... but this hasn't been implemented yet :(");
               }
               outputResult(result);
               return result;
           }
       };
    }
}
