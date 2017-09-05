package com.simpledb.action;

import com.simpledb.Processor;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;

import java.io.OutputStream;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ActionGET extends  Action<String, String>{

    private final ConcurrentLinkedDeque<LookupIndex> indexStack;
    public ActionGET(Processor processor, ConcurrentLinkedDeque<LookupIndex> indexStack, OutputStream out){

        super(processor,null, out);
        this.indexStack = indexStack;
    }

    @Override
    protected Callable<Result> _execute(String input) {

       return new Callable<Result>(){

           @Override
           /*
                Don't really care about thread safety here for now
            */
           public Result call() throws Exception {

               Memtable<String, String> memtable = processor.getMemTable();
               Object value = memtable.getValue(input.trim());
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
