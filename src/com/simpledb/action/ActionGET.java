package com.simpledb.action;

import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;

import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ActionGET extends  Action<String>{

    private final ConcurrentLinkedDeque<LookupIndex> indexStack;
    public ActionGET(ConcurrentLinkedDeque<LookupIndex> indexStack){

        super(null);
        this.indexStack = indexStack;
    }

    @Override
    protected Callable<Result> _execute(Memtable<String> memtable, String input) {

       return new Callable<Result>(){

           @Override
           public Result call() throws Exception {
               Object value = memtable.getMap().get(input.trim());
               if(value != null){

                   return new Result(value.toString());
               }else{

                   System.out.println("Need to traverse lookup index stack");
                   return null;
               }
           }
       };
    }
}
