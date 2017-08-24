package com.simpledb.action;

import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;

import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ActionGET extends  Action<String>{

    public ActionGET(){
        super(null);
    }

    @Override
    protected Callable<Result> _execute(Memtable<String> memtable, ConcurrentLinkedDeque<LookupIndex> indexStack, String input) {

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
