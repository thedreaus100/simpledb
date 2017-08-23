package com.simpledb.action;

import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;

import java.util.Stack;

public class ActionGET extends  Action<String>{

    public ActionGET(){
        super(null);
    }

    @Override
    protected Result _execute(Memtable<String> memtable, Stack<LookupIndex> indexStack, String input) {
        Object value = memtable.getMap().get(input.trim());
        if(value != null){

            return new Result(value.toString());
        }else{

            System.out.println("Need to traverse lookup index stack");
            return null;
        }
    }
}
