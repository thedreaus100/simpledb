package com.simpledb.result;

public class Result {

    private String message;
    public Result(String message){
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString(){
        return "<\t" + message;
    }
}
