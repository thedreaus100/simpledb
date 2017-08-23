package com.simpledb.action;

public class InvalidInputException extends RuntimeException {

    private String message;

    public InvalidInputException(String message){
        this.message = message;
    }

    @Override
    public String getMessage(){
        return this.message;
    }
}
