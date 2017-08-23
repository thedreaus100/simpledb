package com.simpledb.validators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class CompoundValidator<T> implements  Validator<T>{

    public List<Validator<T>> validators;

    public CompoundValidator(){

        validators = new ArrayList<Validator<T>>();
    }

    public CompoundValidator(Validator<T>... tests){
        validators = new ArrayList<Validator<T>>();
        validators.addAll(Arrays.asList(tests));
    }

    public void add(Validator<T> validator){

        validators.add(validator);
    }

    @Override
    public boolean validate(T input){

        return validators.<Predicate<T>>parallelStream()
                .map(validator -> {
                    boolean result = validator.validate(input);
                    return result;
                })
                .allMatch(pass -> pass);

    }

    @Override
    public Predicate<T> getTest(){
        return this::validate;
    }
}
