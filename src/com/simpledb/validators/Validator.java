package com.simpledb.validators;

import java.util.function.Predicate;

public interface Validator<T> {

    public Predicate<T> getTest();
    public boolean validate(T input);
}
