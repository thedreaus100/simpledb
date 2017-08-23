package com.simpledb.tokenizer;

import com.simpledb.KeyValuePair;
import com.simpledb.validators.Validator;

import java.util.function.Predicate;

public interface Tokenizer<T> {

    public boolean canTokenize(T input);
    public Validator<T> getValidator();
    public KeyValuePair<T> tokenize(T input);
}
