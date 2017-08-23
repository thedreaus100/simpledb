package main.com.simpledb.index.src.main.com.simpledb.tokenizer;

import main.com.simpledb.index.src.main.com.simpledb.KeyValuePair;
import main.com.simpledb.index.src.main.com.simpledb.validators.Validator;

public interface Tokenizer<T> {

    public boolean canTokenize(T input);
    public Validator<T> getValidator();
    public KeyValuePair<T> tokenize(T input);
}
