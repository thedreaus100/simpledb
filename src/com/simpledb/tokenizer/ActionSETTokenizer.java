package com.simpledb.tokenizer;

import com.simpledb.KeyValuePair;
import com.simpledb.tokenizer.Tokenizer;
import com.simpledb.validators.CompoundValidator;
import com.simpledb.validators.KeyValueFormatValidator;
import com.simpledb.validators.Validator;

public class ActionSETTokenizer implements Tokenizer<String, String> {

    private CompoundValidator<String> validator;

    public ActionSETTokenizer(){

       validator = new CompoundValidator<String>(
               new KeyValueFormatValidator()
               //TODO: Add Invalid Char Validator
       );
    }


    @Override
    public boolean canTokenize(String input) {
        return validator.validate(input);
    }

    @Override
    public Validator<String> getValidator() {
        return validator;
    }


    @Override
    public KeyValuePair<String, String> tokenize(String input) {

        String[] values = input.split(",\\s*");
        return new KeyValuePair<String, String>(values[0], values[1]);
    }
}
