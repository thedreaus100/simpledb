package com.simpledb.tokenizer;

import com.simpledb.KeyValuePair;
import com.simpledb.validators.CompoundValidator;
import com.simpledb.validators.KeyValueFormatValidator;
import com.simpledb.validators.QueryValidator;
import com.simpledb.validators.Validator;

public class ActionTokenizer implements Tokenizer<String> {

    private CompoundValidator<String> validator;

    public ActionTokenizer(){
        validator = new CompoundValidator<String>(
                new QueryValidator()
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
    public KeyValuePair<String> tokenize(String input) {

        String[] values = input.split(":\\s*");
        return new KeyValuePair<String>(values[0], values[1]);
    }
}
