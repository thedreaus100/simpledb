package main.com.simpledb.index.src.main.com.simpledb.tokenizer;

import main.com.simpledb.index.src.main.com.simpledb.KeyValuePair;
import main.com.simpledb.index.src.main.com.simpledb.validators.CompoundValidator;
import main.com.simpledb.index.src.main.com.simpledb.validators.KeyValueFormatValidator;
import main.com.simpledb.index.src.main.com.simpledb.validators.Validator;

public class DefaultTokenizer implements Tokenizer<String> {

    private CompoundValidator<String> validator;

    public DefaultTokenizer(){

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
    public KeyValuePair<String> tokenize(String input) {

        String[] values = input.split(",\\s*");
        return new KeyValuePair<String>(values[0], values[1]);
    }
}
