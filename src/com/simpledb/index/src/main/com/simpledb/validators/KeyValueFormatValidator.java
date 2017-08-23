package main.com.simpledb.index.src.main.com.simpledb.validators;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public class KeyValueFormatValidator implements Validator<String>{

    private Pattern inputPattern;

    public KeyValueFormatValidator(){

        inputPattern = Pattern.compile("(\\w+),\\s*(\\w+)");
    }

    @Override
    public Predicate<String> getTest() {

        return inputPattern.asPredicate();
    }

    @Override
    public boolean validate(String input){
        return getTest().test(input);
    }
}
