package com.simpledb.validators;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public class QueryValidator implements Validator<String> {

    private Pattern inputPattern;

    public QueryValidator(){

        inputPattern = Pattern.compile("(\\w+):\\s*(.+)");
    }

    @Override
    public Predicate<String> getTest() {
        return inputPattern.asPredicate();
    }

    @Override
    public boolean validate(String input) {
        return getTest().test(input);
    }
}
