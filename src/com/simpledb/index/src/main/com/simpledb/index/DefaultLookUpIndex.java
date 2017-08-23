package main.com.simpledb.index.src.main.com.simpledb.index;

import java.util.LinkedHashMap;
import java.util.Map;

public class DefaultLookUpIndex {

    private Map<String, Integer> offsetMap;

    public DefaultLookUpIndex(){

        offsetMap = new LinkedHashMap<String, Integer>();
    }
}
