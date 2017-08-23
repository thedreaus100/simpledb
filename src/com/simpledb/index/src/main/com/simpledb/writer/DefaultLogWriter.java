package main.com.simpledb.index.src.main.com.simpledb.writer;

import main.com.simpledb.index.src.main.com.simpledb.KeyValuePair;
import main.com.simpledb.index.src.main.com.simpledb.index.LookupIndex;
import main.com.simpledb.index.src.main.com.simpledb.memtable.Memtable;

import java.io.*;

public class DefaultLogWriter implements LogWriter<String> {

    private final char fieldDelimiter;
    private final char keyValuePairDelimiter;
    private final String encoding;

    public DefaultLogWriter(){

        this.fieldDelimiter = ';';
        this.keyValuePairDelimiter = ',';
        encoding = "UTF-8";
    }

    @Override
    public int calculateSpace(KeyValuePair<String> keyValuePair) {

        int size = 2;
        size += getBytes(keyValuePair.getKey()).length;
        if(keyValuePair.getValue() instanceof String){
            size += getBytes((String)keyValuePair.getValue()).length;
        }else{
            size += calculateSpace(keyValuePair);
        }
        return size;
    }

    public byte[] getBytes(String value){
        try{
            return value.getBytes(encoding);
        }catch(UnsupportedEncodingException e){
            return getBytes(value);
        }
    }

    public int calculateSpace(Serializable value){

        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(outputStream);){

            oos.writeObject(value);
            return outputStream.size();

        }catch(IOException e){

            //TODO process exception
            return -1; //Should denote try again... could also be a memory issue here
        }
    }

    @Override
    public LookupIndex dump(Memtable<String> memtable) {

        return null;
    }
}
