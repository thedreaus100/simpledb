package com.simpledb.writer;

import com.simpledb.KeyValuePair;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import org.joda.time.DateTime;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DefaultLogWriter implements LogWriter<String> {

    private final char fieldDelimiter;
    private final char keyValuePairDelimiter;
    private final Charset encoding;
    private String dirname;

    public DefaultLogWriter(String dirname){

        this.fieldDelimiter = ';';
        this.keyValuePairDelimiter = ',';
        this.dirname = dirname;
        encoding = StandardCharsets.UTF_8;
    }

    public DefaultLogWriter(){

        this("." + File.separator + "data");
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

        return value.getBytes(encoding);
    }

    public int calculateSpace(Serializable value){

        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(outputStream);){

            //Transform
            oos.writeObject(value);
            return outputStream.size();

        }catch(IOException e){

            //TODO process exception
            return -1; //Should denote try again... could also be a memory issue here
        }
    }

    @Override
    public LookupIndex dump(Memtable<String> memtable) throws IOException {

        //LookUpIndex {file_name, Map[{"allison": 0},"cathy":"256"....]
        String fileName = this.dirname + File.separator + DateTime.now().getMillis();
        File file = new File(fileName);
        try(FileOutputStream fos = new FileOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            ObjectOutputStream oos = new ObjectOutputStream(bos)){

            memtable
                .getMap()
                .navigableKeySet()
                .stream().forEach(key -> {
                    try{
                        Serializable value = memtable.getMap().get(key);
                        bos.write(getBytes(key));
                        bos.write(keyValuePairDelimiter);
                        oos.writeObject(value);
                        bos.write(this.fieldDelimiter);

                        //pos in file???

                    }catch(IOException e){
                        e.printStackTrace();
                    }
                });

            return null;
        }catch(IOException e){

            e.printStackTrace();
            throw e;
        }
    }
}
