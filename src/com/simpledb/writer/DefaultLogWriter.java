package com.simpledb.writer;

import com.simpledb.KeyValuePair;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultLogWriter implements LogWriter<String> {

    private final char fieldDelimiter;
    private final char keyValuePairDelimiter;
    private final Charset encoding;
    private String dirname;
    protected ReentrantReadWriteLock.ReadLock readLock;

    //Log
    private Logger logger = LogManager.getRootLogger();

    public DefaultLogWriter(ReentrantReadWriteLock.ReadLock readLock, String dirname){

        this.fieldDelimiter = ';';
        this.keyValuePairDelimiter = ',';
        this.dirname = dirname;
        this.readLock = readLock;
        encoding = StandardCharsets.UTF_8;
    }

    public DefaultLogWriter(ReentrantReadWriteLock.ReadLock readLock){

        this(readLock, "." + File.separator + "data");
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

            return -1;
        }
    }

    /*
        Be careful that nothing else can write to this Memtable while this operation is going on.


     */
    @Override
    public LookupIndex dump(Memtable<String> memtable, boolean shouldLock) throws IOException {

       if(shouldLock){
           readLock.lock();
           try{
               return _dump(memtable);
           }finally{
               readLock.unlock();
           }
       }else{
           return _dump(memtable);
       }
    }

    public LookupIndex _dump(Memtable<String> memtable) throws IOException {

        logger.debug("Initiating Dump");
        String fileName = this.dirname + File.separator + DateTime.now().getMillis();
        File file = new File(fileName);
        try(FileOutputStream fos = new FileOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            ObjectOutputStream oos = new ObjectOutputStream(bos)){

            long pos = 0;
            ByteBuffer buffer = ByteBuffer.allocate(memtable.getSize());
            int partionSize = memtable.getSize()/4;
            for(String key:memtable.getMap().navigableKeySet()){

                Serializable value = memtable.getMap().get(key);
                buffer.put(getBytes(key));
                buffer.putChar(keyValuePairDelimiter);
                buffer.put(getBytes((String) value)); //will add unnecessary space
                buffer.putChar(this.fieldDelimiter);

                if(buffer.position() >= partionSize){

                    pos += buffer.position();
                    logger.debug("File POSTION: " + pos);
                    buffer.flip();

                    //Find class to do this more effeciently don't want to write one byte at a time
                    while(buffer.hasRemaining()){
                        bos.write(buffer.get());
                    }
                    buffer.flip();
                }
            }

            return null;
        }catch(IOException e){

            e.printStackTrace();
            throw e;
        }
    }
}
