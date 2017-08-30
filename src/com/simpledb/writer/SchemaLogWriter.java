package com.simpledb.writer;

import com.simpledb.KeyValuePair;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.Memtable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;

public class SchemaLogWriter implements LogWriter<String>{

    protected String schemaDir;
    protected String dataDir;

    public SchemaLogWriter(){
        this("." + File.separator + "schemas", "." + File.separator + "data");
    }

    public SchemaLogWriter(String schemaDir, String dataDir){
        this.schemaDir = schemaDir;
        this.dataDir = dataDir;
    }

    /*
        Need better way to calculate bytes
     */
    @Override
    public int calculateSpace(KeyValuePair<String> keyValuePair) {

        int length = 0;
        length += keyValuePair.getKey().getBytes().length;
        if(keyValuePair.getValue() != null){
            length += ((String)keyValuePair.getValue()).getBytes().length;
        }

        return length;
    }

    @Override
    public LookupIndex dump(Memtable<String> memtable, boolean shouldLock) throws IOException {

        Schema schema = new Schema.Parser().parse(new File(this.schemaDir + File.separator + "default.avsc"));
        File file = new File(this.dataDir + File.separator + String.format("%s.avro", DateTime.now().getMillis()));
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

        try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)){
            dataFileWriter.create(schema, file);
            for(String key:memtable.getMap().navigableKeySet()){

                GenericRecord record = new GenericData.Record(schema);
                record.put("key", key);
                record.put("value", memtable.getMap().get(key).toString());
                record.put("timestamp", DateTime.now().getMillis());

                dataFileWriter.append(record);
            }
        }
        return null;
    }
}