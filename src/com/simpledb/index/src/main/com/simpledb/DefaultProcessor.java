package main.com.simpledb.index.src.main.com.simpledb;

import main.com.simpledb.index.src.main.com.simpledb.memtable.DefaultMemtable;
import main.com.simpledb.index.src.main.com.simpledb.memtable.Memtable;
import main.com.simpledb.index.src.main.com.simpledb.tokenizer.DefaultTokenizer;
import main.com.simpledb.index.src.main.com.simpledb.tokenizer.Tokenizer;
import main.com.simpledb.index.src.main.com.simpledb.validators.CompoundValidator;
import main.com.simpledb.index.src.main.com.simpledb.writer.DefaultLogWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Scanner;

public class DefaultProcessor implements Runnable {

    private Memtable<String> memTable;
    private Tokenizer<String> tokenizer;
    private CompoundValidator<String> validator;
    private final DefaultLogWriter writer;
    private Logger logger = LogManager.getRootLogger();

    public DefaultProcessor(){

        this.writer = new DefaultLogWriter();
        this.memTable = new DefaultMemtable(writer);
        this.tokenizer = new DefaultTokenizer();
        this.validator = new CompoundValidator<String>(
                tokenizer.getValidator()
                //TODO: Add Max key size check
        );
    }

    public void run() {

        Scanner scanner = new Scanner(System.in);
        String input = null;
        do{
           try{
               System.out.print(":\t");
               input = scanner.nextLine();
               if(input != null){
                   input.split(",");
                   if(validator.validate(input)){
                       KeyValuePair<String> keyValuePair = tokenizer.tokenize(input);
                       memTable.insert(keyValuePair);
                       logger.debug(String.format("Memtable size: %s, full: %s", memTable.getSize(), memTable.isFull()));
                       if(memTable.isFull()){

                       }
                       System.out.println(String.format("INSERTED:\t%s", keyValuePair));
                   }else{
                       System.out.println("Invalid Input");
                   }
               }else{
                   System.out.println("WAITING...");
                   Thread.sleep(500);
               }
           }catch(Exception e){

               e.printStackTrace();
               logger.warn(e.getMessage());
           }
        }while(true);
    }
}
