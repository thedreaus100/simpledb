package com.simpledb;

import com.simpledb.action.Action;
import com.simpledb.action.ActionGET;
import com.simpledb.action.ActionSET;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.DefaultMemtable;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.ActionSETTokenizer;
import com.simpledb.tokenizer.ActionTokenizer;
import com.simpledb.tokenizer.Tokenizer;
import com.simpledb.validators.CompoundValidator;
import com.simpledb.writer.DefaultLogWriter;
import com.simpledb.writer.LogWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

public class DefaultProcessor implements Runnable {

    private Memtable<String> memTable;
    private ActionTokenizer actionTokenizer;
    private CompoundValidator<String> validator;
    private final LogWriter<String> writer;
    private Logger logger = LogManager.getRootLogger();
    private InputStream inputStream;
    private final ExecutorService cacheService;
    private Map<String, Action<String>> actionMap;
    private final ConcurrentLinkedDeque<LookupIndex> indexStack;

    public DefaultProcessor(){
        this(System.in);
    }

    public DefaultProcessor(InputStream in){

        this.writer = new DefaultLogWriter();
        this.memTable = new DefaultMemtable(writer);
        this.actionTokenizer = new ActionTokenizer();
        this.cacheService = Executors.newCachedThreadPool();
        this.validator = new CompoundValidator<String>(
                actionTokenizer.getValidator()
                //TODO: Add Max key size check
        );

        this.inputStream = in;
        this.actionMap = new HashMap<String, Action<String>>();
        this.indexStack = new ConcurrentLinkedDeque<LookupIndex>();
        registerActions(this.actionMap);

        ExecutorContext.getInstance().register(cacheService);
    }

    public void registerActions(Map<String, Action<String>> actionMap){

        actionMap.put("SET", new ActionSET());
        actionMap.put("GET", new ActionGET(indexStack));
    }

    public void run() {

        Scanner scanner = new Scanner(this.inputStream);
        String input = null;
        do{
           try{
               System.out.print(":|\t");
               input = scanner.nextLine();
               if(input != null){
                   if(validator.validate(input)){
                       KeyValuePair<String> queryPair = this.actionTokenizer.tokenize(input);
                       Action<String> action = actionMap.get(queryPair.getKey().toUpperCase());
                       //TODO: Need to refactor KeyValuePair CAST isn't good!

                       Future<Result> futureResult = cacheService.submit(
                               action.execute(memTable, (String) queryPair.getValue())
                       );

                       logger.debug(String.format("Memtable size: %s, full: %s", memTable.getSize(), memTable.isFull()));
                       if(memTable.isFull()){
                            this.cacheService.submit(dump(writer, memTable));
                            memTable = new DefaultMemtable(writer);
                       }
                   }else{
                       System.out.println("Invalid Input");
                   }
               }else{
                   Thread.sleep(500);
               }
           }catch(Exception e){

               e.printStackTrace();
               logger.warn(e.getMessage());
           }
        }while(true);
    }

    public Runnable dump(final LogWriter<String> writer, final Memtable<String> memTable){

        return new Runnable(){

            @Override
            public void run() {
                LookupIndex index = null;
                try {
                    index = writer.dump(memTable);
                    System.out.println(index);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
