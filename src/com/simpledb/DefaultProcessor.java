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
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Stack;
import java.util.concurrent.*;
import java.util.function.BiFunction;

public class DefaultProcessor implements Runnable {

    //Core
    private Memtable<String> memTable;
    private final ConcurrentLinkedDeque<LookupIndex> indexStack;
    private final LogWriter<String> writer;
    private final ClientType clientType;

    //Executors
    private final ExecutorService cacheService;
    private final ScheduledExecutorService daemons;

    //Client Input
    private String prompt = ">\t";
    private Map<String, Action<String>> actionMap;
    private ActionTokenizer actionTokenizer;
    private CompoundValidator<String> validator;

    //Log
    private Logger logger = LogManager.getRootLogger();

    //IO
    private InputStream inputStream;
    private OutputStream outputStream;
    private PrintStream printStream;

    public enum ClientType{
        CMD(0), API(1);

        private int type;
        private ClientType(int type){
            this.type = type;
        }

        public int getType() {
            return type;
        }
    }

    public DefaultProcessor(){

        this(ClientType.CMD, System.in, System.out);
    }

    public DefaultProcessor(ClientType clientType, InputStream in, OutputStream out){

        this.clientType = clientType;
        this.writer = new DefaultLogWriter();
        this.memTable = new DefaultMemtable(writer);
        this.actionTokenizer = new ActionTokenizer();
        this.cacheService = Executors.newCachedThreadPool();
        this.daemons = Executors.newScheduledThreadPool(2);
        this.validator = new CompoundValidator<String>(
                actionTokenizer.getValidator()
                //TODO: Add Max key size check
        );

        this.inputStream = in;
        this.outputStream = out;
        this.printStream = new PrintStream(out);
        this.actionMap = new HashMap<String, Action<String>>();
        this.indexStack = new ConcurrentLinkedDeque<LookupIndex>();
        registerActions(this.actionMap);

        ExecutorContext.getInstance()
                .register(cacheService)
                .register(daemons);
    }

    public void registerActions(Map<String, Action<String>> actionMap){

        actionMap.put("SET", new ActionSET(this.outputStream));
        actionMap.put("GET", new ActionGET(indexStack, this.outputStream));
    }

    public void run() {

        daemons.scheduleAtFixedRate(manageMemtable(), 0, 1000, TimeUnit.MILLISECONDS);

        //Main Thread
        processActions();
    }

    public void processActions(){

        Scanner scanner = new Scanner(this.inputStream);
        String input = null;

        prompt();
        do{
            try{
                input = scanner.nextLine();
                if(input != null){
                    if(validator.validate(input)){
                        KeyValuePair<String> queryPair = this.actionTokenizer.tokenize(input);
                        Action<String> action = actionMap.get(queryPair.getKey().toUpperCase());
                        //TODO: Need to refactor KeyValuePair CAST isn't good!

                        Future<Result> futureResult = cacheService.submit(
                                action.execute(memTable, (String) queryPair.getValue())
                        );


                        /*
                            Force Blocking for Actions if the Client is a Human
                         */
                        if(this.clientType.equals(ClientType.CMD)){
                            Result result = futureResult.get(10, TimeUnit.SECONDS);
                            this.printStream.println();
                        }
                    }else{
                        printStream.println("Invalid Input");
                    }
                    prompt();
                }else{
                    //wait for user input
                    Thread.sleep(1000);
                }
            }catch(InterruptedException e){

                e.printStackTrace();
                logger.error(e);
                break;
            }
            catch (ExecutionException e) {
                logger.error("COMMAND FAILED TO EXECUTE: ", e);
                prompt();
            } catch (TimeoutException e) {
                logger.error("COMMAND FAILED TO EXECUTE: ", e);
                prompt();
            }
        }while(true && !Thread.interrupted());
    }

    /*
        Frequentally check the size of the memtable to see if its full if it is full... block subsequent writes
        to the memtable, then spawn a process to dump the memtable to file.

        ideally this would only need to run if the db is actively being written too.
     */
    public Runnable manageMemtable(){

        return ()->{
            //Place Lock on writing to memtable here!!!! nothing should be able to write anything while this is going on!!
            logger.debug(String.format("Memtable size: %s, full: %s", memTable.getSize(), memTable.isFull()));
            if(memTable.isFull()){
                this.cacheService.submit(dump(writer, memTable));
                memTable = new DefaultMemtable(writer);
            }
        };
    }

    public Runnable dump(final LogWriter<String> writer, final Memtable<String> memTable){

        return ()->{
            LookupIndex index = null;
            try {
                index = writer.dump(memTable);
                System.out.println(index);
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }

    public void prompt(){
        if(clientType.equals(ClientType.CMD)){
            this.printStream.print(this.prompt);
        }
    }
}
