package com.simpledb;

import com.simpledb.action.Action;
import com.simpledb.action.ActionGET;
import com.simpledb.action.ActionSET;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.DefaultMemtable;
import com.simpledb.memtable.Memtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.ActionTokenizer;
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
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultProcessor extends Processor<String> {

    //Core
    private Memtable<String> memTable;
    private final ConcurrentLinkedDeque<LookupIndex> indexStack;
    private final LogWriter<String> writer;
    private final ClientType clientType;
    private ReadWriteLock memtableLock;

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
        this.memtableLock = new ReentrantReadWriteLock(true);
        this.writer = new DefaultLogWriter(this.memtableLock);
        this.memTable = new DefaultMemtable(memtableLock, writer);
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

        actionMap.put("SET", new ActionSET(this, this.outputStream));
        actionMap.put("GET", new ActionGET(this, indexStack, this.outputStream));
    }

    @Override
    public Memtable<String> getMemTable(){
        return this.memTable;
    }

    public void run() {

        daemons.scheduleAtFixedRate(manageMemtable(), 0, 100, TimeUnit.MILLISECONDS);

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
                                action.execute((String) queryPair.getValue())
                        );


                        /*
                            Force Blocking for Actions if the Client is a Human

                            ...what if we have multiple clients?
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
        Frequentally check the size of the memtable to see if its full.

        wait until all threads currently writing to the Memtable are done... then obtain a lock
        and dump this to a file.

        needs lock because I want to ensure that no other Threads can add to the Memtable before dump is executed
     */
    public Runnable manageMemtable(){

        return ()->{
            Lock lock = memtableLock.readLock();
            lock.lock();
            try{
                if(memTable.isFull()){
                    System.out.println("MEMTABLE FULL INITATING DUMP: ");
                    logger.debug(String.format("Memtable size: %s, full: %s", memTable.getSize(), memTable.isFull()));
                    this.cacheService.submit(dump(writer, memTable));

                    //No other writes should be allowed to the Memtable now.
                    memTable.dumped();
                    memTable = new DefaultMemtable(memtableLock, writer);

                    //lets threads know a new memtable is available.
                    notifyAll();
                }
            }finally{
                lock.unlock();
                //notifies Threads that are blocked because of a Full Memtable
            }
        };
    }

    /*
        Doesn't need a lock because logically speaking nothing else should be able to continue writing to the dump while this is happening
     */
    public Runnable dump(final LogWriter<String> writer, final Memtable<String> memTable){

        return ()->{
            LookupIndex index = null;
            try {
                index = writer.dump(memTable, false);
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
