package com.simpledb;

import com.simpledb.action.Action;
import com.simpledb.action.ActionGET;
import com.simpledb.action.ActionSET;
import com.simpledb.index.LookupIndex;
import com.simpledb.memtable.DefaultMemtable;
import com.simpledb.memtable.Memtable;
import com.simpledb.memtable.VersionedMemtable;
import com.simpledb.result.Result;
import com.simpledb.tokenizer.ActionTokenizer;
import com.simpledb.validators.CompoundValidator;
import com.simpledb.writer.DefaultLogWriter;
import com.simpledb.writer.LogWriter;
import com.simpledb.writer.SchemaLogWriter;
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

public class DefaultProcessor extends Processor<String, String> {

    //Core
    private Memtable<String, String> memTable;
    private final ConcurrentLinkedDeque<LookupIndex> indexStack;
    private final LogWriter<String, String> writer;
    private final ClientType clientType;
    private Thread memtableManagerThread = null;
    private Boolean alive = true;

    //Executors
    private final ExecutorService cacheService;
    private final ScheduledExecutorService daemons;
    private final ExecutorService memtableManagerService;

    //Client Input
    private String prompt = ">\t";
    private Map<String, Action<String, String>> actionMap;
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

        //this.writer = new DefaultLogWriter(this.memtableReadLock);
        //this.memTable = new DefaultMemtable(writer);

        this.writer = new SchemaLogWriter();
        this.memTable = new VersionedMemtable<String, String>(writer);
        this.actionTokenizer = new ActionTokenizer();
        this.cacheService = Executors.newCachedThreadPool();
        this.daemons = Executors.newScheduledThreadPool(2);
        this.memtableManagerService = Executors.newSingleThreadExecutor();
        this.validator = new CompoundValidator<String>(
                actionTokenizer.getValidator()
                //TODO: Add Max key size check
        );

        this.inputStream = in;
        this.outputStream = out;
        this.printStream = new PrintStream(out);
        this.actionMap = new HashMap<String, Action<String, String>>();
        this.indexStack = new ConcurrentLinkedDeque<LookupIndex>();
        registerActions(this.actionMap);

        ExecutorContext.getInstance()
                .register(cacheService)
                .register(memtableManagerService)
                .register(daemons);
    }

    public void registerActions(Map<String, Action<String, String>> actionMap){

        actionMap.put("SET", new ActionSET(this, this.outputStream));
        actionMap.put("GET", new ActionGET(this, indexStack, this.outputStream));
    }

    @Override
    public Memtable<String, String> getMemTable(){
        return this.memTable;
    }

    /*
       block until it has access to non-full Memtable.
       once full interrupt Memtable manager Thread so that it can dump the memtable.
    */
    @Override
    public synchronized Memtable<String, String> waitForNextMemtable() {

        Memtable<String, String> memtable = null;
        while((memtable = getMemTable()) == null || memtable.isFull()){
            logger.debug(String.format("MEMTABLE FULL - Size: \t%s", memtable.getSize()));
            wakeUpMemtableManagerThread();

            try{
                //Wait for Managememtable to dump memtable
                wait(1000);
            }catch(InterruptedException e){};
            logger.debug("attempting to resume SET");
        }

        return memtable;
    }

    @Override
    public synchronized void wakeUpMemtableManagerThread(){

        if(this.memtableManagerThread != null
                && (this.memtableManagerThread.getState().equals(Thread.State.TIMED_WAITING) || this.memtableManagerThread.getState().equals(Thread.State.WAITING))){
            logger.debug(String.format("WAKING UP THREAD: %s \t %s", this.memtableManagerThread, this.memtableManagerThread.getState()));
            this.memtableManagerThread.interrupt();
        }
    }

    public void run() {

        logger.debug("\n\nSIMPLE DB\n\n");
        memtableManagerService.submit(manageMemtable(5000));
        //Main Thread
        processActions();
    }

    public void exit(){

        alive = false;
        ExecutorContext.getInstance().shutdown();
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
                        KeyValuePair<String, String> queryPair = this.actionTokenizer.tokenize(input);
                        Action<String, String> action = actionMap.get(queryPair.getKey().toUpperCase());
                        //TODO: Need to refactor KeyValuePair CAST isn't good!

                        Future<Result> futureResult = cacheService.submit(
                                action.execute((String) queryPair.getValue())
                        );

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
                    //Add atomic boolean isAsleep to prevent multiple Threads from interrupting this Thread!
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
        }while(alive);

        logger.debug("exit");
    }

    /*
        Frequentally check the size of the memtable to see if its full.

        wait until all threads currently writing to the Memtable are done... then obtain a lock
        and dump this to a file.

        needs lock in order to ensure that no other Threads can add to the Memtable before dump is executed
     */
    public Runnable manageMemtable(long timeout){

        return ()->{

            memtableManagerThread = Thread.currentThread();
            memtableManagerThread.setName("Manage Memtable Thread");

            while(true){
               try{
                   logger.debug("PROCESSING...");
                   Memtable<String, String> currentMemtable = memTable;

                   //wait until memtable is safe to read, then block futher writes.
                   currentMemtable.lock();
                   try{
                       if(currentMemtable.isFull()){

                           logger.debug("Memtable full initating dump!!!");
                           this.cacheService.submit(dump(writer, currentMemtable));

                           //Obtain lock for notifyall
                           synchronized (this){

                              currentMemtable.dumped();
                              memTable = new VersionedMemtable(writer);

                              logger.debug("NOTIFIYING DUMP COMPLETION");
                              notifyAll();
                              logger.debug("DUMP COMPLETE!");
                          }
                       }
                   }finally{
                       currentMemtable.unlock();
                   }

                   Thread.sleep(timeout);
               }catch(InterruptedException e){

                  logger.debug("Sleep interrupted");
               }
            }
        };
    }

    /*
        Doesn't need a lock because logically speaking nothing else should be able to continue writing to the dump while this is happening
     */
    public Runnable dump(final LogWriter<String, String> writer, final Memtable<String, String> memTable){

        return ()->{

            LookupIndex index = null;
            try {
                index = writer.dump(memTable, false);
                logger.debug(index);
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
