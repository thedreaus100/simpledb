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

public class DefaultProcessor extends Processor<String> {

    //Core
    private Memtable<String> memTable;
    private final ConcurrentLinkedDeque<LookupIndex> indexStack;
    private final LogWriter<String> writer;
    private final ClientType clientType;
    private ReentrantReadWriteLock.ReadLock memtableReadLock;
    private ReentrantReadWriteLock.WriteLock memtableWriteLock;
    private ReentrantReadWriteLock readWriteLock;
    private Thread memtableManagerThread = null;

    //Executors
    private final ExecutorService cacheService;
    private final ScheduledExecutorService daemons;
    private final ExecutorService memtableManagerService;

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
        this.readWriteLock = new ReentrantReadWriteLock(true);
        this.memtableReadLock = readWriteLock.readLock();
        this.memtableWriteLock = readWriteLock.writeLock();
        //this.writer = new DefaultLogWriter(this.memtableReadLock);
        this.writer = new SchemaLogWriter();
        this.memTable = new DefaultMemtable(memtableWriteLock, writer);
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
        this.actionMap = new HashMap<String, Action<String>>();
        this.indexStack = new ConcurrentLinkedDeque<LookupIndex>();
        registerActions(this.actionMap);

        ExecutorContext.getInstance()
                .register(cacheService)
                .register(memtableManagerService)
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

    @Override
    public synchronized void wakeUpMemtableManagerThread(){

        if(this.memtableManagerThread != null){
            logger.debug("WAKING UP THREAD!!!");
            this.memtableManagerThread.interrupt();
        }
    }

    public void run() {

        logger.debug("\n\nSIMPLE DB\n\n");
        memtableManagerService.submit(manageMemtable(5000));
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
        }while(true);
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

                   logger.debug(String.format("Attempting to obtain read lock: %s", readWriteLock));
                   memtableReadLock.lock();
                   try{
                       if(memTable.isFull()){
                           //lets threads know a new memtable is available.
                           //Needs to be synchronized to avoid
                           synchronized (this) {
                               //No other writes should be allowed to the Memtable now.
                               logger.debug("Memtable full initating dump!!!");
                               this.cacheService.submit(dump(writer, memTable));

                               memTable.dumped();
                               memTable = new DefaultMemtable(memtableWriteLock, writer);

                               logger.debug("NOTIFIYING DUMP COMPLETION");
                               notifyAll();
                               logger.debug("DUMP COMPLETE!");
                           }
                       }
                   }finally{
                       memtableReadLock.unlock();
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
    public Runnable dump(final LogWriter<String> writer, final Memtable<String> memTable){

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
