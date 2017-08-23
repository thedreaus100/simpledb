package main.com.simpledb.index.src.main.com.simpledb;

import main.com.simpledb.index.src.main.com.simpledb.memtable.Memtable;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static Memtable<String> memTable;
    private static boolean test = true;

    public static void main(String[] args) {

        ExecutorService service = Executors.newFixedThreadPool(10);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                try{
                    service.shutdown();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }){});

        try{
            DefaultProcessor processor = new DefaultProcessor();
            Thread processorThread = new Thread(processor);

            //Start
            if(test){
                final PipedInputStream pis = new PipedInputStream();
                final PipedOutputStream pos  = new PipedOutputStream();
                pos.connect(pis);

                service.execute(()->{
                    System.setIn(pis);
                    boolean flag = true;
                    int key = 0;

                    try{
                        Thread.sleep(0);
                        while(flag){
                            String entry = String.format("%d, Andreaus \n", ++key);
                            System.out.print(entry);
                            pos.write(entry.getBytes());
                            Thread.sleep(500);
                        }
                    }catch(InterruptedException e){
                        flag = false;
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                });
            }
            service.execute(processorThread);

        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
