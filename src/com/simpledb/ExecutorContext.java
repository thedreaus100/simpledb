package com.simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class ExecutorContext {

    private static ExecutorContext self;
    private final List<ExecutorService> services;

    private ExecutorContext(){

        services = new ArrayList<ExecutorService>();
    }

    public static ExecutorContext getInstance() {
        if (self == null) {
            self = new ExecutorContext();
            return self;
        }else{
            return self;
        }
    }

    public ExecutorContext register(ExecutorService service){

        services.add(service);
        return this;
    }

    public List<ExecutorService> getExecutorServices(){

        return services;
    }
}
