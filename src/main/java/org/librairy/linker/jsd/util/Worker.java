package org.librairy.linker.jsd.util;

import org.librairy.boot.storage.executor.ParallelExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class Worker {

    private ParallelExecutor executor;

    @PostConstruct
    public void setup(){
        this.executor = new ParallelExecutor();
    }

    public void run(Runnable task){
        this.executor.execute(task);
    }
}
