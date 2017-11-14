package org.librairy.linker.jsd.service;

import org.librairy.linker.jsd.tasks.PartialSimilarityTask;
import org.librairy.linker.jsd.tasks.SimilarityTask;
import org.librairy.linker.jsd.util.LinkerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class SimilarityService {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityService.class);

    @Autowired
    LinkerHelper helper;

    private ConcurrentHashMap<String,ScheduledFuture<?>> tasksByDomain;
    private ConcurrentHashMap<String,Runnable> tasks;

    private ThreadPoolTaskScheduler threadpool;

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");

    private ConcurrentHashMap<String, Set<String>> pendingSimilarities;

    @Value("#{environment['LIBRAIRY_JSD_MAX_PENDING_SIMILARITIES']?:${librairy.jsd.max.pending.similarities}}")
    Integer maxPendingSimilarities;

    @PostConstruct
    public void setup(){
        this.tasksByDomain  = new ConcurrentHashMap<>();
        this.tasks          = new ConcurrentHashMap<>();

        this.threadpool = new ThreadPoolTaskScheduler();
        this.threadpool.setPoolSize(20);
        this.threadpool.initialize();
        this.threadpool.getScheduledThreadPoolExecutor().setRemoveOnCancelPolicy(true);

        this.pendingSimilarities = new ConcurrentHashMap<String, Set<String>>();

        LOG.info("Partial Similarity Service initialized");

    }

    public boolean process(String domainUri, String resourceUri, long delay){

        LOG.info("Scheduled similarity for " + resourceUri + "  in domain: " + domainUri +" at " + timeFormatter.format(new Date(System.currentTimeMillis() + delay)));
        ScheduledFuture<?> task = tasksByDomain.get(domainUri);
        if (task != null) {
            task.cancel(true);
        }
        if (!pendingSimilarities.containsKey(domainUri)){
            pendingSimilarities.put(domainUri, new TreeSet<>());
        }
        pendingSimilarities.get(domainUri).add(resourceUri);

        if (pendingSimilarities.get(domainUri).size() >= maxPendingSimilarities){
            // execute partial shaping tasks
            LOG.info("Max pending similarities reached ( "+ maxPendingSimilarities+ "). Executing partial similarity task ..");
            Set<String> uris = pendingSimilarities.get(domainUri);
            pendingSimilarities.put(domainUri, new TreeSet<>());
            new PartialSimilarityTask(domainUri, helper, pendingSimilarities.get(domainUri)).run();
        }else{
            task = this.threadpool.schedule(new PartialSimilarityTask(domainUri, helper, pendingSimilarities.get(domainUri)), new Date(System.currentTimeMillis() + delay));
            tasksByDomain.put(domainUri,task);
        }

        return true;
    }

    public boolean process(String domainUri){

        LOG.info("Starting a similarity discover process for domain: " + domainUri + " ...");

        new SimilarityTask(domainUri, helper).run();

        return true;
    }

}

