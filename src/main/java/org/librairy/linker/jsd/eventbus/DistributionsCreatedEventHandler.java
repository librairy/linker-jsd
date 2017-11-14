package org.librairy.linker.jsd.eventbus;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.linker.jsd.cache.DelayCache;
import org.librairy.linker.jsd.data.ShapeCache;
import org.librairy.linker.jsd.service.SimilarityService;
import org.librairy.linker.jsd.tasks.SimilarityTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
@DependsOn("dbChecker")
public class DistributionsCreatedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionsCreatedEventHandler.class);

    @Autowired
    SimilarityService similarityService;

    @Autowired
    DelayCache delayCache;

    @Autowired
    protected EventBus eventBus;

    @PostConstruct
    public void init(){
        BindingKey bindingKey = BindingKey.of(RoutingKey.of("lda.distributions.created"), "linker.jsd.distributions.created");
        LOG.info("Trying to register as subscriber of '" + bindingKey + "' events ..");
        eventBus.subscribe(this,bindingKey );
        LOG.info("registered successfully");
    }


    @Override
    public void handle(Event event) {
        LOG.info("lda distributions created event received: " + event);
        try{
            String domainUri = event.to(String.class);

            Long delay = delayCache.getDelay(domainUri);

            LOG.info("Waiting for domain delay: " + delay + " ..");
            Thread.sleep(delay);

            similarityService.process(domainUri);

        } catch (Exception e){
            // TODO Notify to event-bus when source has not been added
            LOG.error("Error scheduling annotations in domain: " + event, e);
        }
    }
}