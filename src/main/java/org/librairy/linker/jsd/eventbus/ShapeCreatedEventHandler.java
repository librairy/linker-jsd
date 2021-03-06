package org.librairy.linker.jsd.eventbus;

import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.EventBusSubscriber;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.linker.jsd.cache.DelayCache;
import org.librairy.linker.jsd.data.ShapeCache;
import org.librairy.linker.jsd.service.NaiveSimilarityService;
import org.librairy.linker.jsd.service.SimilarityService;
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
public class ShapeCreatedEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(ShapeCreatedEventHandler.class);

    @Autowired
    SimilarityService similarityService;

    @Autowired
    ShapeCache cache;

    @Autowired
    protected EventBus eventBus;

    @Autowired
    DelayCache delayCache;

    @PostConstruct
    public void init(){
        RoutingKey routingKey = RoutingKey.of("shape.created");
        LOG.info("Trying to register as subscriber of '" + routingKey + "' events ..");
        eventBus.subscribe(this, BindingKey.of(routingKey, "linker.jsd.shape.created"));
        LOG.info("registered successfully");
    }


    @Override
    public void handle(Event event) {
        LOG.debug("event received: " + event);
        try{
            cache.refresh();
            Relation relation = event.to(Relation.class);
            String resourceUri  = relation.getStartUri();
            String domainUri    = relation.getEndUri();

            Long delay = delayCache.getDelay(domainUri);

            similarityService.process(domainUri, resourceUri, delay);

            LOG.debug("ACK sent!! [" + resourceUri+"]");
        } catch (RuntimeException e){
            LOG.warn(e.getMessage());
        }catch (Exception e){
            LOG.error("Error adding new source: " + event, e);
        }
    }
}