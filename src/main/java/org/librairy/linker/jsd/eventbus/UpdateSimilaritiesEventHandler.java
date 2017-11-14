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
public class UpdateSimilaritiesEventHandler implements EventBusSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateSimilaritiesEventHandler.class);

    @Autowired
    NaiveSimilarityService naiveSimilarityService;

    @Autowired
    protected EventBus eventBus;

    @PostConstruct
    public void init(){
        RoutingKey routingKey = RoutingKey.of("domain.update.relations.similarity");
        LOG.info("Trying to register as subscriber of '" + routingKey + "' events ..");
        eventBus.subscribe(this, BindingKey.of(routingKey, "linker.jsd.similarity.requested"));
        LOG.info("registered successfully");
    }


    @Override
    public void handle(Event event) {
        LOG.debug("event received: " + event);
        try{

            Relation relation = event.to(Relation.class);
            String domainUri    = relation.getStartUri();
            String resourceUri  = relation.getEndUri();
            Double minScore     = relation.getWeight();


            naiveSimilarityService.handle(resourceUri, domainUri, minScore);

            LOG.debug("ACK sent!! [" + resourceUri+"]");
        } catch (RuntimeException e){
            LOG.warn(e.getMessage());
        }catch (Exception e){
            LOG.error("Error adding new source: " + event, e);
        }
    }
}