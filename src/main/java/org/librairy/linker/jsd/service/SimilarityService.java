package org.librairy.linker.jsd.service;

import com.google.common.primitives.Doubles;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.linker.jsd.data.Shape;
import org.librairy.linker.jsd.data.ShapeDao;
import org.librairy.linker.jsd.data.Similarity;
import org.librairy.linker.jsd.data.SimilarityDao;
import org.librairy.linker.jsd.util.Worker;
import org.librairy.metrics.distance.JensenShannonDivergence;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class SimilarityService {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityService.class);

    @Autowired
    Worker worker;

    @Autowired
    ShapeDao shapeDao;

    @Autowired
    SimilarityDao similarityDao;

    @Autowired
    EventBus eventBus;

    public void handleParallel(String resourceUri, String domainUri){
        worker.run(() -> handle(resourceUri, domainUri));
    }

    public void handle(String resourceUri, String domainUri){

        // Get reference shape

        Instant start = Instant.now();
        Optional<Shape> refShape = shapeDao.get(domainUri, resourceUri);

        if (!refShape.isPresent()){
            LOG.warn("No shape found by resource '" + resourceUri+"' in domain '" + domainUri +"'");
            return;
        }

        // Similarity between resourceUri and other resources from domainUri

        double[] refVector = Doubles.toArray(refShape.get().getVector());

        Integer size = 100;
        Optional<Long> offset = Optional.empty();

        AtomicInteger counter = new AtomicInteger();
        while(true){

            List<Shape> shapes = shapeDao.get(domainUri, size, offset);

            shapes.parallelStream().forEach( shape -> {

                if (shape.getUri().equalsIgnoreCase(resourceUri)) return;

                double score = JensenShannonSimilarity.apply(refVector, Doubles.toArray(shape.getVector()));

                if (score >= 0.5){
                    Similarity similarity = new Similarity();
                    similarity.setScore(score);
                    similarity.setUri1(resourceUri);
                    similarity.setUri2(shape.getUri());

                    similarityDao.save(similarity, domainUri);
                    counter.incrementAndGet();
                }
            });


            if (shapes.size() < size) break;

            offset = Optional.of(shapes.get(size-1).getId());

        }

        Instant end = Instant.now();

        LOG.info("Saved " + counter.get() + " similarity relations for '" + resourceUri + "' in '" + domainUri +"'   - " + ChronoUnit.MINUTES.between(start,end) + "min " + ChronoUnit.SECONDS.between(start,end)%60+ "secs");

        eventBus.post(Event.from(resourceUri), RoutingKey.of("similarities.created"));

    }

}
