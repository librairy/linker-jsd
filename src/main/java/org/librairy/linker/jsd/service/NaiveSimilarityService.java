package org.librairy.linker.jsd.service;

import com.google.common.primitives.Doubles;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.linker.jsd.data.*;
import org.librairy.linker.jsd.util.Worker;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
public class NaiveSimilarityService {

    private static final Logger LOG = LoggerFactory.getLogger(NaiveSimilarityService.class);

    @Autowired
    Worker worker;

    @Autowired
    ShapeCache shapeCache;

    @Autowired
    ShapeDao shapeDao;

    @Autowired
    SimilarityDao similarityDao;

    @Autowired
    EventBus eventBus;

    @Value("#{environment['LIBRAIRY_JSD_SCORE_MIN']?:${librairy.jsd.score.min}}")
    Double minScore;

    public void handleParallel(String resourceUri, String domainUri){
        worker.run(() -> handle(resourceUri, domainUri));
    }

    public void handle(String resourceUri, String domainUri){

        // Get reference shape
        LOG.info("Calculating similarities for '" + resourceUri + "' in '" + domainUri + "' ...");

        Instant start = Instant.now();
        Optional<Shape> refShape = shapeDao.get(domainUri, resourceUri);

        if (!refShape.isPresent()){
            LOG.warn("No shape found by resource '" + resourceUri+"' in domain '" + domainUri +"'");
            return;
        }

        // Similarity between resourceUri and other resources from domainUri

        double[] refVector = Doubles.toArray(refShape.get().getVector());


        AtomicInteger counter = new AtomicInteger();
        QueryKey query = new QueryKey();
        query.setDomainUri(domainUri);
        query.setSize(2500);
        query.setOffset(Optional.empty());
        while(true){

            List<Shape> shapes = shapeCache.get(query);

            shapes.stream().forEach( shape -> {

                if (shape.getUri().equalsIgnoreCase(resourceUri)) return;

                double score = JensenShannonSimilarity.apply(refVector, Doubles.toArray(shape.getVector()));

                if (score >= minScore){
                    Similarity similarity = new Similarity();
                    similarity.setScore(score);
                    similarity.setUri1(resourceUri);
                    similarity.setUri2(shape.getUri());

                    similarityDao.save(similarity, domainUri);
                    counter.incrementAndGet();
                }
            });


            if (shapes.size() < query.getSize()) break;

            query.setOffset(Optional.of(shapes.get(query.getSize()-1).getId()));

        }

        Instant end = Instant.now();

        LOG.info("Saved " + counter.get() + " similarity relations for '" + resourceUri + "' in '" + domainUri +"'   - " + ChronoUnit.MINUTES.between(start,end) + "min " + ChronoUnit.SECONDS.between(start,end)%60+ "secs");

        eventBus.post(Event.from(resourceUri), RoutingKey.of("similarities.created"));

    }

}