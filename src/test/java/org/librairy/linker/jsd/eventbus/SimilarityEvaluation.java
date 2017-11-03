package org.librairy.linker.jsd.eventbus;

import com.google.common.primitives.Doubles;
import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.storage.dao.DomainsDao;
import org.librairy.linker.jsd.Application;
import org.librairy.linker.jsd.data.*;
import org.librairy.linker.jsd.service.RecursiveKMeansSimilarityService;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest
@Import(Application.class)
public class SimilarityEvaluation {

    @Autowired
    SimilarityDao similarityDao;

    @Autowired
    ShapeDao shapesDao;

    @Autowired
    DomainsDao domainsDao;

    @Autowired
    RecursiveKMeansSimilarityService service;

    String domainUri = "http://librairy.linkeddata.es/resources/domains/blueBottle";

    @Test
    public void evaluateSimilarities(){

        Integer numItems = 100 ;
        Optional<Double> minScore = Optional.of(0.9);
        Optional<Integer> maxNumber = Optional.of(3);

        List<Double> intersections = new ArrayList<>();


        List<String> uris = new ArrayList<>();

        uris.addAll(domainsDao.listItems(domainUri,numItems, Optional.empty(), false).stream().map(item -> item.getUri()).collect(Collectors.toList()));
//        uris.addAll(domainsDao.listParts(domainUri,numItems, Optional.empty(), false).stream().map(item -> item.getUri()).collect(Collectors.toList()));

        uris.forEach(uri ->{

            Optional<Shape> shape = shapesDao.get(domainUri, uri);

            // From DDBB
            List<Similarity> similarItems = similarityDao.getSimilarResources(uri, domainUri, Optional.empty(),minScore,maxNumber,Optional.empty());
//            List<Similarity> similarItems = calculateSimilarities(domainUri, item.getUri(), Doubles.toArray(shape.get().getVector()), minScore.get()).stream().limit(maxNumber.get()).collect(Collectors.toList());

            // From Comparison
            List<Similarity> calculatedSimilarItems = calculateSimilarities(domainUri, uri, Doubles.toArray(shape.get().getVector()), minScore.get()).stream().limit(maxNumber.get()).collect(Collectors.toList());

            // Count intersections
            if (similarItems.isEmpty()){
                System.out.println("Similar Items are empty!!!");
                if (calculatedSimilarItems.isEmpty()){
                    intersections.add(100.0);
                }else{
                    intersections.add(0.0);
                }
            }else{
                Double match = Double.valueOf(calculatedSimilarItems.stream().map(sim -> (similarItems.contains(sim) ? 1 : 0)).reduce((a, b) -> a + b).get());
                Double ratio = (match*100)/Double.valueOf(calculatedSimilarItems.size());

                if (ratio != 100.0){
                    System.out.println("ratio lower: " + ratio + "[" + similarItems.size() + "/"+calculatedSimilarItems.size()+"]");
                }
                intersections.add(ratio);
            }

            }
        );

        DoubleSummaryStatistics stats = intersections.stream().collect(DoubleSummaryStatistics::new, DoubleSummaryStatistics::accept, DoubleSummaryStatistics::combine);
        System.out.println("Stats: " + stats);
//        System.out.println("Ratio: " + ((stats.getAverage()*100.0)/maxNumber.get())+"%");
    }


    private List<Similarity> calculateSimilarities(String domainUri, String resourceUri, double[] refVector, Double minScore){
        AtomicInteger counter = new AtomicInteger();
        QueryKey query = new QueryKey();
        query.setDomainUri(domainUri);
        query.setSize(2500);
        query.setOffset(Optional.empty());
        List<Similarity> similarItems = new ArrayList<>();
        while(true){

            List<Shape> shapes = shapesDao.get(query.getDomainUri(), query.getSize(), query.getOffset());

            shapes.stream().forEach( shape -> {

                if (shape.getUri().equalsIgnoreCase(resourceUri)) return;

                double score = JensenShannonSimilarity.apply(refVector, Doubles.toArray(shape.getVector()));

                if (score >= minScore){
                    Similarity similarity = new Similarity();
                    similarity.setScore(score);
                    similarity.setUri1(resourceUri);
                    similarity.setUri2(shape.getUri());

                    similarItems.add(similarity);
                    counter.incrementAndGet();
                }
            });


            if (shapes.size() < query.getSize()) break;

            query.setOffset(Optional.of(shapes.get(query.getSize()-1).getId()));
        }

        return similarItems.stream().sorted((a,b) -> -a.getScore().compareTo(b.getScore())).collect(Collectors.toList());

    }


    @Test
    public void updateSimilarities(){

        service.handle(domainUri);

    }


    @Test
    public void calculateSimilarityBetween(){

        String uri1 = "http://librairy.linkeddata.es/resources/items/2-s2.0-71649099071";
        String uri2 = "http://librairy.linkeddata.es/resources/parts/35728b530c8893f4bf98e6e5d30439fb";


        Optional<Shape> shape1 = shapesDao.get(domainUri, uri1);
        Optional<Shape> shape2 = shapesDao.get(domainUri, uri2);


        Doubles.toArray(shape1.get().getVector());
        double sim = JensenShannonSimilarity.apply(Doubles.toArray(shape1.get().getVector()), Doubles.toArray(shape2.get().getVector()));

        System.out.println(sim);

    }
}
