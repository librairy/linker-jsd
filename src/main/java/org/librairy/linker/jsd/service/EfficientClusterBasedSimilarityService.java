package org.librairy.linker.jsd.service;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.dao.CounterDao;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cache.CacheModeHelper;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.computing.helper.ComputingHelper;
import org.librairy.linker.jsd.data.*;
import org.librairy.linker.jsd.functions.RowToDirichletDistribution;
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

//import org.apache.spark.mllib.clustering.KMeans;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class EfficientClusterBasedSimilarityService {

    private static final Logger LOG = LoggerFactory.getLogger(EfficientClusterBasedSimilarityService.class);

    @Autowired
    Worker worker;

    @Autowired
    ShapeCache shapeCache;

    @Autowired
    ShapeDao shapeDao;

    @Autowired
    CounterDao counterDao;

    @Autowired
    SimilarityDao similarityDao;

    @Autowired
    EventBus eventBus;

    @Autowired
    ComputingHelper computingHelper;

    @Autowired
    CacheModeHelper cacheModeHelper;

    @Value("#{environment['LIBRAIRY_JSD_SCORE_MIN']?:${librairy.jsd.score.min}}")
    Double minScore;

    @Value("#{environment['LIBRAIRY_JSD_SIZE_MIN']?:${librairy.jsd.size.min}}")
    Integer minSize;

    @Value("#{environment['LIBRAIRY_JSD_SIZE_MAX']?:${librairy.jsd.size.max}}")
    Integer maxSize;


    public void handleParallel(String domainUri){
        worker.run(() -> handle(domainUri));
    }

    public void handle(String domainUri){

        try{
            Instant start  = Instant.now();

            LOG.info("Topic Threshold = " + minScore);

            counterDao.reset(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route());

            final ComputingContext context = computingHelper.newContext("jsd.similarity."+ URIGenerator.retrieveId(domainUri));
            final Integer partitions = context.getRecommendedPartitions();

            computingHelper.execute(context, () -> {
                try{

                    //drop similarity tables
                    similarityDao.destroy(domainUri);

                    JavaRDD<DirichletDistribution> vectors = context.getCassandraSQLContext()
                            .read()
                            .format("org.apache.spark.sql.cassandra")
                            .schema(DataTypes
                                    .createStructType(new StructField[]{
                                            DataTypes.createStructField("uri", DataTypes.StringType, false),
                                            DataTypes.createStructField("vector", DataTypes.createArrayType(DataTypes.DoubleType), false)
                                    }))
                            .option("inferSchema", "false") // Automatically infer data types
                            .option("charset", "UTF-8")
//                        .option("spark.sql.autoBroadcastJoinThreshold","-1")
                            .option("mode", "DROPMALFORMED")
                            .options(ImmutableMap.of("table", "shapes", "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                            .load()
                            .repartition(partitions)
                            .toJavaRDD()
                            .map(new RowToDirichletDistribution(domainUri,minScore));

                    LOG.info("Getting shapes from db");

                    vectors.persist(cacheModeHelper.getLevel());

                    vectors.take(1);

                    // recursive clustering
                    clusterByTopicDistribution(vectors, minScore, domainUri, context,0);

                    vectors.unpersist();

                    LOG.info("Similarities created!");
                    eventBus.post(Event.from(domainUri), RoutingKey.of("domain.similarities.created"));

                } catch (Exception e){
                    // TODO Notify to event-bus when source has not been added
                    LOG.error("Error calculating similarities in domain: " + domainUri, e);
                }

                Instant end    = Instant.now();
                LOG.info("Similarities discovered in: "       +
                        ChronoUnit.HOURS.between(start,end) + "h " +
                        ChronoUnit.MINUTES.between(start,end)%60 + "min " +
                        (ChronoUnit.SECONDS.between(start,end)%3600) + "secs");
            });
        } catch (InterruptedException e) {
            LOG.info("Execution interrupted.");
        }

    }

    private void clusterByTopicDistribution (JavaRDD<DirichletDistribution> vectors, Double threshold, String domainUri, ComputingContext context, Integer level ){

        LOG.debug("Clustering by threshold = " + threshold);

        List<String> topicLabels = vectors.map(d -> d.getLabel()).distinct().collect();

        LOG.info("Topic Clusters discovered: " + topicLabels.size());


        for(String topicLabel : topicLabels){
            JavaRDD<DirichletDistribution> shapes = vectors.filter(d -> d.getLabel().equalsIgnoreCase(topicLabel));

            long numItems = shapes.count();
            LOG.debug("Sample size = " + numItems + " [level " + level + "]");

            if (numItems > maxSize){
                Double increment = (1.0-threshold)/10.0;
                final Double newThreshold = threshold+increment;

                clusterByTopicDistribution(shapes.map(s -> new DirichletDistribution(s.getId(), s.getVector(), newThreshold)),newThreshold,domainUri, context, level+1);
            }else if ( (level > 0) && (numItems < minSize) && (StringUtils.contains(topicLabel,"|"))){

                String upperTopicLabel = StringUtils.substringBeforeLast(topicLabel,"|");

                long upperSampleSize = vectors.filter(d -> d.getLabel().startsWith(upperTopicLabel)).count();

                if (upperSampleSize > maxSize){
                    LOG.info("Upper Sample exceeds max size = " + upperSampleSize);
                    saveSimilaritiesBetween(domainUri, context, shapes, numItems);
                }else{
                    LOG.info("Discovering similarities among " + upperSampleSize + "[level "+level+"] grouped by (upper) topic distribution: " + upperTopicLabel +" ...");
                    saveSimilaritiesBetween(domainUri, context, vectors.filter(d -> d.getLabel().startsWith(upperTopicLabel)), upperSampleSize);
                }

            }else{
                LOG.info("Discovering similarities among " + numItems + "[level "+level+"] grouped by topic distribution: " + topicLabel +" ...");
                saveSimilaritiesBetween(domainUri, context, shapes, numItems);

            }

        }

    }

    private void saveSimilaritiesBetween(String domainUri, ComputingContext context, JavaRDD<DirichletDistribution> vectors, long size){

        final Double minValue = minScore;

        JavaRDD<SimilarityRow> simRows = vectors
                .cartesian(vectors)
                .repartition(context.getRecommendedPartitions())
                .filter( p -> !p._1.getId().equals(p._2.getId()))
                .map(pair -> {
                    SimilarityRow row1 = new SimilarityRow();
                    row1.setResource_uri_1(pair._1.getId());
                    row1.setResource_uri_2(pair._2.getId());
                    row1.setScore(JensenShannonSimilarity.apply(ArrayUtils.toPrimitive(pair._1.getVector().toArray(new Double[pair._1.getVector().size()])), ArrayUtils.toPrimitive(pair._2.getVector().toArray(new Double[pair._2.getVector().size()]))));
                    row1.setResource_type_1(URIGenerator.typeFrom(pair._1.getId()).key());
                    row1.setResource_type_2(URIGenerator.typeFrom(pair._2.getId()).key());
                    row1.setDate(TimeUtils.asISO());
                    return row1;

                })
                .filter(r -> r.getScore() > minValue)
//                .persist(helper.getCacheModeHelper().getLevel());
                ;
//        simRows.take(1);


        Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(size).intValue(),Long.valueOf(size-1).intValue());
        counterDao.increment(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route(), combinations);

        LOG.info("saving similarities to db ..");
        context.getSqlContext().createDataFrame(simRows, SimilarityRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", "similarities", "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .mode(SaveMode.Append)
                .save();

    }


}
