package org.librairy.linker.jsd.tasks;

import com.google.common.base.Strings;
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
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.librairy.computing.cluster.ComputingContext;
import org.librairy.linker.jsd.data.DirichletDistribution;
import org.librairy.linker.jsd.data.SimilarityRow;
import org.librairy.linker.jsd.functions.RowToDirichletDistribution;
import org.librairy.linker.jsd.util.LinkerHelper;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

//import org.apache.spark.mllib.clustering.KMeans;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class SimilarityTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityTask.class);

    private final String domainUri;
    private final LinkerHelper helper;
    private final Double minScore;


    public SimilarityTask(String domainUri, LinkerHelper helper){
        this.domainUri  = domainUri;
        this.helper     = helper;
        this.minScore   = helper.getMinScore();
    }


    @Override
    public void run() {

        try{
            Instant start  = Instant.now();

            final Double minScore = helper.getMinScore();

            LOG.info("Topic Threshold = " + minScore);

            helper.getCounterDao().reset(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route());

            final ComputingContext context = helper.getComputingHelper().newContext("jsd.similarity."+ URIGenerator.retrieveId(domainUri));
            final Integer partitions = context.getRecommendedPartitions();

            helper.getComputingHelper().execute(context, () -> {
                try{

                    //drop similarity tables
                    helper.getSimilarityDao().destroy(domainUri);

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

                    vectors.persist(helper.getCacheModeHelper().getLevel());

                    vectors.take(1);

                    // recursive clustering
                    clusterByTopicDistribution(vectors, minScore, domainUri, context,0);

                    vectors.unpersist();

                    LOG.info("Similarities created!");
                    helper.getEventBus().post(Event.from(domainUri), RoutingKey.of("domain.similarities.created"));

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

    private void clusterByTopicDistribution (JavaRDD<DirichletDistribution> vectors, Double threshold, String domainUri, ComputingContext context, Integer level){

        LOG.debug("Clustering by threshold = " + threshold);

        List<String> topicLabels = vectors.map(d -> d.getLabel()).distinct().collect();

        LOG.info( topicLabels.size() + " clusters discovered by threshold=" + threshold + " in domain: " + domainUri);

        AtomicInteger counter = new AtomicInteger();
        for(String topicLabel : topicLabels){

            if (level == 0) LOG.info(" ##### Iteration "+ counter.get() + " of " + topicLabels.size());
            counter.incrementAndGet();
            JavaRDD<DirichletDistribution> shapes = vectors.filter(d -> d.getLabel().equalsIgnoreCase(topicLabel));

            long numItems = shapes.count();
            LOG.info(numItems + " elements in cluster= " + topicLabel);

            if (numItems > helper.getMaxSize()) {

                int numTopics = StringUtils.countMatches(topicLabel, "|") + 1;

                if (numTopics >= shapes.first().getVector().size()) {
                    LOG.info("Max number of topics reached!!. Getting similarities among " + numItems + " elements in cluster: " + topicLabel);
                    JavaRDD<DirichletDistribution> v1 = context.getSparkContext().parallelize(shapes.take(helper.getMinSize()));
                    new PartialSimilarityTask(domainUri, helper, new TreeSet<>()).saveSimilaritiesAmong(domainUri,context, v1, shapes);
                } else {
                    LOG.info("Recursive iteration adding one more topic label from level: " + level);
                    clusterByTopicDistribution(shapes.map(d -> new DirichletDistribution(d.getId(), d.getVector(), numTopics + 1)), threshold, domainUri, context, level + 1);
                }
            }else if (numItems < helper.getMinSize() && level > 0){
                LOG.info("Min number of elements reached in recursive iteration. Getting similarities from " + numItems + " elements in cluster: " + topicLabel + " to others");
                new PartialSimilarityTask(domainUri, helper, new TreeSet<>()).saveSimilaritiesAmong(domainUri,context, shapes, vectors);

            }else{
                LOG.info("Discovering similarities among " + numItems + " elements grouped by topic distribution: " + topicLabel +" ...");
                saveSimilaritiesBetween(domainUri, context, shapes, numItems);

            }

        }

    }

    private void saveSimilaritiesBetween(String domainUri, ComputingContext context, JavaRDD<DirichletDistribution> vectors, long size){

        final Double minValue = minScore;
        JavaRDD<SimilarityRow> simRows;
        if (size < helper.getMaxSize()){
            simRows = vectors
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

        }else{
            final int sampleSize = helper.getMinSize();
            LOG.info("creating partial similarities..");
            simRows = vectors.flatMap( d1 -> vectors.take(sampleSize).stream().map(d2 -> {
                SimilarityRow row1 = new SimilarityRow();
                row1.setResource_uri_1(d1.getId());
                row1.setResource_uri_2(d2.getId());
                row1.setScore(JensenShannonSimilarity.apply(ArrayUtils.toPrimitive(d1.getVector().toArray(new Double[d1.getVector().size()])), ArrayUtils.toPrimitive(d2.getVector().toArray(new Double[d2.getVector().size()]))));
                row1.setResource_type_1(URIGenerator.typeFrom(d1.getId()).key());
                row1.setResource_type_2(URIGenerator.typeFrom(d2.getId()).key());
                row1.setDate(TimeUtils.asISO());
                return row1;
            }).collect(Collectors.toList()))
                    .filter(r -> r.getScore() > minValue);
        }



        //Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(size).intValue(),Long.valueOf(size-1).intValue());
        Long combinations = simRows.count();
        helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route(), combinations);

        LOG.info("saving similarities to db ..");
        context.getSqlContext().createDataFrame(simRows, SimilarityRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", "similarities", "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .mode(SaveMode.Append)
                .save();

    }

}
