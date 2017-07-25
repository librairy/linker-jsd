package org.librairy.linker.jsd.service;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.JSKMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
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
import org.librairy.linker.jsd.data.ShapeCache;
import org.librairy.linker.jsd.data.ShapeDao;
import org.librairy.linker.jsd.data.SimilarityDao;
import org.librairy.linker.jsd.data.SimilarityRow;
import org.librairy.linker.jsd.functions.RowToTupleVector;
import org.librairy.linker.jsd.util.Worker;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class RecursiveKMeansSimilarityService {

    private static final Logger LOG = LoggerFactory.getLogger(RecursiveKMeansSimilarityService.class);

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

    public void handleParallel(String domainUri){
        worker.run(() -> handle(domainUri));
    }

    public void handle(String domainUri){

        try{

            counterDao.reset(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route());

            final ComputingContext context = computingHelper.newContext("jsd.similarity."+ URIGenerator.retrieveId(domainUri));
            final Integer partitions = context.getRecommendedPartitions();

            computingHelper.execute(context, () -> {
                try{

                    //drop similarity tables
                    similarityDao.destroy(domainUri);

                    final long vectorDim = counterDao.getValue(domainUri,"topics");

                    JavaRDD<Tuple2<String,Vector>> vectors = context.getCassandraSQLContext()
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
                            .map(new RowToTupleVector())
                            .filter(el -> el._2.size() == vectorDim);

                    LOG.info("Getting shapes from db");

                    AtomicInteger counter = new AtomicInteger();

                    Integer maxPointsPerCluster = 1000; //1000
                    Integer maxIterations = 50;
                    Double epsilon = 0.00001;
                    similaritiesFrom(domainUri, context, vectors, maxPointsPerCluster, maxIterations, epsilon, counter);

                    LOG.info("Similarities calculated!");

                    eventBus.post(Event.from(domainUri), RoutingKey.of("domain.similarities.created"));

                } catch (Exception e){
                    // TODO Notify to event-bus when source has not been added
                    LOG.error("Error calculating similarities in domain: " + domainUri, e);
                }
            });
        } catch (InterruptedException e) {
            LOG.info("Execution interrupted.");
        }

    }


    private void similaritiesFrom(String domainUri, ComputingContext context, JavaRDD<Tuple2<String,Vector>> documents, int maxSize, int maxIterations, double epsilon, AtomicInteger counter){

        documents.persist(cacheModeHelper.getLevel());

        documents.take(1);

        long size = documents.count();

        if (size < maxSize){
            if (size == 0) return;
            saveSimilaritiesBetween(domainUri, context, documents);
            // Increment counter
            Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(size).intValue(),Long.valueOf(size-1).intValue());
            counterDao.increment(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route(), combinations);
            return;
        }

        int k = Double.valueOf(Math.ceil(Long.valueOf(size).doubleValue() / Integer.valueOf(maxSize).doubleValue())).intValue();

//        if (k == 1){
//            k = 2;
//        }

        LOG.info("Creating " + k + " clusters from " + size + " points constrained by JSD-KMeans [" + "maxSize=" + maxSize +"]");

        JSKMeans kmeans = new JSKMeans()
                .setK(k)
                .setMaxIterations(maxIterations)
                .setEpsilon(1e-12)
                .setInitializationMode("random")
                .setRuns(5)
                ;


        // Get vector based on topic distributions
        RDD<Vector> points = documents.map(el -> el._2).rdd().cache();
        points.take(1);

        // Train k-means model
        KMeansModel model = kmeans.run(points);

        // Clusterize documents
        JavaPairRDD<Long, Tuple2<String, Vector>> indexedDocs = documents
                .zipWithIndex()
                .mapToPair(tuple -> new Tuple2<Long, Tuple2<String, Vector>>(tuple._2, tuple._1));

        JavaPairRDD<Integer, Tuple2<String, Vector>> clusterizedDocs = model.predict(points)
                .toJavaRDD()
                .zipWithIndex()
                .repartition(context.getRecommendedPartitions())
                .mapToPair(tuple -> new Tuple2<Long, Integer>(tuple._2, (Integer) tuple._1))
                .join(indexedDocs, context.getRecommendedPartitions())
                .mapToPair(el -> el._2)
                .persist(cacheModeHelper.getLevel());
        clusterizedDocs.take(1);

        points.unpersist(false);

        Vector[] centroids = model.clusterCenters();

        for (int cid = 0; cid < centroids.length ; cid ++){

            // points in cluster
            final int centroidId = cid;
            JavaRDD<Tuple2<String, Vector>> clusterPoints = clusterizedDocs
                    .filter(t1 -> (t1._1 == centroidId))
                    .map(t2 -> t2._2)
                    .persist(cacheModeHelper.getLevel());

            clusterPoints.take(1);

            long numPoints = clusterPoints.count();

            int index = counter.incrementAndGet();


            if (numPoints <= maxSize){
                Instant startProc  = Instant.now();
                saveSimilaritiesBetween(domainUri, context, clusterPoints);
                Instant endProc    = Instant.now();

                // Increment counter
                Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(numPoints).intValue(),Long.valueOf(numPoints-1).intValue());
                counterDao.increment(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route(), combinations);
                LOG.info(combinations + " similarities calculated and saved from " + numPoints + " documents in " + ChronoUnit.MINUTES.between(startProc,endProc) + "min " + (ChronoUnit.SECONDS.between(startProc,endProc)%60) + "secs");

                clusterPoints.unpersist();

            } else{
                counter.decrementAndGet();
                clusterPoints.unpersist();
                similaritiesFrom(domainUri, context, clusterPoints,maxSize, maxIterations, epsilon, counter);
            }

        }

        clusterizedDocs.unpersist();

        documents.unpersist();

    }

    private void saveSimilaritiesBetween(String domainUri, ComputingContext context, JavaRDD<Tuple2<String,Vector>> vectors){
        JavaRDD<SimilarityRow> simRows = vectors
                .cartesian(vectors)
                .repartition(context.getRecommendedPartitions())
                .filter( p -> !p._1._1.equals(p._2._1))
                .map(pair -> {
                    SimilarityRow row1 = new SimilarityRow();
                    row1.setResource_uri_1(pair._1._1);
                    row1.setResource_uri_2(pair._2._1);
                    row1.setScore(JensenShannonSimilarity.apply(pair._1._2.toArray(), pair._2._2.toArray()));
                    row1.setResource_type_1(URIGenerator.typeFrom(pair._1._1).key());
                    row1.setResource_type_2(URIGenerator.typeFrom(pair._2._1).key());
                    row1.setDate(TimeUtils.asISO());
                    return row1;

                })
//                .persist(helper.getCacheModeHelper().getLevel());
                ;
//        simRows.take(1);

        LOG.info("saving similarities to db ..");
        context.getSqlContext().createDataFrame(simRows, SimilarityRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", "similarities", "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .mode(SaveMode.Append)
                .save();

    }


}
