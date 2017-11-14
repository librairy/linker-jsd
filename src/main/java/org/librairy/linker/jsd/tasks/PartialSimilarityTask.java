package org.librairy.linker.jsd.tasks;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.ArrayUtils;
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
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class PartialSimilarityTask implements Runnable{


    private static final Logger LOG = LoggerFactory.getLogger(PartialSimilarityTask.class);

    private final String domainUri;
    private final LinkerHelper helper;
    private final Double minScore;
    private final Set<String> uris;


    public PartialSimilarityTask(String domainUri, LinkerHelper helper, Set<String> uris){
        this.domainUri  = domainUri;
        this.helper     = helper;
        this.uris       = uris;
        this.minScore   = helper.getMinScore();
    }

    @Override
    public void run() {
        try{
            Instant start  = Instant.now();

            LOG.info("Topic Threshold = " + minScore);


            final ComputingContext context = helper.getComputingHelper().newContext("jsd.partial.similarity."+ URIGenerator.retrieveId(domainUri));
            final Integer partitions = context.getRecommendedPartitions();

            helper.getComputingHelper().execute(context, () -> {
                try{

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

                    // Group by cluster in reference
                    Map<String, List<DirichletDistribution>> distributions = readElements(domainUri, uris);

                    for(String cluster : distributions.keySet()){

                        JavaRDD<DirichletDistribution> v1 = context.getSparkContext().parallelize(distributions.get(cluster));
                        JavaRDD<DirichletDistribution> v2 = vectors.filter(d -> d.getLabel().equalsIgnoreCase(cluster));
                        saveSimilaritiesAmong(domainUri, context, v1, v2);

                    }

                    vectors.unpersist();

                    LOG.info("Partial Similarities created!");
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

    private Map<String, List<DirichletDistribution>> readElements(String domainUri, Set<String> uris){

        Session session = helper.getDbSessionManager().getSpecificSession("lda",URIGenerator.retrieveId(domainUri));

        PreparedStatement statement = session.prepare("select uri, vector from shapes where uri = ? ");
        List<ResultSetFuture> futures = new ArrayList<>();
        List<com.datastax.driver.core.Row> results = new ArrayList<>();
        Iterator<String> iterator = uris.iterator();
        int index = 0;
        while(iterator.hasNext()){
            String uri = iterator.next();
            ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(uri));
            futures.add(resultSetFuture);
            if (index%100 == 0){
                for (ResultSetFuture future : futures){
                    ResultSet rows = future.getUninterruptibly();
                    com.datastax.driver.core.Row row = rows.one();
                    results.add(row);
                }
                futures.clear();
            }
            index += 1;
        }

//        for (String uri: uris) {
//            Resource.Type type = URIGenerator.typeFrom(uri);
//            ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(type.key(), uri));
//            futures.add(resultSetFuture);
//        }

        for (ResultSetFuture future : futures){
            ResultSet rows = future.getUninterruptibly();
            com.datastax.driver.core.Row row = rows.one();

            results.add(row);
        }
        LOG.info(results.size() + " elements read from shapes");

        return results.parallelStream().map(r -> new DirichletDistribution(r.getString(0), r.getList(1, Double.class), minScore)).collect(Collectors.groupingBy(DirichletDistribution::getLabel));

    }

    public void saveSimilaritiesAmong(String domainUri, ComputingContext context, JavaRDD<DirichletDistribution> v1, JavaRDD<DirichletDistribution> v2){
        final Double minValue = minScore;

        JavaRDD<SimilarityRow> simRows = v1
                .cartesian(v2)
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


        //long size = v1.count() + v2.count();
        //Long combinations = CombinatoricsUtils.stirlingS2(Long.valueOf(size).intValue(),Long.valueOf(size-1).intValue());
        Long combinations = simRows.count();

        LOG.info("saving "+combinations+" similarities to db ..");
        context.getSqlContext().createDataFrame(simRows, SimilarityRow.class)
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", "similarities", "keyspace", DBSessionManager.getSpecificKeyspaceId("lda",URIGenerator.retrieveId(domainUri))))
                .mode(SaveMode.Append)
                .save();

        helper.getCounterDao().increment(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route(), combinations);
    }

}
