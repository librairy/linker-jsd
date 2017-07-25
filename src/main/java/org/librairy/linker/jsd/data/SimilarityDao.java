package org.librairy.linker.jsd.data;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.storage.dao.AbstractDao;
import org.librairy.boot.storage.dao.CounterDao;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class SimilarityDao extends AsyncAbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityDao.class);

    @Autowired
    CounterDao counterDao;

    @Autowired
    DBSessionManager sessionManager;

    public boolean save(Similarity similarity, String domainUri){

        LOG.debug("Saving similarity " + similarity + " in domain '" + domainUri + "'");

        String domainId = URIGenerator.retrieveId(domainUri);
        String keyspace = "lda_" + domainId;

        String query = "insert into "+keyspace+".similarities " +
                "(resource_uri_1, score, resource_uri_2, date, resource_type_1, resource_type_2) " +
                "values (" +
                "'"+similarity.getUri1()+"' ," +
                " "+similarity.getScore()+" ," +
                " '"+similarity.getUri2()+"' ," +
                " '"+similarity.getDate()+"' ," +
                " '"+similarity.getType1()+"' ," +
                " '"+similarity.getType2()+"'" +
                ");";

        try{
            Boolean result = executeAsync(query);
            counterDao.increment(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route());
            return result;
        }catch(Exception e){
            LOG.error("Error saving similarity: " + similarity + " in domain: '" + domainUri + "'", e);
            return false;
        }
    }

    public void destroy(String domainUri){
        LOG.info("dropping existing similarities for domain: " + domainUri);
        String domainId = URIGenerator.retrieveId(domainUri);
        String keyspace = "lda_" + domainId;
        try{
            executeAsync("truncate "+keyspace+".similarities ;");
            executeAsync("truncate "+keyspace+".simcentroids ;");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }

    public List<Similarity> getSimilarResources(String resourceUri, String domainUri, Optional<String> type, Optional<Double> minScore, Optional<Integer> limit, Optional<Double> maxScore){

        StringBuilder queryBuilder = new StringBuilder().append("select ")
                .append("resource_uri_2").append(", ").append("score").append(", ").append("date")
                .append(" from ").append("similarities")
                .append(" where ").append("resource_uri_1").append("='").append(resourceUri).append("' ");



        if (type.isPresent()){
            queryBuilder = queryBuilder.append(" and resource_type_2='").append(type.get()).append("' ");
        }


        if (minScore.isPresent()){
            queryBuilder = queryBuilder.append(" and score >=").append(minScore.get()).append(" ");
        }

        if (maxScore.isPresent()){
            queryBuilder = queryBuilder.append(" and score <").append(maxScore.get()).append(" ");
        }

        if (limit.isPresent()){
            queryBuilder = queryBuilder.append(" limit ").append(limit.get()).append(" ");
        }


        String query = queryBuilder.toString();

        try{
            ResultSet result = sessionManager.getSpecificSession("lda", URIGenerator.retrieveId(domainUri)).execute(query);
            List<Row> rows = result.all();

            if (rows == null || rows.isEmpty()) return Collections.emptyList();

            return rows
                    .stream()
                    .map(row -> {
                        Similarity sim = new Similarity();
                        sim.setUri1(resourceUri);
                        sim.setUri2(row.getString(0));
                        sim.setScore(row.getDouble(1));
                        return sim;
                    })
                    .collect(Collectors.toList());
        }catch (InvalidQueryException e){
            LOG.warn("Query error: " + e.getMessage());
            return Collections.emptyList();
        }catch (Exception e){
            LOG.error("Unexpected error", e);
            return Collections.emptyList();
        }


    }
}
