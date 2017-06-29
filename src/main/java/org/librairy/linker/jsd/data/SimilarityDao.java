package org.librairy.linker.jsd.data;

import com.datastax.driver.core.ResultSet;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.storage.dao.AbstractDao;
import org.librairy.boot.storage.dao.CounterDao;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class SimilarityDao extends AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityDao.class);

    @Autowired
    CounterDao counterDao;

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
            Boolean result = execute(query);
            counterDao.increment(domainUri, Relation.Type.SIMILAR_TO_ITEMS.route());
            return result;
        }catch(Exception e){
            LOG.error("Error saving similarity: " + similarity + " in domain: '" + domainUri + "'", e);
            return false;
        }
    }
}
