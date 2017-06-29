package org.librairy.linker.jsd.data;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.librairy.boot.model.domain.resources.Domain;
import org.librairy.boot.model.domain.resources.Resource;
import org.librairy.boot.storage.dao.AbstractDao;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ShapeDao extends AbstractDao{

    private static final Logger LOG = LoggerFactory.getLogger(ShapeDao.class);

    @Autowired
    private DBSessionManager sessionManager;


    public Optional<Shape> get(String domainUri, String resourceUri){

        String domainId = URIGenerator.retrieveId(domainUri);
        String keyspace = "lda_" + domainId;

        StringBuilder query = new StringBuilder()
                .append("select id, vector from ")
                .append(keyspace)
                .append(".shapes where uri='" + resourceUri+"' ;");

        try{
            Optional<Row> result = oneQuery(query.toString());
            if (!result.isPresent()) return Optional.empty();

            Shape shape = new Shape();
            shape.setUri(resourceUri);
            shape.setId(result.get().getLong(0));
            shape.setVector(result.get().getList(1, Double.class));
            return Optional.of(shape);
        }catch (Exception e){
            e.printStackTrace();
            return Optional.empty();
        }


    }

    public List<Shape> get(String domainUri, Integer size, Optional<Long> offset){

        String domainId = URIGenerator.retrieveId(domainUri);
        String keyspace = "lda_" + domainId;

        StringBuilder query = new StringBuilder()
                .append("select id, uri, vector from ")
                .append(keyspace)
                .append(".shapes ");

        if (offset.isPresent()){
            query.append(" where token(id) > token(" + offset.get() + ")");
        }

        query.append(" limit " + size);

        query.append(";");

        Iterator<Row> it = super.iteratedQuery(query.toString());
        List<Shape> shapes = new ArrayList<>();

        while(it.hasNext()){
            Row row = it.next();
            Shape shape = new Shape();
            shape.setId(row.getLong(0));
            shape.setUri(row.getString(1));
            shape.setVector(row.getList(2, Double.class));
            shapes.add(shape);
        }

        return shapes;
    }

}
