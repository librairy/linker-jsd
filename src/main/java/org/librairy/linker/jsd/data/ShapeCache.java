package org.librairy.linker.jsd.data;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Component
public class ShapeCache {

    private static final Logger LOG = LoggerFactory.getLogger(ShapeCache.class);

    private LoadingCache<QueryKey, List<Shape>> cache;

    @Autowired
    ShapeDao shapeDao;

    @PostConstruct
    public void setup(){
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(60, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<QueryKey, List<Shape>>() {
                            public List<Shape> load(QueryKey query) {
                                return shapeDao.get(query.domainUri, query.size, query.offset);
                            }
                        });
    }


    public List<Shape> get(QueryKey query)  {
        try {
            return this.cache.get(query);
        } catch (ExecutionException e) {
            LOG.error("error getting value from database, using default", e);
            return Collections.emptyList();
        }
    }

    public void refresh(){
        this.cache.cleanUp();
    }

}
