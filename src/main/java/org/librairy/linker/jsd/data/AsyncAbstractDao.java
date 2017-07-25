package org.librairy.linker.jsd.data;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.librairy.boot.storage.dao.AbstractDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class AsyncAbstractDao extends AbstractDao {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncAbstractDao.class);

    private ConcurrentLinkedQueue<ResultSetFuture> tasks = new ConcurrentLinkedQueue<ResultSetFuture>();

    protected Boolean executeAsync(String query){


//        if (tasks.size() != 0) {
//            for (ResultSetFuture t:tasks)
//                t.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
//        }


        LOG.debug("Executing query: " + query );
        try{
//            ResultSetFuture result = dbSessionManager.getCommonSession().executeAsync(query.toString());
//            return result.isDone();
            ResultSetFuture future = dbSessionManager.getCommonSession().executeAsync(query);
            tasks.add(future);
            if (tasks.size() < 8000)
                return true;
            for (ResultSetFuture t:tasks)
                t.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
            tasks.clear();
            return true;
        }catch (InvalidQueryException e){
            LOG.warn("Error on query execution [" + query + "]: " + e.getMessage());
            return false;
        } catch (TimeoutException e) {
            LOG.warn("Timeout Error on query execution [" + query + "]: " + e.getMessage());
            return false;
        }
    }
}
