package org.librairy.linker.jsd.dao;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.librairy.boot.storage.dao.DBSessionManager;
import org.librairy.boot.storage.generator.URIGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */

public class LabeledTopicDistributions {

    private static final Logger LOG = LoggerFactory.getLogger(LabeledTopicDistributions.class);

    @Autowired
    protected DBSessionManager sessionManager;


    @PostConstruct
    public void setup(String domainUri){
        LOG.info("creating LDA comparisons table for domain: " + domainUri);

        try{
            getSession(domainUri).execute("create table if not exists shapes(" +
                    "domain text, " +
                    "label text, " +
                    "uri text, " +
                    "shape list<double>,"+
                    "primary key (domain, label, uri));");
        }catch (InvalidQueryException e){
            LOG.warn(e.getMessage());
        }
    }


    private Session getSession(String domainUri){
        return sessionManager.getSpecificSession("linker", URIGenerator.retrieveId(domainUri).toLowerCase());
    }

}
