package org.librairy.linker.jsd.eventbus;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.linker.jsd.Application;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@PropertySource({"classpath:boot.properties"})
public class EventSimulator {

    @Autowired
    EventBus eventBus;

    @Test
    public void similarity() throws InterruptedException {


        Relation relation = new Relation();

        relation.setStartUri("http://minetur.dia.fi.upm.es:9999/api/items/2-s2.0-78649912634"); //resource uri
        relation.setEndUri("http://minetur.dia.fi.upm.es:9999/api/domains/rodf"); // domainUri

        eventBus.post(Event.from(relation), RoutingKey.of("shape.created"));

        Thread.sleep(Long.MAX_VALUE);
    }
}
