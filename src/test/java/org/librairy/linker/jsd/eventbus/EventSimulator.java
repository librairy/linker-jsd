package org.librairy.linker.jsd.eventbus;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.boot.model.Event;
import org.librairy.boot.model.domain.relations.Relation;
import org.librairy.boot.model.modules.BindingKey;
import org.librairy.boot.model.modules.EventBus;
import org.librairy.boot.model.modules.RoutingKey;
import org.librairy.linker.jsd.Application;
import org.librairy.metrics.similarity.JensenShannonSimilarity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest
@Import(Application.class)
public class EventSimulator {

    @Autowired
    EventBus eventBus;

    @Test
    public void similarity() throws InterruptedException {


        Relation relation = new Relation();

        List<String> items = Arrays.asList(new String[]{
                "M57xA96F4Ix",
                "8M7y-P3tR0UZL",
                "ww_16zMtRVs8L",
                "yw-16zda6VaLK"
        });

        for(String item : items){
            relation.setStartUri("http://librairy.linkeddata.es/resources/items/"+item); //resource uri
            relation.setEndUri("http://librairy.linkeddata.es/resources/domains/blueBottle"); // domainUri

            eventBus.post(Event.from(relation), RoutingKey.of("shape.created"));

            System.out.println("Event published: " + relation);

        }

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void domainSimilarity() throws InterruptedException {

        String domainUri = "http://librairy.linkeddata.es/resources/domains/blueBottle";

        eventBus.post(Event.from(domainUri), RoutingKey.of("lda.distributions.created"));
        System.out.println("Event published!");

        Thread.sleep(Long.MAX_VALUE);

    }

}
