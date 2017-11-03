package org.librairy.linker.jsd.service;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.linker.jsd.Application;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;


/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest
@Import(Application.class)
public class EfficientClusterBasedSimilarityServiceTest {


    @Autowired
    EfficientClusterBasedSimilarityService service;


    @Test
    @Ignore
    public void calculateSimilarities(){


        String domainUri = "http://librairy.linkeddata.es/resources/domains/aies6";

        service.handle(domainUri);

    }

}
