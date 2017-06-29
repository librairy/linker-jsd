package org.librairy.linker.jsd.data;

import lombok.Data;
import org.librairy.boot.model.utils.TimeUtils;
import org.librairy.boot.storage.generator.URIGenerator;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class Similarity {

    String uri1;

    String uri2;

    Double score;

    public String getType1(){
        return URIGenerator.typeFrom(uri1).key();
    }

    public String getType2(){
        return URIGenerator.typeFrom(uri2).key();
    }

    public String getDate(){
        return TimeUtils.asISO();
    }
}
