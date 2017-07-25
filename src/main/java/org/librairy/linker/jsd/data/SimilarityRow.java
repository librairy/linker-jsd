package org.librairy.linker.jsd.data;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class SimilarityRow implements Serializable {

    private String resource_uri_1;

    private String resource_type_1;

    private String resource_uri_2;

    private String resource_type_2;

    private Double score;

    private String date;

}