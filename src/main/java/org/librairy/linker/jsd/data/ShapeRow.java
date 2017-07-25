package org.librairy.linker.jsd.data;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class ShapeRow implements Serializable {

    private String uri;
    private Long id;
    private List<Double> vector;
    private String date;
    private String type;
}