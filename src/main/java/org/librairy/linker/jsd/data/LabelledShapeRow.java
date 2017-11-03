package org.librairy.linker.jsd.data;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class LabelledShapeRow implements Serializable {

    private String domain;
    private String label;
    private String uri;
    private List<Double> vector;
    private String type;
}