package org.librairy.linker.jsd.data;

import lombok.Data;

import java.util.List;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class Shape {

    Long id;

    String uri;

    String type;

    String date;

    List<Double> vector;
}
