package org.librairy.linker.jsd.functions;

import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.librairy.linker.jsd.data.DirichletDistribution;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class RowToDirichletDistribution implements Serializable, Function<Row, DirichletDistribution> {

    private final String domain;
    private final Double threshold;

    public RowToDirichletDistribution(String domainUri, Double threshold){
        this.domain = domainUri;
        this.threshold = threshold;
    }

    @Override
    public DirichletDistribution call(Row row) throws Exception {
        return new DirichletDistribution(row.getString(0), row.getList(1),threshold);
    }

}
