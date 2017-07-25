package org.librairy.linker.jsd.functions;

import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class RowToTupleVector implements Serializable, Function<Row, Tuple2<String, Vector>> {

    @Override
    public Tuple2<String, Vector> call(Row row) throws Exception {

        return new Tuple2<String, Vector>(row.getString(0), Vectors.dense(Doubles.toArray(row.getList(1))));
    }

}
