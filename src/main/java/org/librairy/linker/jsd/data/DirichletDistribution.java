package org.librairy.linker.jsd.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import scala.Tuple2;

import java.io.Serializable;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
@Data
public class DirichletDistribution implements Serializable {
    private String label;
    private List<Double> vector;

    private String id;

    public DirichletDistribution(){}

    public DirichletDistribution(String id, Integer dimension){

        this.id = id;

        Random random = new Random();

        List<Integer> probabilities = IntStream.range(0, dimension).mapToObj(i -> random.nextInt(Double.valueOf(Math.pow(10,random.nextInt(3)+1)).intValue())+1).collect(Collectors.toList());

        Integer total = probabilities.stream().reduce((a,b) -> a+b).get();

        this.vector = probabilities.stream().map(val -> {
            double ratio = Double.valueOf(val) / Double.valueOf(total);

            if (ratio == 0.0){
                System.out.println("val:" + val + " / total: " + total);

            }

            return ratio;

        }).collect(Collectors.toList());
    }

    public DirichletDistribution(String id, List<Double> vector, Double threshold){
        this.id = id;
        this.vector = vector;
        this.label = getSortedTopics(threshold);
    }

    @JsonIgnore
    public Integer getHighestTopic(){
        return IntStream.range(0,vector.size())
                .reduce((a,b) -> (vector.get(a) > vector.get(b)? a : b))
                .getAsInt();
    }

    @JsonIgnore
    public Integer getLowestTopic(){
        return IntStream.range(0,vector.size())
                .reduce((a,b) -> (vector.get(a) < vector.get(b)? a : b))
                .getAsInt();
    }

    @JsonIgnore
    public String getSortedTopics(Integer top){
        return IntStream
                .range(0,vector.size())
                .mapToObj(i -> new Tuple2<Integer,Double>(i,vector.get(i)))
                .sorted( (a,b) -> -a._2.compareTo(b._2))
                .map( t -> String.valueOf(t._1))
                .limit(top)
                .collect(Collectors.joining("|"));
    }


    public String getSortedTopics(Double threshold){
        List<Tuple2<Integer, Double>> topics = IntStream
                .range(0, vector.size())
                .mapToObj(i -> new Tuple2<Integer, Double>(i, vector.get(i)))
                .sorted((a, b) -> -a._2.compareTo(b._2))
                .collect(Collectors.toList());

        Integer maxIndex = 0;
        Double accumulated = 0.0;
        for(Tuple2<Integer, Double> topic : topics){

            accumulated += topic._2;
            maxIndex += 1;

            if (accumulated >= threshold) break;

        }
        return topics.stream()
                .map( t -> String.valueOf(t._1))
                .limit(maxIndex)
                .collect(Collectors.joining("|"));

    }

    @JsonIgnore
    public DoubleSummaryStatistics getStats(){
        return vector.stream().collect(DoubleSummaryStatistics::new, DoubleSummaryStatistics::accept, DoubleSummaryStatistics::combine);
    }
}
