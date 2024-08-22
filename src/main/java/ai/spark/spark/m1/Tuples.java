package ai.spark.spark.m1;

import ai.spark.spark.m1.dto.IntegerWithSqRoot;
import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Service
public class Tuples {

    @Autowired
    private SparkConfig sparkConfig;

    public String getTuple() {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(34);
        inputData.add(345);
        inputData.add(355);
        inputData.add(78);

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        JavaRDD<Integer> orjInt = javaSparkContext.parallelize(inputData);
        JavaRDD<IntegerWithSqRoot> sqrt = orjInt.map(IntegerWithSqRoot::new);
        JavaRDD<Tuple2<Integer, Double>> sqrtWithTuple = orjInt.map(val -> new Tuple2<>(val, Math.sqrt(val)));
        sqrtWithTuple.collect().forEach(System.out::println);

        return "Tuple Success !!";
    }
}
