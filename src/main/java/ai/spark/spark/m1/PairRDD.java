package ai.spark.spark.m1;

import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Service
public class PairRDD {

    @Autowired
    private SparkConfig sparkConfig;

    public String getPairRDD() {

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        JavaRDD<String> orjStr = javaSparkContext.parallelize(inputData);

        JavaPairRDD<String, Long> javaPairRDD = orjStr.mapToPair(rawVal -> {
            String[] col = rawVal.split(":");
            return new Tuple2<>(col[0], 1L);
        });
        JavaPairRDD<String, Long> sumJavaRDD = javaPairRDD.reduceByKey(Long::sum);
        sumJavaRDD.foreach(tuple -> System.out.println(tuple._1 + " : " + tuple._2));

        return "PairRDD Success !!";
    }

    public String getPairRDDLambda() {

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        JavaRDD<String> orjStr = javaSparkContext.parallelize(inputData);

        JavaPairRDD<String, Long> javaPairRDD = orjStr.mapToPair(rawVAl -> new Tuple2<>(rawVAl.split(":") [0], 1L));

        JavaPairRDD<String, Long> sumJavaRDD = javaPairRDD.reduceByKey(Long::sum);
        sumJavaRDD.foreach(tuple -> System.out.println(tuple._1 + " : " + tuple._2));

        return "PairRDDLambda Success !!";
    }

    public String getPairRDDFluentAPI() {

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        javaSparkContext.parallelize(inputData)
                .mapToPair(rawVal -> new Tuple2<>(rawVal.split(":") [0], 1L))
                .reduceByKey(Long::sum)
                .foreach(stringLongTuple2 -> System.out.println(stringLongTuple2._1 + " : " + stringLongTuple2._2));

        return "PairRDDFluentAPI Success !!";
    }
}
