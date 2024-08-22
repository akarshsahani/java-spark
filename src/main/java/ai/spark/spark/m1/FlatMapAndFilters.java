package ai.spark.spark.m1;

import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class FlatMapAndFilters {

    @Autowired
    private SparkConfig sparkConfig;

    public String getFlatMap() {

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        JavaRDD<String> orjStr = javaSparkContext.parallelize(inputData);

        JavaRDD<String> javaRDD = orjStr.flatMap(val -> Arrays.asList(val.split(" ")).iterator());
        javaRDD.collect().forEach(System.out::println);

        return "FlatMap Success !!";
    }

    public String getFilter() {

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        JavaRDD<String> orjStr = javaSparkContext.parallelize(inputData);

        JavaRDD<String> javaRDD = orjStr.flatMap(val -> Arrays.asList(val.split(" ")).iterator());
        JavaRDD<String> filterRdd = javaRDD.filter(word -> word.length() > 1);
        filterRdd.collect().forEach(System.out::println);

        return "Filter Success !!";
    }

    public String getFlatMapFilter() {

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        javaSparkContext.parallelize(inputData)
                .flatMap(val -> Arrays.asList(val.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                .collect()
                .forEach(System.out::println);

        return "FlatMapFilter Success !!";
    }
}
