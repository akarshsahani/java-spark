package ai.spark.spark.m1;

import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class Mapping {

    @Autowired
    private SparkConfig sparkConfig;

    public String getMapping() {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(34);
        inputData.add(345);
        inputData.add(355);
        inputData.add(78);

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(inputData);

//        JavaRDD<Double> doubleJavaRDD = javaRDD.map(val -> Math.sqrt(val));

        JavaRDD<Double> result = javaRDD.map(Math::sqrt);
//        result.foreach(System.out::println);
        result.collect().forEach(System.out::println);
        result.foreach(val -> System.out.println(val));

        System.out.println("number of sqroot :: " + result.count());

        JavaRDD<Long>  singleLongRdd = result.map(val -> 1L);
        Long count = singleLongRdd.reduce(Long::sum);
        System.out.println(count);

        return "Map Success !!";
    }
}
