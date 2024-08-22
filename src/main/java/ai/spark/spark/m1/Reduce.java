package ai.spark.spark.m1;

import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class Reduce {

    Logger logger = LoggerFactory.getLogger(Reduce.class);

    @Autowired
    private SparkConfig sparkConfig;

    public String getReduce() {
        List<Double> inputData = new ArrayList<>();
        inputData.add(34.2);
        inputData.add(345.2);
        inputData.add(34.552);
        inputData.add(67.2);

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        JavaRDD<Double> javaRDD = javaSparkContext.parallelize(inputData);
//        Double result = javaRDD.reduce((val1, val2) -> val1 + val2);
        Double result = javaRDD.reduce(Double::sum);

        System.out.println(result);
        return "Reduce Success !!";
    }
}
