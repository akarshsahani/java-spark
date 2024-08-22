package ai.spark.spark.m2;

import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Starting {

    @Autowired
    private SparkConfig sparkConfig;

    public String runSparkSql() {
        

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        return "SparkSql Started !!";
    }
}
