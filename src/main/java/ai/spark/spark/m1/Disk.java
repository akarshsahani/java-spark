package ai.spark.spark.m1;

import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class Disk {

    @Autowired
    private SparkConfig sparkConfig;

    public String loadTextFile() {

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();
        javaSparkContext.textFile("src/main/resources/subtitles/input.txt")
                .flatMap(val -> Arrays.asList(val.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                .collect()
                .forEach(System.out::println);

        return "Text File Loaded";
    }
}
