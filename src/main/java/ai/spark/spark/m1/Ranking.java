package ai.spark.spark.m1;

import ai.spark.spark.util.FileUtil;
import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;

@Service
public class Ranking {

    @Autowired
    private SparkConfig sparkConfig;

    public String getRanking() {

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();
        javaSparkContext.textFile("src/main/resources/subtitles/input.txt")
                .map(statement -> statement.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .filter(statement -> !statement.trim().isEmpty())
                .flatMap(statement -> Arrays.asList(statement.split(" ")).iterator())
                .filter(word -> !word.trim().isEmpty())
                .filter(FileUtil::isNotBoring)
                .mapToPair(word -> new Tuple2<String, Long>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(stringLongTuple2 -> new Tuple2<Long, String >(stringLongTuple2._2, stringLongTuple2._1))
                .sortByKey(false)
                .take(10)
                .forEach(System.out::println);

        return "Ranking Success !!";
    }
}
