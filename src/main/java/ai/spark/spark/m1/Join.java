package ai.spark.spark.m1;

import ai.spark.spark.util.SparkConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Service
public class Join {

    @Autowired
    private SparkConfig sparkConfig;

    public String getJoin() {

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String >> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = javaSparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String > users = javaSparkContext.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
        joinedRdd.collect().forEach(System.out::println);

        return "Join Success !!";
    }

    public String getLeftJoin() {

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String >> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = javaSparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String > users = javaSparkContext.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);
        joinedRdd.collect().forEach(System.out::println);
        joinedRdd.foreach(word -> System.out.println(word._2._2.orElse("").toUpperCase()));

        return "LeftJoin Success !!";
    }

    public String getRightJoin() {

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String >> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = javaSparkContext.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String > users = javaSparkContext.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(users);
        joinedRdd.collect().forEach(System.out::println);
        joinedRdd.foreach(word -> System.out.println("user " + word._2._2 + " had " + word._2._1.orElse(0)));

        return "RightJoin Success !!";
    }
}
