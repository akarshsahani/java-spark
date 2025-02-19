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
public class BigData {

    @Autowired
    private SparkConfig sparkConfig;

    public String getBigData() {

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        boolean testMode = true;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(javaSparkContext, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(javaSparkContext, testMode);
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(javaSparkContext, testMode);

        JavaPairRDD<Integer, Integer> chapterCountRDD = chapterData.mapToPair(row -> new Tuple2<>(row._2, 1))
                .reduceByKey(Integer::sum);

        viewData = viewData.distinct();
        viewData = viewData.mapToPair(row -> new Tuple2<>(row._2, row._1));
//        viewData.collect().forEach(System.out::println);

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRDD = viewData.join(chapterData);
//        joinedRDD.collect().forEach(System.out::println);

        JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joinedRDD.mapToPair(row -> {
            Integer userId = row._2._1;
            Integer courseId = row._2._2;
            return new Tuple2<>(new Tuple2<>(userId, courseId), 1L);
        });

        step3 = step3.reduceByKey(Long::sum);

        JavaPairRDD<Integer, Long> step5 = step3.mapToPair(row -> new Tuple2<>(row._1._2, row._2));

        JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step5.join(chapterCountRDD);

        JavaPairRDD<Integer, Double> step7 = step6.mapValues(value -> (double)value._1 / value._2);

        JavaPairRDD<Integer, Long> step8 = step7.mapValues(value -> {
            if (value > 0.9) return 10L;
            if (value > 0.5) return 4L;
            if (value > 0.25) return 2L;
            return 0L;
        });
        step8 = step8.reduceByKey(Long::sum);

        JavaPairRDD<Integer, Tuple2<Long, String>> step10 = step8.join(titlesData);

        JavaPairRDD<Long, String> step11 = step10.mapToPair(row -> new Tuple2<>(row._2._1, row._2._2));
        step11.sortByKey(false).collect().forEach(System.out::println);
        return "BigData Success !!";
    }

    public String runBigDataOne() {

        JavaSparkContext javaSparkContext = sparkConfig.getJavaSparkContextConnection();

        boolean testMode = true;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(javaSparkContext, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(javaSparkContext, testMode);
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(javaSparkContext, testMode);

        JavaPairRDD<Integer, Integer> chapterCountRDD = chapterData.mapToPair(row -> new Tuple2<>(row._2, 1))
                .reduceByKey(Integer::sum);

        JavaPairRDD<Long, String> result = viewData.distinct()
                .mapToPair(row -> new Tuple2<>(row._2, row._1))
                .join(chapterData)
                .mapToPair(row -> {
                    Integer userId = row._2._1;
                    Integer courseId = row._2._2;
                    return new Tuple2<>(new Tuple2<>(userId, courseId), 1L);
                })
                .reduceByKey(Long::sum)
                .mapToPair(row -> new Tuple2<>(row._1._2, row._2))
                .join(titlesData)
                .mapToPair(row -> new Tuple2<>(row._2._1, row._2._2))
                .sortByKey(false);
//                .collect().
//                forEach(System.out::println);

        result.collect().forEach(System.out::println);


        return "BigDataOne Success !!";
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing-figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, String>(Integer.parseInt(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing-figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(Integer.parseInt(cols[0]), Integer.parseInt(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing-figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
//                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                    return new Tuple2<Integer, Integer>(Integer.parseInt(columns[0]), Integer.parseInt(columns[1]));
                });
    }
}
