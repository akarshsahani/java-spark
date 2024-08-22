package ai.spark.spark.controller;

import ai.spark.spark.m1.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class M1Controller {

    @Autowired
    private Starting starting;

    @Autowired
    private Reduce reduce;

    @Autowired
    private Mapping mapping;

    @Autowired
    private Tuples tuples;

    @Autowired
    private PairRDD pairRDD;

    @Autowired
    private FlatMapAndFilters flatMapAndFilters;

    @Autowired
    private Disk disk;

    @Autowired
    private Ranking ranking;

    @Autowired
    private Join join;

    @Autowired
    private BigData bigData;

    @GetMapping
    public String runStart() {
        return starting.runStarting();
    }

    @GetMapping("/reduce")
    public String runReduce() {
        return reduce.getReduce();
    }

    @GetMapping("/mapping")
    public String runMapping() {
        return mapping.getMapping();
    }

    @GetMapping("/tuples")
    public String runTuples() {
        return tuples.getTuple();
    }

    @GetMapping("/pairrdd")
    public String runPairDD() {
        return pairRDD.getPairRDD();
    }

    @GetMapping("/pairrddlambda")
    public String runPairDDLambda() {
        return pairRDD.getPairRDDLambda();
    }

    @GetMapping("/pairrddfluentapi")
    public String runPairDDFluentAPI() {
        return pairRDD.getPairRDDFluentAPI();
    }

    @GetMapping("/flatmap")
    public String runFlatMap() {
        return flatMapAndFilters.getFlatMap();
    }

    @GetMapping("/filter")
    public String runFilter() {
        return flatMapAndFilters.getFilter();
    }

    @GetMapping("/flatmapfilter")
    public String runFlatMapFilter() {
        return flatMapAndFilters.getFlatMapFilter();
    }

    @GetMapping("/disk")
    public String loadDisk() {
        return disk.loadTextFile();
    }

    @GetMapping("/rank")
    public String getRanking() {
        return ranking.getRanking();
    }

    @GetMapping("/join")
    public String getJoin() {
        return join.getJoin();
    }

    @GetMapping("/leftjoin")
    public String getLeftJoin() {
        return join.getLeftJoin();
    }

    @GetMapping("/rightjoin")
    public String getRightJoin() {
        return join.getRightJoin();
    }

    @GetMapping("/bigdata")
    public String getBigBata() {
        return bigData.runBigDataOne();
    }
}
