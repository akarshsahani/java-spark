package ai.spark.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

@Component
public class SparkConfig implements DisposableBean {

    // TODO working, but written much more optimised code
    // TODO to use below code remove implements DisposableBean
    /*private static JavaSparkContext javaSparkContext;

    public synchronized JavaSparkContext getJavaSparkContextConnection() {
        if (javaSparkContext == null) {
            SparkConf sparkConf = new SparkConf().setAppName("StartingSpark").setMaster("local[2]");
            javaSparkContext = new JavaSparkContext(sparkConf);
        }
        return javaSparkContext;
    }

    public void closeJavaContextConnection() {
        if (javaSparkContext != null) {
            javaSparkContext.close();
            javaSparkContext = null;
        }
    }*/

    private JavaSparkContext javaSparkContext;

    public JavaSparkContext getJavaSparkContextConnection() {
        if (javaSparkContext == null) {
            SparkConf sparkConf = new SparkConf().setAppName("StartingSpark").setMaster("local[2]");
            javaSparkContext = new JavaSparkContext(sparkConf);
        }
        return javaSparkContext;
    }

    @Override
    public void destroy() throws Exception {
        if (javaSparkContext != null) {
            javaSparkContext.close();
        }
    }
}
