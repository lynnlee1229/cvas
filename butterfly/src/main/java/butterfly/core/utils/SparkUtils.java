package butterfly.core.utils;


import butterfly.core.spatialrdd.ButterflySparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class SparkUtils {

    public static SparkSession createSession(SparkConf sparkConf, String className) {
        return SparkSession
                .builder()
                .appName(className + "_" + System.currentTimeMillis())
                .config(sparkConf)
                .getOrCreate();
    }

    public static JavaSparkContext createJavaSparkContext(String master, String appName, SparkConf conf) {
        return new JavaSparkContext(master, appName, conf);
    }

    public static JavaSparkContext createJavaSparkContext(SparkConf conf) {
        return new JavaSparkContext(conf);
    }

    public static JavaSparkContext createJavaSparkContext(String master, String appName) {
        return new JavaSparkContext(master, appName);
    }

    public static ButterflySparkContext createButterflySparkContext(String master, String appName, SparkConf conf) {
        return new ButterflySparkContext(master, appName, conf);
    }

    public static ButterflySparkContext createButterflySparkContext(SparkConf conf) {
        return new ButterflySparkContext(conf);
    }

    public static ButterflySparkContext createButterflySparkContext(String master, String appName) {
        return new ButterflySparkContext(master, appName);
    }

}
