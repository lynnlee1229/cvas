package butterfly.example.sedonapractice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * @author Lynn Lee
 * @date 2023/12/6
 **/
public class sedonaIO {
    public static void main(String[] args) {

    }

    @Test
    public void readWKT() {
        SparkConf conf = new SparkConf().setAppName("SedonaIOTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String InputLocation = "/Users/lynnlee/Data/diploma/shenzhen_landuse.csv";
        System.out.println(sc.textFile(InputLocation).count());
//        SpatialRDD<Geometry> geometrySpatialRDD = WktReader.readToGeometryRDD(sc, InputLocation, 3, true, false);
//        geometrySpatialRDD.getRawSpatialRDD().foreach(geometry -> System.out.println(geometry.toString()));
    }
}
