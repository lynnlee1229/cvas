package tmpexp.testbase;

import butterfly.core.enums.Distribution;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.core.spatialrdd.PartitionHelper;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.core.SpatialPartitioner;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2024/1/28
 **/
public class TestBase implements Serializable {
    // 输出父路径
    protected static String FATHER_PATH;

    protected static SparkConf conf;
    protected static JavaSparkContext sc;
    protected static ButterflySparkContext bsc;
    public final Distribution defaultPointDistribution = Distribution.UNIFORM;
    public final Distribution defaultNonPointDistribution = Distribution.UNIFORM;
    public final int defaultNumPartitions = 8;
    public final int defaultPolygonSegmentNum = 10;
    public final double defaultPolygonSize = 0.1;
    @BeforeClass
    public static void setUp() {
        initialize("TestBase");
    }

    protected static void initialize(final String testSuiteName) {
        conf = new SparkConf().setAppName(testSuiteName).setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryoserializer.buffer.max", "512m");
//        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());
        sc = new JavaSparkContext(conf);
        bsc = new ButterflySparkContext(sc);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    protected static void deleteFile(String path)
            throws IOException {
        Configuration hadoopConfig = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConfig);
        fileSystem.delete(new Path(path), true);
    }

    public JavaRDD<IFeature> genPointData(Distribution distribution, int pointNum, Integer numPartitions, ButterflyOptions opts) {
        if (distribution == null) {
            distribution = defaultPointDistribution;
        }
        if (numPartitions == null) {
            numPartitions = defaultNumPartitions;
        }
        if (opts == null) {
            opts = new ButterflyOptions();
        }
        return bsc.generatePointData(distribution, pointNum, numPartitions, opts);
    }

    public JavaRDD<IFeature> genNonPointData(Distribution distribution, Integer nonPointNum, Integer numPartitions, ButterflyOptions opts, Integer polygonSegmentNum, Double polygonSize) {
        if (distribution == null) {
            distribution = defaultNonPointDistribution;
        }
        if (numPartitions == null) {
            numPartitions = defaultNumPartitions;
        }
        if (opts == null) {
            opts = new ButterflyOptions();
        }
        if (polygonSegmentNum == null) {
            polygonSegmentNum = defaultPolygonSegmentNum;
        }
        if (polygonSize == null) {
            polygonSize = defaultPolygonSize;
        }
        return bsc.generatePolygonData(distribution, nonPointNum, numPartitions, opts, polygonSize, polygonSegmentNum);
    }


    public static JavaRDD<IFeature> partition(JavaRDD<IFeature> rdd, Class<? extends SpatialPartitioner> spatialPartitioner, String pcriterion, long pvalue, Function<IFeature, Object> sizeFunction, ButterflyOptions opts) {
        return PartitionHelper.partition(rdd, spatialPartitioner, pcriterion, pvalue, sizeFunction, opts);
    }

    // 封装文件输出功能
    protected void writeToFile(String fileName, String content) {
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }

            try (PrintStream fileOut = new PrintStream(file.getAbsoluteFile())) {
                fileOut.print(content);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 其他通用功能可以在此添加

    private void printProgress(int completed, int total, long startTime) {
        // 如果total小于等于10，或者completed未达到10%的倍数，则打印
        if (total <= 10 || completed % (total / 10) == 0) {
            double progress = ((double) completed / total) * 100;
            System.out.printf("Progress: %.2f%% (Completed: %d, Total: %d)\n", progress, completed, total);

            // Calculate estimated remaining time only if progress is greater than zero
            if (progress > 0) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                long estimatedRemainingTime = (long) ((elapsedTime / progress) * (100 - progress));
                System.out.printf("Estimated Remaining Time: %d seconds\n", estimatedRemainingTime / 1000);
            }
        }
    }
}
