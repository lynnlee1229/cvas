import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

import java.io.IOException;

/**
 * @author Lynn Lee
 * @date 2024/2/20
 **/
public class TestBase
{
    protected static SparkConf conf;
    protected static JavaSparkContext sc;

    protected static void initialize(final String testSuiteName)
    {
        conf = new SparkConf().setAppName(testSuiteName).setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
//        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    protected static void deleteFile(String path)
            throws IOException
    {
        Configuration hadoopConfig = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConfig);
        fileSystem.delete(new Path(path), true);
    }
    }
