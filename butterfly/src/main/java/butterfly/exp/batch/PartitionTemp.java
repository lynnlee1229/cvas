//package butterfly.exp.batch;
//
//import butterfly.core.spatialrdd.ButterflySparkContext;
//import butterfly.core.spatialrdd.PartitionHelper;
//import butterfly.exp.common.ExpUtil;
//import edu.ucr.cs.bdlab.beast.conf.ButterflyConfiguration;
//import edu.ucr.cs.bdlab.beast.geolite.IFeature;
//import org.apache.spark.api.java.JavaRDD;
//
//import java.util.List;
//
//public class PartitionTemp {
//
//    public static void main(String[] args) {
//        ButterflyConfiguration config = ExpUtil.parseConfig(args, "expconfiglocal/exp11.json");
//        System.out.println(config.toPrettyJSON());
//        config.getKeys().forEach(key -> System.out.println(key + " : " + config.get(key)));
//        ButterflySparkContext bsc = ExpUtil.createButterflySparkContext(config);
//        List<JavaRDD<IFeature>> rddList = ExpUtil.getRddList(bsc, config);
//        JavaRDD<IFeature> iFeatureJavaRDD = rddList.get(0);
//        PartitionHelper.partition(iFeatureJavaRDD, bsc, config);
//
//    }
//}
//
