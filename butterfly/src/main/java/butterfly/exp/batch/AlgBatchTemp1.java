package butterfly.exp.batch;

import butterfly.core.common.Key;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.exp.common.ExpUtil;
import butterfly.exp.common.HDFSUtil;
import cn.edu.whu.lynn.conf.ButterflyConfiguration;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public class AlgBatchTemp1 {

    public static void main(String[] args) {
//        PropertyConfigurator.configure(JoinBatchTemp.class.getClassLoader().getResource("log4j.properties"));
        ButterflyConfiguration config = ExpUtil.parseConfig(args,"expconfiglocal/exp12.json");
        System.out.println(config.toPrettyJSON());
        config.getKeys().forEach(key -> System.out.println(key + " : " + config.get(key)));
        ButterflySparkContext bsc = ExpUtil.createButterflySparkContext(config);
        List<JavaRDD<IFeature>> rddList = ExpUtil.getRddList(bsc, config);
        // Algorithm
        ButterflyConfiguration algParam = config.getConfiguration(Key.ALG_PARAM_KEY);
        if (algParam != null) {
            MetricsAccumulator metricsAccumulator = new MetricsAccumulator();
            metricsAccumulator.setLeftCount(bsc.sc().longAccumulator("leftCount"))
                    .setRightCount(bsc.sc().longAccumulator("rightCount"))
                    .setResultCount(bsc.sc().longAccumulator("resultCount"))
                    .setJoinTime(bsc.sc().doubleAccumulator("joinTime"))
                    .setFullProcessTime(bsc.sc().doubleAccumulator("fullProcessTime"))
                    .setThroughput(bsc.sc().doubleAccumulator("throughput"))
                    .setFullProcessTimeAll(bsc.sc().doubleAccumulator("fullProcessTimeAll"))
                    .setThroughputAll(bsc.sc().doubleAccumulator("throughputAll"))
                    .setParams(config.getMap(Key.PARAM_MAP));
            long l1 = System.nanoTime();
            JavaRDD<IFeature> algResult = ExpUtil.getAlgorithmResult(rddList, algParam, metricsAccumulator);
            long l2 = System.nanoTime();
            metricsAccumulator.getFullProcessTime().add((l2 - l1) / 1e9);
//            metricsAccumulator.calcJoinPreparationTime();
            if (algResult != null) {
                metricsAccumulator.getLeftCount().add(rddList.get(0).count());
                if (rddList.size() > 1) {
                    metricsAccumulator.getRightCount().add(rddList.get(1).count());
                }
                metricsAccumulator.getResultCount().add(algResult.count());
                long l3 = System.nanoTime();
                metricsAccumulator.getFullProcessTimeAll().add((l3 - l1) / 1e9);
                metricsAccumulator.getThroughputAll().add(metricsAccumulator.getResultCount().value() / metricsAccumulator.getFullProcessTimeAll().value());
                metricsAccumulator.getThroughput().add(metricsAccumulator.getResultCount().value() / metricsAccumulator.getFullProcessTime().value());
            }
            metricsAccumulator.printMetrics();
//            System.out.println(metricsAccumulator.toCSV(true));
//            System.out.println(metricsAccumulator.toJSON());
            HDFSUtil.writeToCSV(config, metricsAccumulator);
        }
    }
}
