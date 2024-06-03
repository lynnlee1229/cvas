package butterfly.exp.batch;

import butterfly.core.common.Key;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.exp.common.ExpUtil;
import butterfly.exp.common.HDFSUtil;
import cn.edu.whu.lynn.conf.ButterflyConfiguration;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public class JoinBatchTemp {

    public static void main(String[] args) {
//        PropertyConfigurator.configure(JoinBatchTemp.class.getClassLoader().getResource("log4j.properties"));
        ButterflyConfiguration config = ExpUtil.parseConfig(args, "expconfiglocal/exp09.json");
        System.out.println(config.toPrettyJSON());
        config.getKeys().forEach(key -> System.out.println(key + " : " + config.get(key)));
        ButterflySparkContext bsc = ExpUtil.createButterflySparkContext(config);
        List<JavaRDD<IFeature>> rddList = ExpUtil.getRddList(bsc, config);
        // 降序打印poygon点数
        //        JavaRDD<IFeature> PolygonRDD = rddList.get(1);
//        PolygonRDD.mapToPair(p -> new Tuple2<>(p.getGeometry().getNumPoints(), p.getGeometry())).sortByKey(false).collect().forEach(p -> System.out.println(p._1 + " : " + p._2.getGeometryType()));
        // Join
        ButterflyConfiguration joinParam = config.getConfiguration(Key.JOIN_PARAM_KEY);
        if (joinParam != null) {
            // TODO 参数传入累加器，用于计算执行时间、吞吐量等指标
            MetricsAccumulator metricsAccumulator = new MetricsAccumulator();
            metricsAccumulator.setLeftCount(bsc.sc().longAccumulator("leftCount"))
                    .setMbrCount(bsc.sc().longAccumulator("mbrCount"))
                    .setResultCount(bsc.sc().longAccumulator("resultCount"))
                    .setJoinPreparationTime(bsc.sc().doubleAccumulator("joinPreparationTime"))
//                    .setFilterTime(bsc.sc().doubleAccumulator("filterTime"))
                    .setQuadSplitTime(bsc.sc().doubleAccumulator("quadSplitTime"))
                    .setJoinTime(bsc.sc().doubleAccumulator("joinTime"))
                    .setFullProcessTime(bsc.sc().doubleAccumulator("fullProcessTime"))
                    .setThroughput(bsc.sc().doubleAccumulator("throughput"))
                    .setFullProcessTimeAll(bsc.sc().doubleAccumulator("fullProcessTimeAll"))
                    .setThroughputAll(bsc.sc().doubleAccumulator("throughputAll"))
                    .setParams(config.getMap(Key.PARAM_MAP));
            if(rddList.size()>1){
                metricsAccumulator.setRightCount(bsc.sc().longAccumulator("rightCount"));
            }
            long l1 = System.nanoTime();
            JavaPairRDD<IFeature, IFeature> joinResult = ExpUtil.getJoinResultWithMetric(rddList, joinParam, metricsAccumulator);
            long l2 = System.nanoTime();
            metricsAccumulator.getFullProcessTime().add((l2 - l1) / 1e9);
            metricsAccumulator.calcJoinPreparationTime();
            if (joinResult != null) {
                metricsAccumulator.getLeftCount().add(rddList.get(0).count());
                if(rddList.size() > 1) {
                    metricsAccumulator.getRightCount().add(rddList.get(1).count());
                }
                metricsAccumulator.getResultCount().add(joinResult.count());
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
