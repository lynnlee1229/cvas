package butterfly.exp.batch;

import butterfly.core.common.Key;
import butterfly.core.enums.PartitionerType;
import butterfly.core.spatialPartitioner.SizeFunctions;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.core.spatialrdd.PartitionHelper;
import butterfly.exp.common.ExpUtil;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.conf.ButterflyConfiguration;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.synopses.Summary;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class PartitionBatchTemp {

    public static void main(String[] args) {
//        PropertyConfigurator.configure(JoinBatchTemp.class.getClassLoader().getResource("log4j.properties"));
        ButterflyConfiguration config = ExpUtil.parseConfig(args, "expconfiglocal/exp11.json");
        ButterflySparkContext bsc = ExpUtil.createButterflySparkContext(config);
        List<JavaRDD<IFeature>> rddList = ExpUtil.getRddList(bsc, config);
        // initRDD
        JavaRDD<IFeature> initRDD = rddList.get(0);
        // Partition
//        ButterflyConfiguration partitionParam = config.getConfiguration(Key.PARTITION_PARAM);
        ButterflyConfiguration partitionParam = config.getConfiguration(Key.PARTITION_PARAM_BAK);
        String pcriterion = partitionParam.getString(Key.PCRITERION, "size");
        long pvalue = partitionParam.getLong(Key.PVALUE, 20000);
        String sizeFunctionName = partitionParam.getString(Key.SiZE_FUNCTION, "pointNum");
        String outPath = partitionParam.getString("picOutputPath", null);
        ButterflyOptions opts = ExpUtil.parseButterflyOptions(partitionParam.getConfiguration(Key.OPTS));
        PartitionerType partitionerType = PartitionerType.getPartitionerType(opts.getString(Key.PARTITIONER_TYPE, "GRID"));
        Function<IFeature, Object> sizeFunction = SizeFunctions.getSizeFunction(sizeFunctionName);
        initRDD = PartitionHelper.partition(initRDD, partitionerType.getPartitionerClass(), pcriterion, pvalue, sizeFunction, opts);
        // 对initRDD每个分区进行统计
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = PartitionHelper.computePartialSummaries(initRDD, sizeFunction);
        tuple2JavaRDD.collect().forEach(System.out::println);
        calculateMetrics(tuple2JavaRDD, "numPoints");

//        PlotHelper.plotFeatures(initRDD, 1000, 1000, outPath, GeometricPlotter.class, null, new ButterflyOptions().set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }

    public static void calculateMetrics(JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD, String sizeType) {
        // 根据sizeType选择相应的获取size的方法
        JavaRDD<Double> sizes = tuple2JavaRDD.map(tuple -> {
            Summary summary = tuple._2;
            if ("numPoints".equals(sizeType)) {
                return (double) summary.numPoints();
            } else if ("numFeatures".equals(sizeType)) {
                return (double) summary.numFeatures();
            } else {
                throw new IllegalArgumentException("Unsupported size type: " + sizeType);
            }
        });

        // 计算分区之间size的方差
        double variance = calculateVariance(sizes);
        System.out.println("Size variance: " + variance);

        // 计算分区之间size的最大值
        double max = calculateMax(sizes);
        System.out.println("Size max: " + max);

        // 计算分区之间size的最小值
        double min = calculateMin(sizes);
        System.out.println("Size min: " + min);

        // 极差
        double range = max - min;
        System.out.println("Size range: " + range);

        // 计算变异系数
        double coefficientOfVariation = calculateCoefficientOfVariation(sizes);
        System.out.println("Size coefficient of variation: " + coefficientOfVariation);
    }

    private static double calculateVariance(JavaRDD<Double> sizes) {
        double sum = sizes.reduce((x, y) -> x + y);
        double mean = sum / sizes.count();
        return sizes.map(x -> (x - mean) * (x - mean)).reduce((x, y) -> x + y) / sizes.count();
    }

    private static double calculateMax(JavaRDD<Double> sizes) {
        return sizes.reduce(Math::max);
    }

    private static double calculateMin(JavaRDD<Double> sizes) {
        return sizes.reduce(Math::min);
    }

    private static double calculateStandardDeviation(JavaRDD<Double> sizes) {
        double variance = calculateVariance(sizes);
        return Math.sqrt(variance);
    }

    private static double calculateCoefficientOfVariation(JavaRDD<Double> sizes) {
        double sum = sizes.reduce((x, y) -> x + y);
        double mean = sum / sizes.count();
        if (mean == 0) return 0; // 防止除以零
        double standardDeviation = calculateStandardDeviation(sizes);
        return standardDeviation / mean;
    }
}
//        if (partitionParam != null) {
//            // TODO 参数传入累加器，用于计算执行时间、吞吐量等指标
//            MetricsAccumulator metricsAccumulator = new MetricsAccumulator();
//            metricsAccumulator.setLeftCount(bsc.sc().longAccumulator("leftCount"))
//                    .setRightCount(bsc.sc().longAccumulator("rightCount"))
//                    .setMbrCount(bsc.sc().longAccumulator("mbrCount"))
//                    .setResultCount(bsc.sc().longAccumulator("resultCount"))
//                    .setJoinPreparationTime(bsc.sc().doubleAccumulator("joinPreparationTime"))
////                    .setFilterTime(bsc.sc().doubleAccumulator("filterTime"))
//                    .setQuadSplitTime(bsc.sc().doubleAccumulator("quadSplitTime"))
//                    .setJoinTime(bsc.sc().doubleAccumulator("joinTime"))
//                    .setFullProcessTime(bsc.sc().doubleAccumulator("fullProcessTime"))
//                    .setThroughput(bsc.sc().doubleAccumulator("throughput"))
//                    .setParams(config.getMap(Key.PARAM_MAP));
//            long l1 = System.nanoTime();
////            JavaPairRDD<IFeature, IFeature> joinResult = ExpUtil.getJoinResultWithMetric(rddList, joinParam, metricsAccumulator);
//            long l2 = System.nanoTime();
//            metricsAccumulator.getFullProcessTime().add((l2 - l1) / 1e9);
//            metricsAccumulator.calcJoinPreparationTime();
//            if (joinResult != null) {
//                metricsAccumulator.getLeftCount().add(rddList.get(0).count());
//                metricsAccumulator.getRightCount().add(rddList.get(1).count());
//                metricsAccumulator.getResultCount().add(joinResult.count());
//                metricsAccumulator.getThroughput().add(metricsAccumulator.getResultCount().value() / metricsAccumulator.getFullProcessTime().value());
//            }
//            metricsAccumulator.printMetrics();
////            System.out.println(metricsAccumulator.toCSV(true));
////            System.out.println(metricsAccumulator.toJSON());
//            HDFSUtil.writeToCSV(config, metricsAccumulator);
//        }
//    }
//}
