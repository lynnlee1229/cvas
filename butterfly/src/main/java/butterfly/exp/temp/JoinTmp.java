package butterfly.exp.temp;

import butterfly.core.common.Key;
import cn.edu.whu.lynn.conf.ButterflyConfiguration;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.exp.common.ExpUtil;
import butterfly.exp.common.FileUtil;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.InputStream;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/3/5
 **/
public class JoinTmp {
    public static void main(String[] args) {
        String fileStr;
        if (args.length > 1) {
            String fs = args[0];
            String filePath = args[1];
            fileStr = FileUtil.readFully(fs, filePath);
        } else if (args.length == 1) {
            String confPath = args[0];
            fileStr = FileUtil.readFileToString(confPath);
        } else {
            InputStream resourceAsStream = JoinTmp.class.getClassLoader()
//                    .getResourceAsStream("jointemp/WKT2JoinLocalTemp.json");
//            .getResourceAsStream("jointemp/Gen2JoinTemp.json");
                    .getResourceAsStream("conftemp/WKT2JoinLocalTemp.json");
            fileStr = FileUtil.readFileToString(resourceAsStream);
        }
        ButterflyConfiguration config = ButterflyConfiguration.from(fileStr);
//        Logger.getLogger(SpatialJoinWithSimplification.class).setLevel(Level.DEBUG);
        ButterflySparkContext bsc = ExpUtil.createButterflySparkContext(config);
        List<JavaRDD<IFeature>> rddList = ExpUtil.getRddList(bsc, config);
        // Join
        ButterflyConfiguration joinParam = config.getConfiguration(Key.JOIN_PARAM_KEY);
        if (joinParam != null) {
            // TODO 参数传入累加器，用于计算执行时间、吞吐量等指标
            MetricsAccumulator metricsAccumulator = new MetricsAccumulator();
            metricsAccumulator.setLeftCount(bsc.sc().longAccumulator("leftCount"))
                    .setRightCount(bsc.sc().longAccumulator("rightCount"))
                    .setMbrCount(bsc.sc().longAccumulator("mbrCount"))
                    .setResultCount(bsc.sc().longAccumulator("resultCount"))
                    .setJoinPreparationTime(bsc.sc().doubleAccumulator("joinPreparationTime"))
                    .setFilterTime(bsc.sc().doubleAccumulator("filterTime"))
                    .setQuadSplitTime(bsc.sc().doubleAccumulator("quadSplitTime"))
                    .setJoinTime(bsc.sc().doubleAccumulator("joinTime"));
            JavaPairRDD<IFeature, IFeature> joinResult = ExpUtil.getJoinResultWithMetric(rddList, joinParam, metricsAccumulator);
            if (joinResult != null) {
                metricsAccumulator.getResultCount().add(joinResult.count());
            }
            metricsAccumulator.printMetrics();
        }
    }

}
