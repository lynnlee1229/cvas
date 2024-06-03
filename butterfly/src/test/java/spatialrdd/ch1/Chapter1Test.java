package spatialrdd.ch1;

import butterfly.core.common.CaseVectorRDD;
import butterfly.core.spatialPartitioner.SizeFunctions;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.core.spatialrdd.VectorRDD;
import cn.edu.whu.lynn.JavaSpatialSparkContext;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.indexing.GridPartitioner;
import cn.edu.whu.lynn.indexing.RRSGrovePartitioner;
import cn.edu.whu.lynn.synopses.Summary;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/1/8
 * 第一章Test：主要测试分区、负载均衡、内存索引、复杂矢量计算的内容
 **/
public class Chapter1Test implements Serializable {

    public final String OUT_PATH = "outs/ch1/";
    public JavaSpatialSparkContext jsc;

    {
        if (jsc != null) {
            jsc.close();
        }
        jsc = new JavaSpatialSparkContext("local[*]", "chapter1Test");
    }

    @Test
    public void IOTest() {
        ButterflySparkContext bsc = new ButterflySparkContext(jsc);
//        ButterflySparkContext bsc = SparkUtils.createButterflySparkContext("local[*]", "chapter1Test");
        final String path = "hdfs://localhost:9000/butterfly/testout/shenzhen_landuse.csv";
        JavaRDD<IFeature> iFeatureJavaRDD = bsc.readWKTFile(path, 4, ',', true);
        System.out.println(iFeatureJavaRDD.count());
    }

    /**
     * 测试分区
     */
    @Test
    public void partitionTest1() {
        VectorRDD<Geometry> polygonDataGen10K = CaseVectorRDD.getPolygonDataGen10K();
        polygonDataGen10K.doPartition2(GridPartitioner.class, "size", 20000, f ->
                        f.getGeometry().getNumPoints()
                , new ButterflyOptions());
        JavaRDD<IFeature> featureRDD = polygonDataGen10K.getFeatureRDD();
        System.out.println(featureRDD.getNumPartitions());
        Summary summary = polygonDataGen10K.summary();
        List<Tuple2<Integer, Summary>> collect = polygonDataGen10K.partitionSummary().collect();
        collect.forEach(t -> System.out.println(t._1 + " " + t._2));
        System.out.println(summary);

        int bp = -1;
    }


    /**
     * 测试负载均衡
     */

    /**
     * 按照点数分区
     */
    @Test
    public void loadBalanceTest1() {
        VectorRDD<Geometry> polygonShenzhenLandUse = CaseVectorRDD.getPolygonShenzhenLandUse();
        polygonShenzhenLandUse.doPartition2(RRSGrovePartitioner.class, "size", 25000, new SizeFunctions.pointNum(), new ButterflyOptions());
        Summary summary = polygonShenzhenLandUse.summary();
        System.out.println(summary);
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = polygonShenzhenLandUse.partitionSummary();
        List<Tuple2<Integer, Summary>> collect = tuple2JavaRDD.collect();
        for (Tuple2<Integer, Summary> t : collect) {
            System.out.println(t._1 + " " + t._2);
        }
    }

    /**
     * 测试内存索引
     */
    @Test
    public void memoryIndexTest1() {

    }

    /**
     * 测试复杂矢量计算
     */
    @Test
    public void complexVectorTest1() {

    }

}
