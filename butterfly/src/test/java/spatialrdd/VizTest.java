package spatialrdd;

import butterfly.core.common.ButterflyConstant;
import butterfly.core.common.CaseVectorRDD;
import butterfly.core.common.PredefinedVectorRDD;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.core.spatialrdd.VectorRDD;
import cn.edu.whu.lynn.JavaSpatialRDDHelper;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.core.GeometryAdaptiveSplitter;
import cn.edu.whu.lynn.core.GeometryQuadSplitter;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.indexing.GridPartitioner;
import cn.edu.whu.lynn.indexing.RRSGrovePartitioner;
import cn.edu.whu.lynn.indexing.STRPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/1/10
 **/
public class VizTest {
    @Test
    public void vizTest1() {
        VectorRDD<Geometry> pointTDrive = CaseVectorRDD.getPointTDrive();
        JavaRDD<IFeature> featureRDD = pointTDrive.getFeatureRDD();
        List<Geometry> geom10k = featureRDD.map(f -> f.getGeometry()).take(10000);

        // 建立 R 树索引
        STRtree spatialIndex = new STRtree();
        // 将点数据添加到索引中
        for (Geometry geom : geom10k) {
            spatialIndex.insert(geom.getEnvelopeInternal(), geom);
        }
        // 构建索引
        spatialIndex.build();
    }

    @Test
    public void vizTest2() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> polygonShenzhenLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonShenzhenLandUse();
        polygonShenzhenLandUse.doPartition2(STRPartitioner.class, "pnum", 2000);
        polygonShenzhenLandUse.plotImage(1000, 1000, "outs/vizTest2.png", new ButterflyOptions());
    }

    @Test
    public void vizTest3() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> polygonShenzhenLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonShenzhenLandUse();
        polygonShenzhenLandUse.doPartition2(RRSGrovePartitioner.class, "pnum", 2000);
//        polygonShenzhenLandUse.doPartition2(RSGrovePartitioner.class, "size", 2000, new SizeFunctions.pointNum(), new BeastOptions());
        polygonShenzhenLandUse.plotImage(1000, 1000, "outs/vizTest3.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }

    @Test
    public void vizTest4() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> landUseMax = new PredefinedVectorRDD(javaSparkContext).getLandUseMax();
        landUseMax.doPartition2(GridPartitioner.class, "pnum", 2000);
//        landUseMax.doPartition2(RSGrovePartitioner.class, "size", 2000, new SizeFunctions.pointNum(), new BeastOptions());
        landUseMax.plotImage(1000, 1000, "outs/landusemax.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }

    @Test
    public void quadSplitTest() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.LAND_USE_MAX);

        int[] thresholds = {50, 100, 200, 400, 800, 1600};

        for (int threshold : thresholds) {
            long startTime = System.nanoTime();

            RDD<IFeature> iFeatureRDD = GeometryQuadSplitter.splitRDD(geometryVectorRDD.getFeatureRDD().rdd(), threshold, true);
            JavaRDD<IFeature> javaRDD = iFeatureRDD.toJavaRDD();

            long endTime = System.nanoTime();
            long elapsedTime = endTime - startTime;
            // 修改填充颜色为绿色
            JavaSpatialRDDHelper.plotImage(javaRDD, 2000, 2000, "outs/landuse/splitPolygons_" + threshold + ".png",
                    new ButterflyOptions().set("stroke", "black").set("fill", "#aaffaa"));

            System.out.println("Threshold " + threshold + " calculation time: " + elapsedTime + " nanoseconds");
        }
    }

    @Test
    public void quadSplitTestDebug() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        ButterflySparkContext bsc = new ButterflySparkContext(jsc);
        JavaRDD<IFeature> districtRDD = bsc.readWKTFile("hdfs://localhost:9000/butterfly/testin/行政边界_市级.csv", 0, ",", true);
        int threshold = 1000;
        RDD<IFeature> iFeatureRDD = GeometryQuadSplitter.splitRDD(districtRDD.rdd(), threshold, true);
        System.out.println(iFeatureRDD.count());

    }

    @Test
    public void quadAdaptiveSplitTest() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.LAND_USE_MAX);

        int[] thresholds = {50, 100, 200, 400, 800, 1600};

        for (int threshold : thresholds) {
            long startTime = System.nanoTime();

            RDD<IFeature> iFeatureRDD = GeometryAdaptiveSplitter.adapSplitRDD(geometryVectorRDD.getFeatureRDD().rdd(), threshold, true, 256);
            JavaRDD<IFeature> javaRDD = iFeatureRDD.toJavaRDD();

            long endTime = System.nanoTime();
            long elapsedTime = endTime - startTime;
            // 修改填充颜色为绿色
            JavaSpatialRDDHelper.plotImage(javaRDD, 2000, 2000, "outs/landuse/adapSplitPolygons_" + threshold + ".png",
                    new ButterflyOptions().set("stroke", "black").set("fill", "#aaffaa"));

            System.out.println("Threshold " + threshold + " calculation time: " + elapsedTime + " nanoseconds");
        }
    }


}
