package spatialrdd;

import butterfly.core.common.ButterflyConstant;
import butterfly.core.common.CaseVectorRDD;
import butterfly.core.common.PredefinedVectorRDD;
import butterfly.core.spatialPartitioner.SizeFunctions;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.core.spatialrdd.VectorRDD;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import com.github.davidmoten.rtree.geometry.Rectangle;
import cn.edu.whu.lynn.JavaSpatialRDDHelper;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.core.GeometryAdaptiveSplitter;
import cn.edu.whu.lynn.core.GeometryQuadSplitter;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.indexing.GridPartitioner;
import cn.edu.whu.lynn.indexing.RRSGrovePartitioner;
import cn.edu.whu.lynn.indexing.RSGrovePartitioner;
import cn.edu.whu.lynn.indexing.STRPartitioner;
import cn.edu.whu.lynn.synopses.Summary;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/1/10
 **/
public class ch1Plot {
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
    public void plotWuhanLanduse() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "cvasTest");
        VectorRDD<Geometry> polygonShenzhenLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonWuhanLandUse();
        polygonShenzhenLandUse.doPartition2(STRPartitioner.class, "pnum", 2000);
        polygonShenzhenLandUse.plotImage(1000, 1000, "outs/ch1/wuhan_landuse.png", new ButterflyOptions().set("fill", "#aaffaa"));
    }

    @Test
    public void plotWuhanLandusePNumPartionGrid() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "cvasTest");
        VectorRDD<Geometry> polygonWuhanLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonWuhanLandUse();
        JavaRDD<IFeature> featureRDD = polygonWuhanLandUse.getFeatureRDD();
        System.out.println(featureRDD.count());
        System.out.println(featureRDD.map(f -> f.getGeometry().getNumPoints()).reduce((a, b) -> a + b));
        polygonWuhanLandUse.doPartition2(GridPartitioner.class, "pnum", 20000);
//        polygonWuhanLandUse.doPartition2(RSGrovePartitioner.class, "size", 2000, new SizeFunctions.pointNum(), new BeastOptions());
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = polygonWuhanLandUse.partitionSummary();
        tuple2JavaRDD.collect().forEach(System.out::println);
        polygonWuhanLandUse.plotImage(1000, 1000, "outs/ch1/wuhan_landuse_partition_gird_pnum.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }
    @Test
    public void plotWuhanLanduseFNumartionGrid() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "cvasTest");
        VectorRDD<Geometry> polygonShenzhenLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonWuhanLandUse();
//        polygonShenzhenLandUse.doPartition2(RGrovePartitioner.class, "pnum", 2000);
        polygonShenzhenLandUse.doPartition2(GridPartitioner.class, "size", 1000, new SizeFunctions.featureNum(), new ButterflyOptions());
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = polygonShenzhenLandUse.partitionSummary();
        tuple2JavaRDD.collect().forEach(System.out::println);
        polygonShenzhenLandUse.plotImage(1000, 1000, "outs/ch1/wuhan_landuse_partition_grid_fnum.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }

    @Test
    public void soutWuhanLanduseFNumartionGrid() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "cvasTest");
        VectorRDD<Geometry> polygonShenzhenLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonWuhanLandUse();
//        polygonShenzhenLandUse.doPartition2(RGrovePartitioner.class, "pnum", 2000);
        polygonShenzhenLandUse.doPartition2(RRSGrovePartitioner .class, "size", 1000, new SizeFunctions.featureNum(), new ButterflyOptions());
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = polygonShenzhenLandUse.partitionSummary();
        tuple2JavaRDD.collect().forEach(System.out::println);
        polygonShenzhenLandUse.plotImage(1000, 1000, "outs/ch1/wuhan_landuse_partition_fnum_new.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }

    @Test
    public void soutWuhanLanduseFNumartionGrid1() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "cvasTest");
        VectorRDD<Geometry> polygonShenzhenLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonWuhanLandUse();
//        polygonShenzhenLandUse.doPartition2(RGrovePartitioner.class, "pnum", 2000);
        polygonShenzhenLandUse.doPartition2(RRSGrovePartitioner .class, "size", 1000, new SizeFunctions.storageSize(), new ButterflyOptions());
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = polygonShenzhenLandUse.partitionSummary();
        tuple2JavaRDD.collect().forEach(System.out::println);
        polygonShenzhenLandUse.plotImage(1000, 1000, "outs/ch1/wuhan_landuse_partition_storage_new.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }

    @Test
    public void plotWuhanLandusePartioner() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "cvasTest");
        VectorRDD<Geometry> polygonShenzhenLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonWuhanLandUse();
//        polygonShenzhenLandUse.doPartition2(RGrovePartitioner.class, "pnum", 2000);
        polygonShenzhenLandUse.doPartition2(RSGrovePartitioner.class, "size", 150, new SizeFunctions.pointNum(), new ButterflyOptions());
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = polygonShenzhenLandUse.partitionSummary();
        tuple2JavaRDD.collect().forEach(System.out::println);
        polygonShenzhenLandUse.plotImage(1000, 1000, "outs/ch1/wuhan_landuse_partitionerTest_rrstree_pnum.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }

    @Test
    public void plotRtree() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "cvasTest");
        VectorRDD<Geometry> polygonWuhanLandUse = new PredefinedVectorRDD(javaSparkContext).getPolygonWuhanLandUse();
        List<Geometry> geometries = polygonWuhanLandUse.getFeatureRDD().map(f -> f.getGeometry()).collect();
//        RTree<Geometry,com.github.davidmoten.rtree.geometry.Geometry> rTree = RTree.maxChildren(500).create();
        RTree<Geometry,com.github.davidmoten.rtree.geometry.Geometry> rTree = RTree.star().maxChildren(500).create();
        for (Geometry geometry : geometries) {
//            rTree = rTree.add(geometry, ToPoint(geometry.getCentroid()));
            rTree = rTree.add(geometry, ToRect(geometry.getEnvelopeInternal()));
        }
        rTree.visualize(1000, 1000).save("outs/ch1/rstree.png");


////        polygonShenzhenLandUse.doPartition2(RGrovePartitioner.class, "pnum", 2000);
//        polygonShenzhenLandUse.doPartition2(RGrovePartitioner.class, "size", 2000, new SizeFunctions.pointNum(), new ButterflyOptions());
//        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = polygonShenzhenLandUse.partitionSummary();
//        tuple2JavaRDD.collect().forEach(System.out::println);
//        polygonShenzhenLandUse.plotImage(1000, 1000, "outs/ch1/wuhan_landuse_partitionerTest_kdtree_pnum.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }
    private static Rectangle ToRect(Envelope box) {
        return Geometries.rectangle(box.getMinX(), box.getMinY(), box.getMaxX(), box.getMaxY());
    }
    private static Point ToPoint(org.locationtech.jts.geom.Point point) {
        return Geometries.point(point.getX(), point.getY());
    }

    @Test
    public void vizTest4() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("local[*]", "cvasTest");
        VectorRDD<Geometry> landUseMax = new PredefinedVectorRDD(javaSparkContext).getLandUseMax();
        landUseMax.doPartition2(GridPartitioner.class, "pnum", 2000);
//        landUseMax.doPartition2(RSGrovePartitioner.class, "size", 2000, new SizeFunctions.pointNum(), new BeastOptions());
        landUseMax.plotImage(1000, 1000, "outs/landusemax.png", new ButterflyOptions().set(ButterflyConstant.PLOT_PARTITION_MBR, true).set(ButterflyConstant.PLOT_BY_PARTITION, true));
    }

    @Test
    public void quadSplitTest() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "cvasTest");
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
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "cvasTest");
        ButterflySparkContext bsc = new ButterflySparkContext(jsc);
        JavaRDD<IFeature> districtRDD = bsc.readWKTFile("hdfs://localhost:9000/butterfly/testin/行政边界_市级.csv", 0, ",", true);
        int threshold = 1000;
        RDD<IFeature> iFeatureRDD = GeometryQuadSplitter.splitRDD(districtRDD.rdd(), threshold, true);
        System.out.println(iFeatureRDD.count());

    }

    @Test
    public void quadAdaptiveSplitTest() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "cvasTest");
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
