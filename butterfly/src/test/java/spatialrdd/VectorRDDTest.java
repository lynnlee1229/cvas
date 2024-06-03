package spatialrdd;

import butterfly.core.common.ButterflyConstant;
import butterfly.core.common.CaseVectorRDD;
import butterfly.core.spatialrdd.VectorRDD;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import cn.edu.whu.lynn.JavaSpatialRDDHelper;
import cn.edu.whu.lynn.core.GeometryQuadSplitter;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.geolite.GeometryHelper;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.indexing.HCurvePartitioner;
import cn.edu.whu.lynn.indexing.STRPartitioner;
import cn.edu.whu.lynn.indexing.ZCurvePartitioner;
import cn.edu.whu.lynn.synopses.Summary;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2023/12/29
 **/
public class VectorRDDTest {
    @Test
    public void saveIndexTest() {
        VectorRDD<Geometry> pointDataGen10K = CaseVectorRDD.getPointDataGen10K();
        pointDataGen10K.doPartition2(HCurvePartitioner.class, "fixed", 4);
        pointDataGen10K.savePartition("outs/index", new ButterflyOptions());
    }

    @Test
    public void fromWKTFile() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromWKTFile(ButterflyConstant.SHENZHEN_LANDUSE_CSV, "geometry", ',', true);
        geometryVectorRDD.getFeatureRDD().foreach(g -> System.out.println(g));
    }

    @Test
    public void fromShapefile() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_LANDUSE_SHP);
        System.out.println(geometryVectorRDD.getFeatureRDD().count());
    }

    @Test
    public void fromCSVFile() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromCSVPoint(ButterflyConstant.TDRIVE, "2", "3", ',', false);
        System.out.println(geometryVectorRDD.getFeatureRDD().count());
//        geometryVectorRDD.getFeatureJavaRDD().foreach(g -> System.out.println(g));
    }

    @Test
    public void beastPlotTest() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_LANDUSE_SHP);
        JavaSpatialRDDHelper.plotImage(geometryVectorRDD.getFeatureRDD(), 2000, 2000, "initPolygons.png",
                new ButterflyOptions().set("stroke", "black").set("fill", "#ffffff"));
        System.out.println(geometryVectorRDD.getFeatureRDD().count());
    }

    @Test
    public void quadSplitTest() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_BOUNDRY);
        RDD<IFeature> iFeatureRDD = GeometryQuadSplitter.splitRDD(geometryVectorRDD.getFeatureRDD().rdd(), 100, true);
        JavaRDD<IFeature> javaRDD = iFeatureRDD.toJavaRDD();
        JavaSpatialRDDHelper.plotImage(javaRDD, 2000, 2000, "pics/splitPolygons-带下界.png",
                new ButterflyOptions().set("stroke", "black").set("fill", "#9999e6"));
    }

    @Test
    public void partitionVisualization() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_LANDUSE_SHP);
        JavaRDD<IFeature> featureJavaRDD = geometryVectorRDD.getFeatureRDD();
        System.out.println(featureJavaRDD.count());
        int numPartition = 4;
//        JavaPairRDD<Integer, IFeature> integerIFeatureJavaPairRDD = JavaSpatialRDDHelper.spatialPartition(featureJavaRDD, ZCurvePartitioner.class, numPartition);
        JavaPairRDD<Integer, IFeature> integerIFeatureJavaPairRDD = JavaSpatialRDDHelper.spatialPartition(featureJavaRDD, STRPartitioner.class, numPartition);
        String[] colorSet = new String[]{"#ff0000", "#00ff00", "#0000ff", "#ffff00"};
        for (int i = 0; i < numPartition; i++) {
            int finalI = i;
            JavaRDD<IFeature> values = integerIFeatureJavaPairRDD.filter(f -> f._1 == finalI).values();
            System.out.println(values.count());
            Summary summary = JavaSpatialRDDHelper.summary(values);
            System.out.println(summary);
            JavaSpatialRDDHelper.plotImage(values, 2000, 2000, "pics/partition-new" + i + ".png",
                    new ButterflyOptions().set("stroke", "black").set("fill", colorSet[i]));
        }
    }

    @Test
    public void bufferTest() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_LANDUSE_SHP);
        JavaRDD<IFeature> featureJavaRDD = geometryVectorRDD.getFeatureRDD();
        System.out.println(featureJavaRDD.count());
        int numPartition = 4;
        JavaPairRDD<Integer, IFeature> integerIFeatureJavaPairRDD = JavaSpatialRDDHelper.spatialPartition(featureJavaRDD, ZCurvePartitioner.class, numPartition);
        for (int i = 0; i < numPartition; i++) {
            // 计时
            long start = System.currentTimeMillis();
            int finalI = i;
            JavaRDD<IFeature> values = integerIFeatureJavaPairRDD.filter(f -> f._1 == finalI).values();
            values.map(f -> f.getGeometry().buffer(0.0001));
            long end = System.currentTimeMillis();
            System.out.println(values.count());
            System.out.println(JavaSpatialRDDHelper.summary(values).numPoints());
            System.out.println("buffer time:" + (end - start));
        }
    }

    @Test
    public void RtreePlotTest() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_LANDUSE_SHP);
        JavaRDD<IFeature> featureJavaRDD = geometryVectorRDD.getFeatureRDD();
        JavaRDD<Geometry> geometryJavaRDD = featureJavaRDD.map(f -> f.getGeometry());
        List<Geometry> collect = geometryJavaRDD.collect();
        RTree<Object, com.github.davidmoten.rtree.geometry.Geometry> objectGeometryRTree = RTree.star().maxChildren(40).create();
        for (Geometry geometry : collect) {
            Envelope envelopeInternal = geometry.getEnvelopeInternal();
            objectGeometryRTree = objectGeometryRTree.add(geometry, envelopeToRect(envelopeInternal));
        }
        objectGeometryRTree.visualize(1000, 1000).save("rtree.png");
    }

    private static Rectangle envelopeToRect(Envelope box) {
        return Geometries.rectangle(box.getMinX(), box.getMinY(), box.getMaxX(), box.getMaxY());
    }

    @Test
    public void summary() {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> geometryVectorRDD = new VectorRDD<>(jsc);
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_LANDUSE_SHP);
        System.out.println(geometryVectorRDD.summary());
    }

    @Test
    public void partitionerTest() {
        VectorRDD<Geometry> pointDataGen10K = CaseVectorRDD.getPointDataGen10K();
        pointDataGen10K.doPartition2(HCurvePartitioner.class, "fixed", 16);
        JavaRDD<IFeature> featureRDD = pointDataGen10K.getFeatureRDD();
        Optional<Partitioner> partitioner = featureRDD.partitioner();
        Summary summary = pointDataGen10K.summary();
        System.out.println(summary);
        List<Tuple2<Integer, Summary>> tuple2s = computePartialSummaries(featureRDD, IFeature::getStorageSize).collect();
        tuple2s.forEach(t -> System.out.println(t._1 + ":" + t._2));
        int numPartitions = featureRDD.getNumPartitions();
        System.out.println(numPartitions);
        int bp = -1;
    }

    public static JavaRDD<Tuple2<Integer, Summary>> computePartialSummaries(JavaRDD<IFeature> featureRDD, Function<IFeature, Integer> sizeFunction) {
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = featureRDD.mapPartitionsWithIndex((idx, features) -> {
            Summary summary = new Summary();
            while (features.hasNext()) {
                IFeature feature = features.next();
                if (summary.getCoordinateDimension() == 0) {
                    summary.setCoordinateDimension(GeometryHelper.getCoordinateDimension(feature.getGeometry()));
                }
                summary.expandToGeometryWithSize(feature.getGeometry(), sizeFunction.call(feature));
            }
            List<Tuple2<Integer, Summary>> summaries = new ArrayList<>();
            summaries.add(new Tuple2<>(idx, summary));
            return summaries.iterator();
        }, true);
        // If the number of partitions is very large, Spark would fail because it will try to collect all
        // the partial summaries locally and would run out of memory in such case
        if (tuple2JavaRDD.getNumPartitions() > 1000) {
            tuple2JavaRDD = tuple2JavaRDD.coalesce(1000);
        }
        return tuple2JavaRDD;
    }

}