package butterfly.core.algrithm.buffer;

import butterfly.core.algrithm.join.JoinHelper;
import butterfly.core.common.Key;
import butterfly.core.spatialrdd.PlotHelper;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import cn.edu.whu.lynn.core.SJPredicate;
import cn.edu.whu.lynn.geolite.Feature;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.davinci.Canvas;
import cn.edu.whu.lynn.davinci.GeometricPlotter;
import cn.edu.whu.lynn.davinci.Plotter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * @author Lynn Lee
 * @date 2024/1/22
 **/
public class BufferHelper implements Serializable {
//    public static JavaRDD<IFeature> bufferV2(JavaRDD<IFeature> featureRDD, MetricsAccumulator metricsAccumulator, ButterflyOptions opts) {
//        return null;
//        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
//        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
//        // 不是调用buffer，而是更高效的buffer生成算法1
//        JavaRDD<IFeature> bufferedRDD = featureRDD.map(f -> {
//            Geometry geometry = f.getGeometry();
//            Geometry bufferedGeometry;
//            if (geometry instanceof Point) {
//                // 对于点几何，直接生成buffer
//                bufferedGeometry = geometry.buffer(bufferDistance);
//            } else if (geometry instanceof LineString || geometry instanceof Polygon) {
//                // 对于线和面几何，先进行简化操作
//                Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(geometry, bufferDistance / 2);
//                // 使用简化后的几何体生成buffer
//                bufferedGeometry = simplifiedGeometry.buffer(bufferDistance);
//                // 对于生成的buffer几何体进行膨胀操作
//                bufferedGeometry = bufferedGeometry.buffer(-bufferDistance);
//            } else if (geometry instanceof MultiPoint || geometry instanceof MultiLineString || geometry instanceof MultiPolygon) {
//                // 对于多点、多线和多边形集合，遍历其中的每一个几何体进行处理
//                GeometryCollection geometryCollection = (GeometryCollection) geometry;
//                Geometry[] geometries = new Geometry[geometryCollection.getNumGeometries()];
//                for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
//                    Geometry part = geometryCollection.getGeometryN(i);
//                    Geometry simplifiedPart = DouglasPeuckerSimplifier.simplify(part, bufferDistance / 2);
//                    Geometry bufferedPart = simplifiedPart.buffer(bufferDistance);
//                    Geometry inflatedBuffer = bufferedPart.buffer(-bufferDistance);
//                    geometries[i] = inflatedBuffer;
//                }
//                bufferedGeometry = geometryCollection.getFactory().createGeometryCollection(geometries);
//            } else {
//                // 其他类型的几何体，暂时不处理
//                bufferedGeometry = geometry;
//            }
//            return Feature.create(null, bufferedGeometry);
//        });
//
//        if (bufferDissolve) {
//            JavaPairRDD<IFeature, IFeature> iFeatureIFeatureJavaPairRDD = JoinHelper.spatialSelfJoinWithMetric(bufferedRDD, opts, metricsAccumulator, SJPredicate.Intersects);
//            return iFeatureIFeatureJavaPairRDD
//                    .mapToPair(pair -> new Tuple2<>(pair._1(), pair._2()))
//                    .reduceByKey((f1, f2) -> {
//                        Geometry geom1 = f1.getGeometry();
//                        Geometry geom2 = f2.getGeometry();
//                        // 合并两个几何要素
//                        Geometry mergedGeometry = geom1.union(geom2);
//                        return Feature.create(null, mergedGeometry);
//                    })
//                    .values();
//        } else {
//            return bufferedRDD;
//        }
//    }

//    public static JavaRDD<IFeature> bufferV1(JavaRDD<IFeature> featureRDD, ButterflyOptions opts) {
//        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
//        String bufferUnit = opts.getString(Key.BUFFER_UNIT, "degree");
//        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
//        // TODO 单位不为度时的处理
//        JavaRDD<IFeature> bufferedRDD = featureRDD.map(f -> {
//            Geometry geometry = f.getGeometry().buffer(bufferDistance);
//            return Feature.create(null, geometry);
//        });
//        if (bufferDissolve) {
//            JavaPairRDD<IFeature, IFeature> iFeatureIFeatureJavaPairRDD = JoinHelper.spatialSelfJoin(bufferedRDD, opts, SJPredicate.Intersects);
//            // 按照自连接结果分组，然后合并为同一个feature(这一步要尽可能的高效)
//            return iFeatureIFeatureJavaPairRDD
//                    .mapToPair(pair -> new Tuple2<>(pair._1(), pair._2()))
//                    .reduceByKey((f1, f2) -> {
//                        Geometry geom1 = f1.getGeometry();
//                        Geometry geom2 = f2.getGeometry();
//                        // 合并两个几何要素
//                        Geometry mergedGeometry = geom1.union(geom2);
//                        return Feature.create(null, mergedGeometry);
//                    })
//                    .values();
//        } else return bufferedRDD;
//    }

    public static JavaRDD<IFeature> bufferV1(JavaRDD<IFeature> featureRDD, MetricsAccumulator metricsAccumulator, ButterflyOptions opts) {
        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
        String bufferUnit = opts.getString(Key.BUFFER_UNIT, "degree");
        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
        JavaRDD<IFeature> bufferedRDD = JoinHelper.buffer(featureRDD, opts, metricsAccumulator, bufferDistance);
        bufferedRDD.cache();
//        JavaRDD<IFeature> bufferedRDD = featureRDD.map(f -> {
//            Geometry geometry = f.getGeometry().buffer(bufferDistance);
//            return Feature.create(null, geometry);
//        });
        bufferedRDD.cache();
        if (bufferDissolve) {
//            JavaPairRDD<IFeature, IFeature> spatialSelfJoin = JoinHelper.spatialSelfJoinWithMetric(bufferedRDD, opts, metricsAccumulator, SJPredicate.Intersects);
//            JavaPairRDD<IFeature, IFeature> spatialSelfJoin = JoinHelper.spatialJoinWithMetric(bufferedRDD, opts, bufferedRDD, SJPredicate.Intersects, null, metricsAccumulator);
//            JavaPairRDD<IFeature, List<IFeature>> iFeatureListJavaPairRDD = JoinHelper.selfJoinToCC(bufferedRDD, spatialSelfJoin, opts, metricsAccumulator, SJPredicate.Intersects);
            JavaPairRDD<IFeature, List<IFeature>> iFeatureListJavaPairRDD = JoinHelper.bufferWithDissolve(featureRDD, opts, metricsAccumulator);
            return iFeatureListJavaPairRDD.filter(Objects::nonNull).map(
                    pair -> {
                        IFeature representative = pair._1();
                        List<IFeature> cluster = pair._2();
                        Geometry mergedGeometry = null;
                        for (IFeature feature : cluster) {
                            if (mergedGeometry == null) {
                                mergedGeometry = feature.getGeometry();
                            } else {
                                mergedGeometry = mergedGeometry.union(feature.getGeometry());
                            }
                        }
                        if (mergedGeometry != null) {
                            return Feature.create(null, mergedGeometry);
                        } else {
                            return null;
                        }
                    }
            );
        }
        return bufferedRDD;
    }

    public static JavaRDD<IFeature> bufferV2(JavaRDD<IFeature> featureRDD, MetricsAccumulator metricsAccumulator, ButterflyOptions opts) {
        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
        String bufferUnit = opts.getString(Key.BUFFER_UNIT, "degree");
        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
        JavaRDD<IFeature> bufferedRDD = JoinHelper.buffer(featureRDD, opts, metricsAccumulator, bufferDistance);
//        JavaRDD<IFeature> bufferedRDD = featureRDD.map(f -> {
//            Geometry geometry = f.getGeometry().buffer(bufferDistance);
//            return Feature.create(null, geometry);
//        });
        if (bufferDissolve) {
            JavaPairRDD<IFeature, IFeature> iFeatureIFeatureJavaPairRDD = JoinHelper.spatialSelfJoin(bufferedRDD, opts, SJPredicate.Intersects);

        }
        return bufferedRDD;
    }
//    public static JavaRDD<IFeature> bufferV1BAK(JavaRDD<IFeature> featureRDD, MetricsAccumulator metricsAccumulator, ButterflyOptions opts) {
//        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
//        String bufferUnit = opts.getString(Key.BUFFER_UNIT, "degree");
//        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
//        // TODO 单位不为度时的处理
//        JavaRDD<IFeature> bufferedRDD = featureRDD.map(f -> {
//            Geometry geometry = f.getGeometry().buffer(bufferDistance);
//            return Feature.create(null, geometry);
//        });
//        if (bufferDissolve) {
//            JavaPairRDD<IFeature, IFeature> iFeatureIFeatureJavaPairRDD = JoinHelper.spatialSelfJoinWithMetric(bufferedRDD, opts, metricsAccumulator, SJPredicate.Intersects);
//            // 使用并查集生成待融合簇
//            JavaPairRDD<IFeature, Iterable<IFeature>> clusters = generateClusters(iFeatureIFeatureJavaPairRDD);
//            // 将待融合簇中的要素进行融合
//            return clusters.flatMap(pair -> {
//                IFeature representative = pair._1();
//                Iterable<IFeature> cluster = pair._2();
//                Geometry mergedGeometry = null;
//                for (IFeature feature : cluster) {
//                    if (mergedGeometry == null) {
//                        mergedGeometry = feature.getGeometry();
//                    } else {
//                        mergedGeometry = mergedGeometry.union(feature.getGeometry());
//                    }
//                }
//                if (mergedGeometry != null) {
//                    return Collections.singletonList((IFeature) Feature.create(null, mergedGeometry)).iterator();
//                } else {
//                    return Collections.emptyIterator();
//                }
//            });
//        } else return bufferedRDD;
//    }

    // 使用并查集生成待融合簇
    private static JavaPairRDD<IFeature, Iterable<IFeature>> generateClusters(JavaPairRDD<IFeature, IFeature> pairRDD) {
        // 将自连接结果转换为具有连通性的数据结构
        JavaPairRDD<IFeature, IFeature> edges = pairRDD.flatMapToPair(pair -> {
            IFeature feature1 = pair._1();
            IFeature feature2 = pair._2();
            return Arrays.asList(new Tuple2<>(feature1, feature2), new Tuple2<>(feature2, feature1)).iterator();
        });

        // 利用并查集生成待融合簇
        JavaPairRDD<IFeature, IFeature> parents = edges.reduceByKey((f1, f2) -> f1);
        JavaPairRDD<IFeature, Iterable<IFeature>> clusters = parents.groupByKey();
        return clusters;
    }

    public static JavaRDD<IFeature> bufferBaseline(JavaRDD<IFeature> featureRDD, MetricsAccumulator metricsAccumulator, ButterflyOptions opts) {
        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
        JavaRDD<IFeature> bufferedRDD = featureRDD.map(f -> {
            Geometry geometry = f.getGeometry().buffer(bufferDistance);
            return Feature.create(null, geometry);
        });

        if (bufferDissolve) {
            List<IFeature> features = bufferedRDD.collect();
            List<IFeature> mergedFeatures = new ArrayList<>();
            int size = features.size();
            boolean[] mergedFlags = new boolean[size]; // Flags to track merged features
            for (int i = 0; i < size; i++) {
                if (mergedFlags[i]) {
                    continue; // Skip already merged features
                }
                IFeature feature1 = features.get(i);
                Geometry geom1 = feature1.getGeometry();
                for (int j = i + 1; j < size; j++) {
                    if (mergedFlags[j]) {
                        continue; // Skip already merged features
                    }
                    IFeature feature2 = features.get(j);
                    Geometry geom2 = feature2.getGeometry();
                    if (!Objects.equals(feature1, feature2) && geom1.intersects(geom2)) {
                        geom1 = geom1.union(geom2);
                        mergedFlags[j] = true; // Mark feature2 as merged
                    }
                }
                mergedFeatures.add(Feature.create(null, geom1));
            }
            return JavaSparkContext.fromSparkContext(featureRDD.context()).parallelize(mergedFeatures);
        } else {
            return bufferedRDD;
        }
    }


    public static JavaRDD<IFeature> buffer(JavaRDD<IFeature> featureRDD, double bufferDistance) {
        return featureRDD.map(f -> {
            Geometry geometry = f.getGeometry().buffer(bufferDistance);
            return Feature.create(null, geometry);
        });
    }

    public static void plotBuffer(JavaRDD<IFeature> feature, double dis, int imageWidth, int imageHeight, String imagePath, ButterflyOptions options) {
        Canvas initCanvas = PlotHelper.plotToCanvas(feature, imageWidth, imageHeight, GeometricPlotter.class, null, new ButterflyOptions(options).set("fill", "black"));
        Canvas bufferedCanvas = PlotHelper.plotToCanvas(buffer(feature, dis), imageWidth, imageHeight, GeometricPlotter.class, null, new ButterflyOptions(options).set("fill", "red"));
        // TODO 优化绘图，目前画出来很难看
        ArrayList<Canvas> canvasArrayList = new ArrayList<Canvas>() {{
            add(bufferedCanvas);
//            add(initCanvas);
        }};
        Plotter finalPlotter = PlotHelper.createPlotterInstance(GeometricPlotter.class, options);
        PlotHelper.plotCanvas(canvasArrayList, imagePath, options, finalPlotter, feature.rdd().context().hadoopConfiguration());
    }

    public static void plotBufferV1(JavaRDD<IFeature> feature, double dis, int imageWidth, int imageHeight, String imagePath, ButterflyOptions options) {
        Canvas initCanvas = PlotHelper.plotToCanvas(feature, imageWidth, imageHeight, GeometricPlotter.class, null, new ButterflyOptions(options).set("fill", "black"));
        Canvas bufferedCanvas = PlotHelper.plotToCanvas(feature, imageWidth, imageHeight, GeometricPlotter.class, null, new ButterflyOptions(options).set("fill", "red"));
        // TODO 优化绘图，目前画出来很难看
        ArrayList<Canvas> canvasArrayList = new ArrayList<Canvas>() {{
            add(bufferedCanvas);
//            add(initCanvas);
        }};
        Plotter finalPlotter = PlotHelper.createPlotterInstance(GeometricPlotter.class, options);
        PlotHelper.plotCanvas(canvasArrayList, imagePath, options, finalPlotter, feature.rdd().context().hadoopConfiguration());
    }
}
