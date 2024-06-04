package butterfly.core.algrithm.buffer;

import butterfly.core.algrithm.join.JoinHelper;
import butterfly.core.common.Key;
import butterfly.core.spatialrdd.PlotHelper;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import cn.edu.whu.lynn.core.SJPredicate;
import cn.edu.whu.lynn.davinci.Canvas;
import cn.edu.whu.lynn.davinci.GeometricPlotter;
import cn.edu.whu.lynn.davinci.Plotter;
import cn.edu.whu.lynn.geolite.Feature;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author Lynn Lee
 * @date 2024/1/22
 **/
public class BufferHelper implements Serializable {
    public static JavaRDD<IFeature> bufferV1(JavaRDD<IFeature> featureRDD, MetricsAccumulator metricsAccumulator, ButterflyOptions opts) {
        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
        String bufferUnit = opts.getString(Key.BUFFER_UNIT, "degree");
        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
        JavaRDD<IFeature> bufferedRDD = JoinHelper.buffer(featureRDD, opts, metricsAccumulator, bufferDistance);
        bufferedRDD.cache();
        if (bufferDissolve) {
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
    @Deprecated
    /**
     * 基于缓冲区基本单元融合的缓冲区生成，实验发现小数据量时不如直接用JTS，弃用
     */
    public static JavaRDD<IFeature> bufferWithBufferUnit(JavaRDD<IFeature> featureRDD, MetricsAccumulator metricsAccumulator, ButterflyOptions opts) {
        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
        String bufferUnit = opts.getString(Key.BUFFER_UNIT, "degree");
        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
        JavaRDD<IFeature> bufferUnitFeature = featureRDD.flatMap(f -> {
            Geometry geometry = f.getGeometry();
            int id = geometry.hashCode();
            // 拆分成若干线段
            Coordinate[] coordinates = geometry.getCoordinates();
            List<IFeature> features = new ArrayList<>();
            for (int i = 0; i < coordinates.length - 1; i++) {
                Coordinate start = coordinates[i];
                Coordinate end = coordinates[i + 1];
                Geometry line = geometry.getFactory().createLineString(new Coordinate[]{start, end});
                Geometry buffer = line.buffer(bufferDistance);
                buffer.setUserData((Object)(id));
                features.add(Feature.create(null, buffer));
            }
            return features.iterator();
        });
        bufferUnitFeature.cache();
        bufferUnitFeature.groupBy(f ->(int) f.getGeometry().getUserData()).map(
                pair -> {
                    int id = pair._1();
                    Iterable<IFeature> iFeatures = pair._2();
                    List<IFeature> features = new ArrayList<>();
                    for (IFeature iFeature : iFeatures) {
                        features.add(iFeature);
                    }
                    Geometry mergedGeometry = null;
                    for (IFeature feature : features) {
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
        // dissolve还是用图方法，这里就不做了
//        if (bufferDissolve) {
//            JavaPairRDD<IFeature, IFeature> iFeatureIFeatureJavaPairRDD = JoinHelper.spatialSelfJoin(bufferedRDD, opts, SJPredicate.Intersects);
//        }
        return null;
    }

    public static JavaRDD<IFeature> bufferV2(JavaRDD<IFeature> featureRDD, MetricsAccumulator metricsAccumulator, ButterflyOptions opts) {
        double bufferDistance = opts.getDouble(Key.BUFFER_DIS, 0.0);
        String bufferUnit = opts.getString(Key.BUFFER_UNIT, "degree");
        boolean bufferDissolve = opts.getBoolean(Key.BUFFER_DISSOLVE, false);
        JavaRDD<IFeature> bufferedRDD = JoinHelper.buffer(featureRDD, opts, metricsAccumulator, bufferDistance);
        if (bufferDissolve) {
            JavaPairRDD<IFeature, IFeature> iFeatureIFeatureJavaPairRDD = JoinHelper.spatialSelfJoin(bufferedRDD, opts, SJPredicate.Intersects);
        }
        return bufferedRDD;
    }

    @Deprecated
    /**
     *  使用并查集生成待融合簇（弃用，最后用的是图）
     */
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
        }};
        Plotter finalPlotter = PlotHelper.createPlotterInstance(GeometricPlotter.class, options);
        PlotHelper.plotCanvas(canvasArrayList, imagePath, options, finalPlotter, feature.rdd().context().hadoopConfiguration());
    }
}
