package butterfly.core.algrithm.join;

import butterfly.core.common.Key;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import cn.edu.whu.lynn.core.SJAlgorithm;
import cn.edu.whu.lynn.core.SJPredicate;
import cn.edu.whu.lynn.geolite.Feature;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.operations.SpatialJoinWithSimplification;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/1/12
 **/
public class JoinHelper implements Serializable {

    public static JavaPairRDD<IFeature, IFeature> spatialJoin(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, JavaRDD<IFeature> drivenRDD, SJPredicate predicate, SJAlgorithm joinMethod) {
        // 二路空间连接
        return spatialJoin(driverRDD, opts, drivenRDD, predicate, joinMethod, null);
    }

    public static JavaPairRDD<IFeature, IFeature> spatialJoin(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, JavaRDD<IFeature> drivenRDD, SJPredicate predicate, SJAlgorithm joinMethod, LongAccumulator mbrCount) {
        // 二路空间连接
        return SpatialJoinWithSimplification.spatialJoin(driverRDD, drivenRDD, predicate, joinMethod, mbrCount, opts);
    }

    public static JavaPairRDD<IFeature, IFeature> spatialJoinWithMetric(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, JavaRDD<IFeature> drivenRDD, SJPredicate predicate, SJAlgorithm joinMethod, MetricsAccumulator metricsAccumulator) {
        // 二路空间连接
        return SpatialJoinWithSimplification.spatialJoinWithMetric(driverRDD, drivenRDD, predicate, joinMethod, metricsAccumulator, opts);
    }

    /**
     * 二路空间连接
     *
     * @param driverRDD 驱动RDD
     * @param opts      配置项
     * @param drivenRDD 被驱动RDD
     * @return 空间连接结果
     */
    public static JavaPairRDD<IFeature, IFeature> spatialJoin(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, JavaRDD<IFeature> drivenRDD, SJPredicate predicate) {
        return spatialJoin(driverRDD, opts, drivenRDD, predicate, null);
    }

    /**
     * 多路空间连接
     *
     * @param driverRDD  驱动RDD
     * @param opts       配置项
     * @param drivenRDDs 被驱动RDD
     * @return 空间连接结果
     */
    @SafeVarargs
    public static JavaPairRDD<IFeature, Iterator<IFeature>> spatialMultiWayJoin(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, Tuple2<JavaRDD<IFeature>, SJPredicate>... drivenRDDs) {
        // 多路空间连接
        // TODO 当项目使用 Java 9 或更高版本后，可以考虑使用泛型类型推断的改进
        return spatialMultiWayJoinInternalV1(driverRDD, opts, drivenRDDs);
    }

    public static JavaPairRDD<IFeature, Iterator<IFeature>> spatialMultiWayJoin(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, List<Tuple2<JavaRDD<IFeature>, SJPredicate>> drivenRDDs) {
        // 多路空间连接
        return spatialMultiWayJoinInternalV1(driverRDD, opts, drivenRDDs.toArray(new Tuple2[0]));
    }

    public static JavaPairRDD<IFeature, Iterator<IFeature>> spatialMultiWayJoinInternalV1(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, Tuple2<JavaRDD<IFeature>, SJPredicate>[] drivenRDDs) {
        // 多路空间连接Version1 目前还是使用迭代方法实现多路连接
        // TODO 非for循环实现
        if (drivenRDDs.length == 0) return null;
        JavaPairRDD<IFeature, Iterator<IFeature>> result = null;
        for (Tuple2<JavaRDD<IFeature>, SJPredicate> drivenRDD : drivenRDDs) {
            JavaPairRDD<IFeature, Iterator<IFeature>> tmpResult = spatialJoin(driverRDD, opts, drivenRDD._1(), drivenRDD._2()).groupByKey() // 按键分组
                    .mapValues(Iterable::iterator);
            result = result == null ? tmpResult : result.union(tmpResult);
        }
        return result;
    }

    /**
     * 空间自连接
     *
     * @param rdd       RDD
     * @param opts      配置项
     * @param predicate 谓词
     * @return 空间连接结果
     */
    public static JavaPairRDD<IFeature, IFeature> spatialSelfJoin(JavaRDD<IFeature> rdd, ButterflyOptions opts, SJPredicate predicate) {
        // 二路空间连接拓展-空间自连接
        return SpatialJoinWithSimplification.selfJoin(rdd, predicate, null, opts);
    }

    public static JavaPairRDD<IFeature, IFeature> spatialSelfJoinWithMetric(JavaRDD<IFeature> rdd, ButterflyOptions opts, MetricsAccumulator metricsAccumulator, SJPredicate predicate) {
        // 二路空间连接拓展-空间自连接
        return SpatialJoinWithSimplification.selfJoinWithMetric(rdd, predicate, metricsAccumulator, opts);
    }

    public static JavaPairRDD<IFeature, List<IFeature>> selfJoinToCC(JavaRDD<IFeature> rdd, JavaPairRDD<IFeature, IFeature> pairRDD, ButterflyOptions opts, MetricsAccumulator metricsAccumulator, SJPredicate predicate) {
        return SpatialJoinWithSimplification.selfJoinToCC(rdd, pairRDD, predicate, metricsAccumulator, opts);
    }

    public static JavaPairRDD<IFeature,List<IFeature>> bufferWithDissolve(JavaRDD<IFeature> rdd, ButterflyOptions opts, MetricsAccumulator metricsAccumulator) {
        return SpatialJoinWithSimplification.bufferWithDissolve(rdd, SJPredicate.Intersects, metricsAccumulator, opts);
    }


    public static JavaRDD<IFeature> buffer(JavaRDD<IFeature> rdd, ButterflyOptions opts, MetricsAccumulator metricsAccumulator, Double distance) {
        return SpatialJoinWithSimplification.buffer(rdd, metricsAccumulator, opts, distance);
    }

    public static JavaPairRDD<IFeature, IFeature> spatialSelfJoinBaseLine(JavaRDD<IFeature> rdd, ButterflyOptions opts, MetricsAccumulator metricsAccumulator, SJPredicate predicate) {
        // 二路空间连接拓展-空间自连接
        return SpatialJoinWithSimplification.selfJoinBL(rdd, rdd, predicate, SJAlgorithm.PBSM, metricsAccumulator, opts);
    }

    public static JavaPairRDD<IFeature, IFeature> spatialJoinSedona(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, JavaRDD<IFeature> drivenRDD, SJPredicate predicate, SJAlgorithm joinMethod, MetricsAccumulator metricsAccumulator) {
//        JoinQuery.SpatialJoinQueryFlat()
        // TODO sedona空间连接
        boolean useindex = opts.getBoolean("useindex", true);
        int ignoreComplexThreshold = opts.getInt("ignoreComplexThreshold", 1000000);
        int numPartitions = opts.getInt("numPartitions", 128);
        SpatialPredicate sjPredicate = null;
        if (predicate != null) {
            // TODO
            sjPredicate = toSedonaPredicate(predicate);
        } else {
            String tmpPredicate = opts.getString(Key.PREDICATE, "Intersects");
            sjPredicate = SpatialPredicate.valueOf(tmpPredicate);
        }
        SpatialRDD<Geometry> driverSpatialRDD = toSpatialRDD(driverRDD, ignoreComplexThreshold);
        SpatialRDD<Geometry> drivenSpatialRDD = toSpatialRDD(drivenRDD, 1000000);
        JavaPairRDD<Geometry, Geometry> geometryGeometryJavaPairRDD = null;
        try {
            driverSpatialRDD.analyze();
            driverSpatialRDD.spatialPartitioning(GridType.EQUALGRID, numPartitions);
            drivenSpatialRDD.spatialPartitioning(driverSpatialRDD.getPartitioner());
            geometryGeometryJavaPairRDD = JoinQuery.SpatialJoinQueryFlat(driverSpatialRDD, drivenSpatialRDD, useindex, sjPredicate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (geometryGeometryJavaPairRDD == null) {
            return null;
        }
        return geometryGeometryJavaPairRDD.mapToPair(geometryTuple -> {
            Feature feature1 = Feature.create(null, geometryTuple._1());
            Feature feature2 = Feature.create(null, geometryTuple._2());
//            Feature feature1 = Feature.create((Row) geometryTuple._1().getUserData(), geometryTuple._1());
//            Feature feature2 = Feature.create((Row) geometryTuple._2().getUserData(), geometryTuple._2());
            return new Tuple2<>(feature1, feature2);
        });
    }


    public static JavaPairRDD<IFeature, IFeature> spatialSelfJoinSedona(JavaRDD<IFeature> rdd, ButterflyOptions opts, MetricsAccumulator metricsAccumulator, SJPredicate predicate) {
        // TODO sedona空间自连接
        return null;
    }

    public static SpatialRDD<Geometry> toSpatialRDD(JavaRDD<IFeature> rdd, int ignoreComplexThreshold) {
        // TODO 转换为sedona的SpatialRDD
//        public static void testSpatialJoinQuery() throws Exception {
//            queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true);
//            objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);
//            objectRDD.spatialPartitioning(joinQueryPartitioningType);
//            queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());
//            objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
//            queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
//
//            for(int i = 0; i < eachQueryLoopTimes; ++i) {
//                long resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, false, true).count();
//
//                assert resultSize > 0L;
//            }
//
//        }
//
//        public static void testSpatialJoinQueryUsingIndex() throws Exception {
//            queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true);
//            objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true);
//            objectRDD.spatialPartitioning(joinQueryPartitioningType);
//            queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());
//            objectRDD.buildIndex(PointRDDIndexType, true);
//            objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
//            queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
//
//            for(int i = 0; i < eachQueryLoopTimes; ++i) {
//                long resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, false).count();
//
//                assert resultSize > 0L;
//            }
//
//        }
        JavaRDD<Geometry> geometryJavaRDD = rdd.map(f -> {
            Geometry geometry = f.getGeometry();
//            geometry.setUserData(f.copy());
            return geometry;
        }).filter(geometry -> geometry.getNumPoints() < ignoreComplexThreshold);
        SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
        spatialRDD.setRawSpatialRDD(geometryJavaRDD);
        return spatialRDD;
    }

    private static SpatialPredicate toSedonaPredicate(SJPredicate predicate) {
        switch (predicate) {
            case Intersects:
                return SpatialPredicate.INTERSECTS;
            case Covers:
                return SpatialPredicate.COVERS;
            case Equals:
                return SpatialPredicate.EQUALS;
            case Within:
                return SpatialPredicate.WITHIN;
            case Crosses:
                return SpatialPredicate.CROSSES;
            case Touches:
                return SpatialPredicate.TOUCHES;
            case Contains:
                return SpatialPredicate.CONTAINS;
            case Overlaps:
                return SpatialPredicate.OVERLAPS;
            case Covered_by:
                return SpatialPredicate.COVERED_BY;
            default:
                return null;
        }
    }

}
