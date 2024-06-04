package butterfly.core.algrithm.overlap;

import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import cn.edu.whu.lynn.core.SJAlgorithm;
import cn.edu.whu.lynn.core.SJPredicate;
import cn.edu.whu.lynn.geolite.Feature;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.operations.SpatialJoinWithSimplification;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.overlay.OverlayOp;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2024/1/23
 **/
public class OverlayHelper implements Serializable {
    public static JavaRDD<IFeature> overlayWithMetric(JavaRDD<IFeature> driverRDD, ButterflyOptions opts, JavaRDD<IFeature> drivenRDD, SJPredicate predicate, SJAlgorithm joinMethod, MetricsAccumulator metricsAccumulator, OverlayOption overlayOption) {
        JavaPairRDD<IFeature, IFeature> joinPairRDD = SpatialJoinWithSimplification.spatialJoinWithMetric(driverRDD, drivenRDD, predicate, joinMethod, metricsAccumulator, opts);
        return joinPairRDD.map((pair) -> {
            Geometry driver = pair._1().getGeometry();
            Geometry driven = pair._2().getGeometry();
            // 计算空间关系
            OverlayOp overlayOp = new OverlayOp(driver, driven);
            // 生成新的Feature
            switch (overlayOption) {
                case INTERSECTION:
                    return Feature.create(null, overlayOp.getResultGeometry(OverlayOp.INTERSECTION));
                case UNION:
                    return Feature.create(null, overlayOp.getResultGeometry(OverlayOp.UNION));
                case DIFFERENCE:
                    return Feature.create(null, overlayOp.getResultGeometry(OverlayOp.DIFFERENCE));
                case SYMMETRIC_DIFFERENCE:
                    return Feature.create(null, overlayOp.getResultGeometry(OverlayOp.SYMDIFFERENCE));
                default:
                    return Feature.create(null, overlayOp.getResultGeometry(OverlayOp.INTERSECTION));
            }
        });
    }
}
