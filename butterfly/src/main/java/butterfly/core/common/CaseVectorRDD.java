package butterfly.core.common;

import butterfly.core.enums.Distribution;
import butterfly.core.spatialrdd.VectorRDD;
import cn.edu.whu.lynn.common.ButterflyOptions;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Lynn Lee
 * @date 2024/1/8
 * 测试用，内置几个用于测试的VectorRDD,包括小规模点集、线集、面集
 **/
public class CaseVectorRDD {
    static JavaSparkContext jsc;
    static VectorRDD<Geometry> geometryVectorRDD;

    static {
        jsc = new JavaSparkContext("local[*]", "caseVectorRDD");
        geometryVectorRDD = new VectorRDD<>(jsc);
    }

    /**
     * Tdrive数据集
     */
    public static VectorRDD<Geometry> getPointTDrive() {
        geometryVectorRDD.fromCSVPoint(ButterflyConstant.TDRIVE, "2", "3", ',', false);
        return geometryVectorRDD;
    }

    /**
     * 10k生成点集
     */
    public static VectorRDD<Geometry> getPointDataGen10K() {
        geometryVectorRDD.fromWKTFile(ButterflyConstant.UNIFORM_POINTS_10K, 0, '\t', false);
        return geometryVectorRDD;
    }

    /**
     * 10k生成点集
     */
    public static VectorRDD<Geometry> getPolygonDataGen10K() {
        geometryVectorRDD.fromWKTFile(ButterflyConstant.UNIFORM_POLYGONS_10K, 0, '\t', false);
        return geometryVectorRDD;
    }

    /**
     * 深圳土地利用数据集
     */
    public static VectorRDD<Geometry> getPolygonShenzhenLandUse() {
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_LANDUSE_SHP);
        return geometryVectorRDD;
    }

    public static VectorRDD<Geometry> getPointFromGenerator(int cardinality, Distribution distributionType, int numPartitions, ButterflyOptions opts) {
        geometryVectorRDD.fromPointGenerator(cardinality, distributionType, numPartitions, opts);
        return geometryVectorRDD;
    }

    public static VectorRDD<Geometry> getPolygonFromGenerator(int cardinality, Distribution distributionType, int numPartitions, ButterflyOptions opts, double maxSize, int maxSegments) {
        geometryVectorRDD.fromPolygonGenerator(cardinality, distributionType, numPartitions, opts, maxSize, maxSegments);
        return geometryVectorRDD;
    }

}
