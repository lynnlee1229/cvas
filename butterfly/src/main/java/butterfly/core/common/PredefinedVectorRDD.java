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
public class PredefinedVectorRDD {
    JavaSparkContext jsc;
    VectorRDD<Geometry> geometryVectorRDD;

    public PredefinedVectorRDD(JavaSparkContext jsc) {
        this.jsc = jsc;
        geometryVectorRDD = new VectorRDD<>(jsc);
    }


    /**
     * Tdrive数据集
     */
    public VectorRDD<Geometry> getPointTDrive() {
        geometryVectorRDD.fromCSVPoint(ButterflyConstant.TDRIVE, "2", "3", ',', false);
        return geometryVectorRDD;
    }

    /**
     * 10k生成点集
     */
    public VectorRDD<Geometry> getPointDataGen10K() {
        geometryVectorRDD.fromWKTFile(ButterflyConstant.UNIFORM_POINTS_10K, 0, '\t', false);
        return geometryVectorRDD;
    }

    /**
     * 10k生成点集
     */
    public VectorRDD<Geometry> getPolygonDataGen10K() {
        geometryVectorRDD.fromWKTFile(ButterflyConstant.UNIFORM_POLYGONS_10K, 0, '\t', false);
        return geometryVectorRDD;
    }

    /**
     * 深圳土地利用数据集
     */
    public VectorRDD<Geometry> getPolygonShenzhenLandUse() {
        geometryVectorRDD.fromShapefile(ButterflyConstant.SHENZHEN_LANDUSE_SHP);
        return geometryVectorRDD;
    }

    public VectorRDD<Geometry> getPolygonWuhanLandUse() {
        geometryVectorRDD.fromShapefile(ButterflyConstant.WUHAN_LANDUSE_SHP);
        return geometryVectorRDD;
    }

    public VectorRDD<Geometry> getBeijingDistrict() {
        geometryVectorRDD.fromShapefile(ButterflyConstant.BEIJING_DISTRICT);
        return geometryVectorRDD;
    }

    public VectorRDD<Geometry> getPointFromGenerator(int cardinality, Distribution distributionType, int numPartitions, ButterflyOptions opts) {
        geometryVectorRDD.fromPointGenerator(cardinality, distributionType, numPartitions, opts);
        return geometryVectorRDD;
    }

    public VectorRDD<Geometry> getPolygonFromGenerator(int cardinality, Distribution distributionType, int numPartitions, ButterflyOptions opts, double maxSize, int maxSegments) {
        geometryVectorRDD.fromPolygonGenerator(cardinality, distributionType, numPartitions, opts, maxSize, maxSegments);
        return geometryVectorRDD;
    }

    public VectorRDD<Geometry> getLandUseMax(){
        geometryVectorRDD.fromShapefile(ButterflyConstant.LAND_USE_MAX);
        return geometryVectorRDD;
    }

}
