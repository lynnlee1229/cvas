package spatialrdd.ch3;

import butterfly.core.algrithm.buffer.BufferHelper;
import butterfly.core.common.CaseVectorRDD;
import butterfly.core.common.Key;
import butterfly.core.spatialrdd.VectorRDD;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Lynn Lee
 * @date 2024/1/8
 * 第三章Test：主要测试空间分析的内容
 **/
public class Chapter3Test implements Serializable {
    public final String OUT_PATH = "outs/ch3/";
    public final double BUFFER_GEOGRAPHICAL_DIS = 1e-2;
    public final double BUFFER_GEOMETRIC_DIS = 1000;
//    public JavaSpatialSparkContext jsc = new JavaSpatialSparkContext("local[*]", "chapter3Test");


    @Test
    public void bufferTest1() {
        VectorRDD<Geometry> pointDataGen10K = CaseVectorRDD.getPointDataGen10K();
        JavaRDD<IFeature> featureRDD = pointDataGen10K.getFeatureRDD();
        BufferHelper.plotBuffer(featureRDD, BUFFER_GEOGRAPHICAL_DIS, 1000, 1000, "outs/ch3/bufferTest1.png", new ButterflyOptions());
//        PlotHelper.plotFeatures(bufferedGeom, 1000, 1000, "outs/ch3/bufferTest1.png", GeometricPlotter.class, null, new BeastOptions().set("fill","red"));

    }

    @Test
    public void bufferTest2() {
        VectorRDD<Geometry> polygonGenerator = CaseVectorRDD.getPointDataGen10K();
        JavaRDD<IFeature> featureRDD = polygonGenerator.getFeatureRDD();
        JavaRDD<IFeature> buffered = BufferHelper.bufferV1(featureRDD, null, new ButterflyOptions().set(Key.BUFFER_DIS, 0.001).set(Key.BUFFER_DISSOLVE,"true"));
        System.out.println(buffered.filter(Objects::nonNull).count());
    }

}
