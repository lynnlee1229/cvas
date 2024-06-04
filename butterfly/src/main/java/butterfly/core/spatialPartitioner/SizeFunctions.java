package butterfly.core.spatialPartitioner;

import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.function.Function;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

/**
 * @author Lynn Lee
 * @date 2024/1/14
 **/
public class SizeFunctions {
    // 根据点数
    public static class pointNum implements Function<IFeature, Object> {
        @Override
        public Object call(IFeature feature) throws Exception {
            return feature.getGeometry().getNumPoints();
        }
    }

    // 根据面积
    public static class area implements Function<IFeature, Object> {
        @Override
        public Object call(IFeature feature) throws Exception {
            return feature.getGeometry().getArea();
        }
    }

    // 根据长度
    public static class length implements Function<IFeature, Object> {
        @Override
        public Object call(IFeature feature) throws Exception {
            return feature.getGeometry().getLength();
        }
    }

    // 根据内存大小
    public static class storageSize implements Function<IFeature, Object> {
        @Override
        public Object call(IFeature feature) throws Exception {
            return feature.getStorageSize();
        }
    }

    // 根据要素个数
    public static class featureNum implements Function<IFeature, Object> {
        @Override
        public Object call(IFeature feature) throws Exception {
            return 1;
        }
    }

    // 根据计算复杂度
    public static class complexity implements Function<IFeature, Object> {
        @Override
        public Object call(IFeature feature) throws Exception {
            return complexityFunction(feature);
        }
    }

    public static double complexityFunction(IFeature feature) {
        Geometry geometry = feature.getGeometry();
        double area, pointNum, convexHullArea, mbrArea;
        if (geometry instanceof Point) {
            area = 0;
            pointNum = 1;
            convexHullArea = 0;
            mbrArea = 0;
        } else if (geometry instanceof LineString) {
            area = 0;
            pointNum = geometry.getNumPoints();
            convexHullArea = 0;
            mbrArea = 0;
        } else {
            area = geometry.getArea();
            pointNum = geometry.getNumPoints();
            convexHullArea = geometry.convexHull().getArea();
            mbrArea = geometry.getEnvelopeInternal().getArea();
        }
        double constant = 8.015e-05;
        double f1 = 2.597e-07;
        double f2 = 2.012e-05;
        double f3 = -5.346e-07;
        double f4 = -6.993e-06;
        return constant + f1 * pointNum + f2 * area + f3 * (convexHullArea / area) + f4 * (mbrArea / area);
    }

    public static Function<IFeature, Object> getSizeFunction(String sizeFunction) {
        switch (sizeFunction) {
            case "featureNum":
                return new featureNum();
            case "storageSize":
                return new storageSize();
            case "pointNum":
                return new pointNum();
            case "area":
                return new area();
            case "length":
                return new length();
            case "complexity":
                return new complexity();
            default:
                return new pointNum();
        }
    }
}
