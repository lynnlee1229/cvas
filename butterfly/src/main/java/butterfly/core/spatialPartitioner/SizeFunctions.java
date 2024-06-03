package butterfly.core.spatialPartitioner;

import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.function.Function;

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
            default:
                return new pointNum();
        }
    }
}
